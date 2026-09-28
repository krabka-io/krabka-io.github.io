package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/net/websocket"
)

const maxKafkaFrame = 8 * 1024 * 1024
const maxPeers = 32

type packet struct {
	Type    string   `json:"type"`
	Token   string   `json:"token,omitempty"`
	Message string   `json:"message,omitempty"`
	Conn    uint32   `json:"conn,omitempty"`
	Broker  uint16   `json:"broker,omitempty"`
	Brokers []uint16 `json:"brokers,omitempty"`
	Data    string   `json:"data,omitempty"`
}

type peer struct {
	id     uint32
	broker uint16
	conn   net.Conn
}

type session struct {
	ws        *websocket.Conn
	write     sync.Mutex
	mu        sync.Mutex
	listeners map[uint16]net.Listener
	peers     map[uint32]*peer
	next      *atomic.Uint32
	closed    bool
}

func (s *session) send(p packet) error {
	s.write.Lock()
	defer s.write.Unlock()
	return websocket.JSON.Send(s.ws, p)
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for _, l := range s.listeners {
		l.Close()
	}
	for _, p := range s.peers {
		p.conn.Close()
	}
	s.mu.Unlock()
	s.ws.Close()
}

func brokerAddress(id uint16) string {
	return fmt.Sprintf("127.0.0.1:%d", 9091+id)
}

func (s *session) configure(ids []uint16) error {
	wanted := make(map[uint16]bool, len(ids))
	for _, id := range ids {
		if id == 0 || id > 10000 {
			return errors.New("broker id must be between 1 and 10000 for the local bridge")
		}
		wanted[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	staged := make(map[uint16]net.Listener)
	for id := range wanted {
		if s.listeners[id] != nil {
			continue
		}
		l, err := net.Listen("tcp", brokerAddress(id))
		if err != nil {
			for _, opened := range staged {
				opened.Close()
			}
			return fmt.Errorf("cannot listen on %s: %w", brokerAddress(id), err)
		}
		staged[id] = l
	}
	for id, l := range s.listeners {
		if !wanted[id] {
			l.Close()
			delete(s.listeners, id)
			for key, p := range s.peers {
				if p.broker == id {
					p.conn.Close()
					delete(s.peers, key)
				}
			}
		}
	}
	for id, l := range staged {
		s.listeners[id] = l
		go s.accept(id, l)
	}
	return nil
}

func (s *session) accept(broker uint16, l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed || len(s.peers) >= maxPeers {
			closed := s.closed
			s.mu.Unlock()
			conn.Close()
			if closed {
				return
			}
			continue
		}
		id := s.next.Add(1)
		if id == 0 {
			id = s.next.Add(1)
		}
		p := &peer{id: id, broker: broker, conn: conn}
		s.peers[p.id] = p
		s.mu.Unlock()
		if err := s.send(packet{Type: "open", Conn: p.id, Broker: broker}); err != nil {
			conn.Close()
			return
		}
		go s.readKafka(p)
	}
}

func (s *session) readKafka(p *peer) {
	defer func() {
		s.mu.Lock()
		if s.peers[p.id] == p {
			delete(s.peers, p.id)
		}
		s.mu.Unlock()
		p.conn.Close()
		s.send(packet{Type: "close", Conn: p.id})
	}()
	for {
		var prefix [4]byte
		if _, err := io.ReadFull(p.conn, prefix[:]); err != nil {
			return
		}
		n := int(prefix[0])<<24 | int(prefix[1])<<16 | int(prefix[2])<<8 | int(prefix[3])
		if n < 0 || n > maxKafkaFrame {
			return
		}
		frame := make([]byte, n+4)
		copy(frame, prefix[:])
		if _, err := io.ReadFull(p.conn, frame[4:]); err != nil {
			return
		}
		if err := s.send(packet{Type: "data", Conn: p.id, Data: base64.StdEncoding.EncodeToString(frame)}); err != nil {
			return
		}
	}
}

func (s *session) input(p packet) error {
	s.mu.Lock()
	client := s.peers[p.Conn]
	s.mu.Unlock()
	if client == nil {
		return nil
	}
	if p.Type == "close" {
		return client.conn.Close()
	}
	if p.Type != "data" {
		return errors.New("unknown bridge message")
	}
	data, err := base64.StdEncoding.DecodeString(p.Data)
	if err != nil || len(data) < 4 || len(data) > maxKafkaFrame+4 {
		return errors.New("invalid Kafka frame")
	}
	n := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	if n != len(data)-4 {
		return errors.New("invalid Kafka frame length")
	}
	for len(data) > 0 {
		n, err := client.conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func bridgeHandler(token, origin string) http.Handler {
	var active sync.Mutex
	var current *session
	var next atomic.Uint32
	server := &websocket.Server{
		Handshake: func(_ *websocket.Config, r *http.Request) error {
			if r.Header.Get("Origin") != origin {
				return fmt.Errorf("only %s may pair with this bridge", origin)
			}
			return nil
		},
		Handler: func(ws *websocket.Conn) {
			defer ws.Close()
			ws.MaxPayloadBytes = 12 * 1024 * 1024
			var hello packet
			if websocket.JSON.Receive(ws, &hello) != nil || hello.Type != "hello" || subtle.ConstantTimeCompare([]byte(hello.Token), []byte(token)) != 1 {
				return
			}
			s := &session{ws: ws, listeners: make(map[uint16]net.Listener), peers: make(map[uint32]*peer), next: &next}
			active.Lock()
			if current != nil {
				current.close()
			}
			current = s
			active.Unlock()
			defer func() {
				s.close()
				active.Lock()
				if current == s {
					current = nil
				}
				active.Unlock()
			}()
			if s.send(packet{Type: "ready"}) != nil {
				return
			}
			for {
				var p packet
				if err := websocket.JSON.Receive(ws, &p); err != nil {
					return
				}
				switch p.Type {
				case "configure":
					if err := s.configure(p.Brokers); err != nil {
						s.send(packet{Type: "error", Message: err.Error()})
					} else {
						s.send(packet{Type: "configured", Brokers: p.Brokers})
					}
				case "data", "close":
					if err := s.input(p); err != nil {
						s.send(packet{Type: "error", Message: err.Error()})
					}
				default:
					s.send(packet{Type: "error", Message: "unknown bridge message"})
				}
			}
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/bridge", server)
	return mux
}

func runBridge(ctx context.Context, origin string) error {
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	token := hex.EncodeToString(secret)
	listener, err := net.Listen("tcp", "127.0.0.1:19092")
	if err != nil {
		return fmt.Errorf("bridge port 19092 is unavailable: %w", err)
	}
	defer listener.Close()
	httpServer := &http.Server{Handler: bridgeHandler(token, origin)}
	stop, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() { <-stop.Done(); httpServer.Shutdown(context.Background()) }()
	fmt.Println("Open https://krabka.io/docs/lab and enter this pairing token in Connect with kafkactl:")
	fmt.Println(token)
	fmt.Println("Bridge ready on 127.0.0.1:19092. Keep this terminal open.")
	err = httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
