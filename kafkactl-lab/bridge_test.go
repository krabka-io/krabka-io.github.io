package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestBridgeFramesAndPairing(t *testing.T) {
	const origin = "http://127.0.0.1:4321"
	server := httptest.NewServer(bridgeHandler("secret", origin))
	defer server.Close()
	address := "ws" + strings.TrimPrefix(server.URL, "http") + "/bridge"
	if ws, err := websocket.Dial(address, "", "http://evil.example"); err == nil {
		ws.Close()
		t.Fatal("wrong origin connected")
	}
	if ws, err := websocket.Dial(address, "", origin); err != nil {
		t.Fatal(err)
	} else {
		websocket.JSON.Send(ws, packet{Type: "hello", Token: "wrong"})
		ws.SetReadDeadline(time.Now().Add(time.Second))
		var reply packet
		if websocket.JSON.Receive(ws, &reply) == nil {
			t.Fatal("wrong token paired")
		}
		ws.Close()
	}
	ws, err := websocket.Dial(address, "", origin)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := websocket.JSON.Send(ws, packet{Type: "hello", Token: "secret"}); err != nil {
		t.Fatal(err)
	}
	var reply packet
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "ready" {
		t.Fatalf("ready: %#v %v", reply, err)
	}
	const broker uint16 = 9999
	const second uint16 = 9998
	if err := websocket.JSON.Send(ws, packet{Type: "configure", Brokers: []uint16{broker, second}}); err != nil {
		t.Fatal(err)
	}
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "configured" {
		t.Fatalf("configured: %#v %v", reply, err)
	}
	secondConn, err := net.DialTimeout("tcp", brokerAddress(second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer secondConn.Close()
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "open" || reply.Broker != second {
		t.Fatalf("second broker: %#v %v", reply, err)
	}
	conn, err := net.DialTimeout("tcp", brokerAddress(broker), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "open" || reply.Broker != broker {
		t.Fatalf("open: %#v %v", reply, err)
	}
	id := reply.Conn
	request := append([]byte{0, 0, 0, 4}, []byte("ping")...)
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "data" || reply.Conn != id {
		t.Fatalf("request: %#v %v", reply, err)
	}
	got, err := base64.StdEncoding.DecodeString(reply.Data)
	if err != nil || !bytes.Equal(got, request) {
		t.Fatalf("request bytes: %q %v", got, err)
	}
	response := append([]byte{0, 0, 0, 4}, []byte("pong")...)
	if err := websocket.JSON.Send(ws, packet{Type: "data", Conn: id, Data: base64.StdEncoding.EncodeToString(response)}); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, len(response))
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, response) {
		t.Fatalf("response bytes: %q %v", got, err)
	}
	if err := websocket.JSON.Send(ws, packet{Type: "close", Conn: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := secondConn.Write([]byte{0, 128, 0, 1}); err != nil {
		t.Fatal(err)
	}
	for {
		if err := websocket.JSON.Receive(ws, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Type == "close" && reply.Conn != id {
			break
		}
	}
	ws.Close()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		conn, err := net.DialTimeout("tcp", brokerAddress(second), 100*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("broker listener survived WebSocket disconnect")
}

func TestConfigurePortConflictLeavesListenersClosed(t *testing.T) {
	const free uint16 = 9997
	const busy uint16 = 9996
	occupied, err := net.Listen("tcp", brokerAddress(busy))
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s := &session{listeners: make(map[uint16]net.Listener), peers: make(map[uint32]*peer)}
	if err := s.configure([]uint16{free, busy}); err == nil {
		t.Fatal("port conflict accepted")
	}
	if len(s.listeners) != 0 {
		t.Fatal("partial configuration")
	}
	if conn, err := net.DialTimeout("tcp", brokerAddress(free), 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("staged listener leaked")
	}
}

func TestReconnectGetsNewConnectionID(t *testing.T) {
	const origin = "http://127.0.0.1:4321"
	const broker uint16 = 9995
	server := httptest.NewServer(bridgeHandler("secret", origin))
	defer server.Close()
	address := "ws" + strings.TrimPrefix(server.URL, "http") + "/bridge"
	open := func() (uint32, *websocket.Conn) {
		t.Helper()
		ws, err := websocket.Dial(address, "", origin)
		if err != nil {
			t.Fatal(err)
		}
		ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := websocket.JSON.Send(ws, packet{Type: "hello", Token: "secret"}); err != nil {
			t.Fatal(err)
		}
		var reply packet
		if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "ready" {
			t.Fatalf("ready: %#v %v", reply, err)
		}
		if err := websocket.JSON.Send(ws, packet{Type: "configure", Brokers: []uint16{broker}}); err != nil {
			t.Fatal(err)
		}
		if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "configured" {
			t.Fatalf("configured: %#v %v", reply, err)
		}
		conn, err := net.DialTimeout("tcp", brokerAddress(broker), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "open" {
			t.Fatalf("open: %#v %v", reply, err)
		}
		return reply.Conn, ws
	}
	first, ws := open()
	ws.Close()
	second, ws := open()
	defer ws.Close()
	if first == second {
		t.Fatal("connection ID reused across WebSocket sessions")
	}
	var reply packet
	var peers []net.Conn
	for range maxPeers - 1 {
		conn, err := net.DialTimeout("tcp", brokerAddress(broker), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, conn)
		if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "open" {
			t.Fatalf("peer open: %#v %v", reply, err)
		}
	}
	defer func() {
		for _, conn := range peers {
			conn.Close()
		}
	}()
	excess, err := net.DialTimeout("tcp", brokerAddress(broker), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	excess.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := excess.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("excess peer was not rejected: %v", err)
	}
	excess.Close()
	peers[0].Close()
	for {
		if err := websocket.JSON.Receive(ws, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Type == "close" {
			break
		}
	}
	replacement, err := net.DialTimeout("tcp", brokerAddress(broker), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := websocket.JSON.Receive(ws, &reply); err != nil || reply.Type != "open" {
		t.Fatalf("peer listener did not recover: %#v %v", reply, err)
	}
}
