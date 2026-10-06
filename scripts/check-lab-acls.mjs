// Small trust-boundary check; the real broker allow/deny cases run in check-lab-clusters.
import assert from 'node:assert/strict';
import { ExternalHost, saslRequest, validateSaslResponse } from '../public/playground/lab/external.js';

const response = (correlation, body) => {
  const bytes = new Uint8Array(8+body.length), view = new DataView(bytes.buffer);
  view.setInt32(0,bytes.length-4); view.setInt32(4,correlation); bytes.set(body,8); return bytes;
};
const handshake = response(-2147483648, Uint8Array.from([0,0,0,0,0,1,0,5,80,76,65,73,78]));
const authenticated = response(-2147483647, Uint8Array.from([0,0,255,255,0,0,0,0,0,0,0,0,0,0,0,0]));
validateSaslResponse(handshake,true); validateSaslResponse(authenticated,false);
for (const [bytes, kind] of [[handshake,true],[authenticated,false]]) {
  for (let length=0;length<bytes.length;length++) assert.throws(() => validateSaslResponse(bytes.subarray(0,length),kind));
  const denied=bytes.slice(); denied[9]=58; assert.throws(() => validateSaslResponse(denied,kind));
  const wrong=bytes.slice(); wrong[7]^=1; assert.throws(() => validateSaslResponse(wrong,kind));
}
const events=[], routed=[], sent=[];
const world={nodes:[{id:1,kind:'krabka-broker'}, {id:4,kind:'producer'}, {id:7,kind:'admin',state:{authorization:{ready:false}}}]};
const host=new ExternalHost({world:()=>world,scenario:()=>({authorization:{acls:[]}}),event:(_id,kind,detail)=>events.push({kind,detail}),route:(frames)=>routed.push(...frames)});
const wasi={on(){},send:(bytes)=>sent.push(bytes),bufferedAmount:0,close(){}};
const node={id:1,proc:{connect:()=>wasi},servers:new Map(),clients:new Map(),waitingDials:[]};host.nodes.set(1,node);
const open={src:{node:4,port:0},dst:{node:1,port:9092},conn:1,payload:{kind:'open'}};
host.toListener(node,open); assert.equal(node.servers.size,0); assert.equal(sent.length,0);
host.toListener(node,{...open,principal:'node-7@LAB.KRABKA'}); assert.equal(node.servers.size,0);
host.toListener(node,{...open,principal:'node-4@LAB.KRABKA'});
const conn=[...node.servers.values()][0], application=saslRequest(17,123,new TextEncoder().encode('ignored'));
host.send(node,conn,application); assert.equal(sent.length,1);
host.fromProcess(node,conn,handshake); assert.equal(sent.length,2); assert.ok(conn.auth);
host.fromProcess(node,conn,authenticated); assert.equal(conn.auth,null); assert.equal(sent.length,2);
world.nodes[2].state.authorization.ready=true; host.at(0,[]);
assert.equal(sent.length,3); assert.deepEqual(sent[2],application);
world.nodes[2].state.authorization.ready=false; host.at(1,[]);
host.send(node,conn,application); assert.equal(sent.length,3);
world.nodes[2].state.authorization.ready=true; host.at(2,[]); assert.equal(sent.length,4);
assert.equal(events.filter((e)=>e.kind==='broker_authenticated').length,1);
assert.equal(routed.filter((f)=>f.payload.kind==='data').length,0);
// A stalled readiness gate, authentication, or runtime buffer has the same bounds.
host.flushOut();
for (const blocked of [{aclGate:true}, {auth:{}}, {}]) {
  for (const limit of ['bytes', 'frames']) {
    const resets=[];
    const blockedConn={key:`blocked-${limit}`,local:open.dst,peer:open.src,id:2,wasi:{...wasi,bufferedAmount:blocked.aclGate || blocked.auth ? 0 : 1<<20,close:(info)=>resets.push(info)},hold:[],held:0,done:false,...blocked};
    node.servers.set(blockedConn.key,blockedConn);
    const chunk=new Uint8Array(limit==='bytes' ? 1024*1024 : 1);
    const count=limit==='bytes' ? 100 : 2048;
    for (let i=0;i<count;i++) host.send(node,blockedConn,chunk);
    assert.equal(blockedConn.done,false); assert.equal(blockedConn.held,count*chunk.length);
    const before=sent.length, closes=routed.filter((f)=>f.payload.kind==='close').length;
    host.send(node,blockedConn,new Uint8Array(1)); host.flushOut();
    assert.equal(blockedConn.done,true); assert.equal(blockedConn.held,0); assert.equal(blockedConn.hold.length,0);
    assert.equal(node.servers.has(blockedConn.key),false);
    assert.deepEqual(resets,[{reset:true}]);
    assert.equal(routed.filter((f)=>f.payload.kind==='close').length,closes+1);
    host.send(node,blockedConn,chunk); host.flushHold(blockedConn);
    assert.equal(blockedConn.held,0); assert.equal(sent.length,before);
  }
}
// Refreshing a credential table must preserve a killed or paused broker.
node.identitySet='old'; node.state='killed';
world.nodes[0].hosted=true; world.nodes[0].alive=false;
const stopped=[],started=[];
host.stopProcess=(n)=>stopped.push(n.id); host.launch=(n)=>started.push({id:n.id,paused:n.paused});
host.sync(); assert.deepEqual(stopped,[1]); assert.deepEqual(started,[]);
world.nodes[0].alive=true; node.paused=true; world.nodes.push({id:8,kind:'consumer'});
host.sync(); assert.deepEqual(started,[{id:1,paused:true}]);
console.log('Verified SASL parsing, identity binding, bounded queues, and fail-closed ACL provisioning gate.');
