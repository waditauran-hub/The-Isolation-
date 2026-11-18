// Supervivientes1: servidor web + WebSocket nativo para Render (sin dependencias externas).
// Ejecutar: npm start
'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const PORT = Number(process.env.PORT) || 10000;
const HOST = '0.0.0.0';
const ROOT = __dirname;
const MAX_PLAYERS = 9;
const MAX_MESSAGE = 64 * 1024;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map();
let uid = 0, presenceDirty = false;
const mime = { '.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.svg':'image/svg+xml','.mp3':'audio/mpeg','.ogg':'audio/ogg','.webp':'image/webp','.ico':'image/x-icon' };
const httpServer = http.createServer((req,res)=>{
  const url = new URL(req.url || '/', 'http://localhost');
  if(req.method!=='GET'&&req.method!=='HEAD'){res.writeHead(405,{'Allow':'GET, HEAD'});return res.end('Method not allowed');}
  if(url.pathname==='/healthz'){res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});return res.end(JSON.stringify({ok:true,service:'supervivientes1',rooms:rooms.size}));}
  let pathname;try{pathname=decodeURIComponent(url.pathname)}catch{res.writeHead(400);return res.end('Bad request')}
  if(pathname==='/')pathname='/index.html';
  const file=path.resolve(ROOT,'.'+pathname);
  if(file!==path.join(ROOT,'index.html')&&!file.startsWith(ROOT+path.sep)){res.writeHead(403);return res.end('Forbidden')}
  fs.stat(file,(err,stat)=>{if(err||!stat.isFile()){res.writeHead(404);return res.end('Not found')}
    res.writeHead(200,{'Content-Type':mime[path.extname(file).toLowerCase()]||'application/octet-stream','Content-Length':stat.size,'Cache-Control':path.basename(file)==='index.html'?'no-cache':'public, max-age=3600','X-Content-Type-Options':'nosniff','Referrer-Policy':'strict-origin-when-cross-origin'});
    if(req.method==='HEAD')return res.end();fs.createReadStream(file).pipe(res);
  });
});
const clients=new Set();
function frame(opcode,payload=Buffer.alloc(0)){
  if(!Buffer.isBuffer(payload))payload=Buffer.from(payload);
  const n=payload.length;let header;
  if(n<126){header=Buffer.from([0x80|opcode,n]);}
  else if(n<65536){header=Buffer.alloc(4);header[0]=0x80|opcode;header[1]=126;header.writeUInt16BE(n,2);}
  else{header=Buffer.alloc(10);header[0]=0x80|opcode;header[1]=127;header.writeBigUInt64BE(BigInt(n),2);}
  return Buffer.concat([header,payload]);
}
function send(socket,obj){if(!socket.destroyed&&!socket.writableEnded)socket.write(frame(1,Buffer.from(JSON.stringify(obj))));}
function close(socket,code=1002){if(socket.destroyed)return;const p=Buffer.alloc(2);p.writeUInt16BE(code);try{socket.write(frame(8,p));}catch{}socket.end();}
function attachWebSocket(socket){
  socket.pid='p'+(++uid).toString(36)+Math.random().toString(36).slice(2,6);socket.rooms=new Set();socket.pres=null;socket.msgWindow=Date.now();socket.msgCount=0;socket.wsBuffer=Buffer.alloc(0);clients.add(socket);presenceDirty=true;
  socket.on('data',chunk=>{
    socket.wsBuffer=Buffer.concat([socket.wsBuffer,chunk]);
    while(socket.wsBuffer.length>=2){
      const b0=socket.wsBuffer[0],b1=socket.wsBuffer[1],fin=(b0&0x80)!==0,opcode=b0&0x0f,masked=(b1&0x80)!==0;
      if(!fin||!masked){close(socket);return;}
      let len=b1&0x7f,offset=2;
      if(len===126){if(socket.wsBuffer.length<4)return;len=socket.wsBuffer.readUInt16BE(2);offset=4;}
      else if(len===127){if(socket.wsBuffer.length<10)return;const big=socket.wsBuffer.readBigUInt64BE(2);if(big>BigInt(MAX_MESSAGE)){close(socket,1009);return;}len=Number(big);offset=10;}
      if(len>MAX_MESSAGE){close(socket,1009);return;}
      if(socket.wsBuffer.length<offset+4+len)return;
      const mask=socket.wsBuffer.subarray(offset,offset+4);offset+=4;
      const payload=Buffer.from(socket.wsBuffer.subarray(offset,offset+len));socket.wsBuffer=socket.wsBuffer.subarray(offset+len);
      for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4];
      if(opcode===8){close(socket,1000);return;}
      if(opcode===9){socket.write(frame(10,payload));continue;}
      if(opcode===10)continue;
      if(opcode!==1){close(socket);return;}
      handleMessage(socket,payload.toString('utf8'));
      if(socket.destroyed)return;
    }
  });
  socket.on('close',()=>{for(const room of [...socket.rooms])leaveRoom(socket,room);clients.delete(socket);presenceDirty=true;});
  socket.on('error',()=>{});
}
httpServer.on('upgrade',(req,socket)=>{
  const url=new URL(req.url||'/', 'http://localhost');
  const key=req.headers['sec-websocket-key'];
  if(url.pathname!=='/ws'||req.headers.upgrade?.toLowerCase()!=='websocket'||typeof key!=='string') {socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');return socket.destroy();}
  const accept=crypto.createHash('sha1').update(key+WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  attachWebSocket(socket);
});
const isObj=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const short=(v,n)=>typeof v==='string'&&v.length>0&&v.length<=n;
const finite=v=>typeof v==='number'&&Number.isFinite(v);
const safeSize=v=>{try{return Buffer.byteLength(JSON.stringify(v),'utf8')<=48*1024}catch{return false}};
const validRoom=r=>typeof r==='string'&&/^sv-[a-z0-9]{5}$/.test(r);
const cleanChat=(t,n)=>String(t).replace(/[\u0000-\u001f\u007f<>]/g,'').replace(/\s+/g,' ').trim().slice(0,n);
// chatMessage: el servidor valida el texto, decide nombre y rol (lobby / survivor / killer) con el último estado de la sala y lo reenvía a todos (incluido el remitente)
function handleChat(s,roomName,room,d){
 if(!isObj(d)||typeof d.m!=='string'||d.m.length>400)return;
 const now=Date.now();if(now-(s.chatAt||0)<350)return;s.chatAt=now;
 const st=room.state;const id=s.pids&&s.pids[roomName];if(!st||!id||!st.names[id])return;
 const m=cleanChat(d.m,100);if(!m)return;
 const r=st.st==='play'?(st.roles[id]==='k'?'k':'s'):'lobby';
 const out={id,n:cleanChat(st.names[id],14)||'JUGADOR',m,r,role:r==='k'?'killer':r==='s'?'survivor':'lobby',t:now};
 for(const target of room.clients)send(target,{op:'msg',room:roomName,type:'chatMessage',data:out,peer:s.pid});
}
function validAct(a){
 if(!isObj(a)||!short(a.id,40)||!['join','mv','chat','look','pick','conf','leave'].includes(a.t))return false;
 if(a.t==='join')return short(a.name,14)&&(a.sk==null||short(a.sk,64))&&(a.ch==null||short(a.ch,48))&&(a.looks==null||(isObj(a.looks)&&safeSize(a.looks)));
 if(a.t==='mv')return finite(a.x)&&finite(a.y)&&Math.abs(a.x)<=2&&Math.abs(a.y)<=2;
 if(a.t==='chat')return typeof a.m==='string'&&a.m.length<=140;
 if(a.t==='look')return isObj(a.looks)&&safeSize(a.looks);
 if(a.t==='pick')return short(a.c,48);
 return true;
}
function validPosition(d){return isObj(d)&&short(d.id,40)&&finite(d.x)&&finite(d.y)&&Math.abs(d.x)<=100000&&Math.abs(d.y)<=100000&&['f','dx','dy','h','fu','iv','mv','run','jp','sta','ex'].every(k=>d[k]==null||finite(d[k])||typeof d[k]==='boolean');}
function validGameAction(d){if(!isObj(d)||!short(d.u,40)||!short(d.t,16)||!['mopen','mclose','mdone','stun','atk','kab','esc','noise'].includes(d.t))return false;if(['mopen','mclose','mdone'].includes(d.t))return short(d.id,64);return safeSize(d);}
function validState(type,d){
 if(!isObj(d)||!safeSize(d))return false;
 if(type==='st')return short(d.code,5)&&isObj(d.pl)&&Object.keys(d.pl).length<=MAX_PLAYERS+1&&['lobby','sel','play'].includes(d.st)&&Array.isArray(d.chat)&&d.chat.length<=40;
 if(type==='gs')return finite(d.r)&&Array.isArray(d.p)&&d.p.length<=MAX_PLAYERS+1&&Array.isArray(d.m)&&d.m.length<=32&&Array.isArray(d.b)&&d.b.length<=100&&Array.isArray(d.g)&&d.g.length<=100;
 if(type==='deny')return short(d.to,40)&&typeof d.m==='string'&&d.m.length<=180;
 return false;
}
function validPresence(p){if(!isObj(p)||!isObj(p.r))return false;const r=p.r;return short(r.c,5)&&short(r.n,48)&&short(r.h,24)&&Number.isInteger(r.p)&&r.p>=0&&r.p<=MAX_PLAYERS&&short(r.m,48)&&short(r.md,8)&&short(r.s,24)&&typeof r.pub==='boolean';}
function leaveRoom(s,room){const entry=rooms.get(room);if(!entry)return;entry.clients.delete(s);s.rooms.delete(room);if(entry.host===s)entry.host=null;if(!entry.clients.size)rooms.delete(room);}
function handleMessage(s,raw){
 const now=Date.now();if(now-s.msgWindow>=1000){s.msgWindow=now;s.msgCount=0;}if(++s.msgCount>70||Buffer.byteLength(raw)>MAX_MESSAGE)return;
 let m;try{m=JSON.parse(raw)}catch{return;}if(!isObj(m)||typeof m.op!=='string')return;
 if(m.op==='presence'){if(!validPresence(m.p))return;s.pres=m.p;presenceDirty=true;return;}
 if(!validRoom(m.room))return;
 if(m.op==='join'){let room=rooms.get(m.room);if(!room){room={clients:new Set(),host:s};rooms.set(m.room,room);}if(s.rooms.has(m.room)||room.clients.size>=MAX_PLAYERS||s.rooms.size>=2)return;room.clients.add(s);s.rooms.add(m.room);return;}
 if(m.op==='leave'){if(s.rooms.has(m.room))leaveRoom(s,m.room);return;}
 if(m.op!=='emit'||!s.rooms.has(m.room)||!short(m.type,16))return;
 const room=rooms.get(m.room);if(!room||!isObj(m.data))return;
 if(m.type==='chatMessage')return handleChat(s,m.room,room,m.data);
 let ok=false;
 if(m.type==='act'&&s!==room.host)ok=validAct(m.data);
 else if(m.type==='gp'&&s!==room.host)ok=validPosition(m.data);
 else if(m.type==='ga'&&s!==room.host)ok=validGameAction(m.data);
 else if(['st','gs','deny'].includes(m.type)&&s===room.host)ok=validState(m.type,m.data);
 if(!ok)return;
 if(m.type==='act'&&m.data.t==='join'){s.pids=s.pids||{};if(!s.pids[m.room])s.pids[m.room]=m.data.id;}
 if(m.type==='st'){const d=m.data,names={};for(const k of Object.keys(d.pl))if(isObj(d.pl[k]))names[k]=typeof d.pl[k].name==='string'?d.pl[k].name:'';room.state={st:d.st,roles:isObj(d.roles)?d.roles:{},names};if(short(d.host,40)){s.pids=s.pids||{};s.pids[m.room]=d.host;}}
 for(const target of room.clients)if(target!==s)send(target,{op:'msg',room:m.room,type:m.type,data:m.data,peer:s.pid});
}
setInterval(()=>{if(!presenceDirty)return;presenceDirty=false;const list=[...clients].map(s=>({id:s.pid,presence:s.pres||null}));for(const s of clients)send(s,{op:'peers',list});},750).unref();
httpServer.listen(PORT,HOST,()=>console.log(`Supervivientes1 listo en puerto ${PORT}`));
httpServer.on('error',err=>{console.error('Error del servidor:',err);process.exitCode=1;});
