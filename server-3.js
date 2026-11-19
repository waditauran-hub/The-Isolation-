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
 const R=room.R,st=R?{st:R.st,roles:R.roles,names:Object.fromEntries(Object.entries(R.pl).map(([k,p])=>[k,p.name]))}:room.state;const id=s.pids&&s.pids[roomName];if(!st||!id||!st.names[id])return;
 const m=cleanChat(d.m,100);if(!m)return;
 const r=st.st==='play'?(st.roles[id]==='k'?'k':'s'):'lobby';
 const out={id,n:cleanChat(st.names[id],14)||'JUGADOR',m,r,role:r==='k'?'killer':r==='s'?'survivor':'lobby',t:now};
 if(R&&R.pl[id]){R.pl[id].say=m;R.pl[id].sayT=now}
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
/* ===== AUTORIDAD CENTRAL DE SALA (Lobby -> Data Select -> Cinemática -> Partida) =====
   El servidor decide los cambios de escena y los envía a TODOS los sockets de la sala al mismo tiempo.
   La simulación dentro de la ronda sigue en el anfitrión (gs/gp/ga), pero el flujo de la sala ya no depende de él. */
const COUNTDOWN_MS=20000, SELECT_MS=10000, MAX_NAME=14;
const MAPS=['Bosque maldito','Fábrica abandonada','Salón de juegos'];
const SURV_IDS=['val','dan','zoe','sam','abr','nic','lia','tom'], KILL_IDS=['cos','enl','pay','doc','wlf'], ALL_IDS=SURV_IDS.concat(KILL_IDS);
const pick1=a=>a[Math.floor(Math.random()*a.length)];
const humans=R=>Object.keys(R.pl).filter(i=>!R.pl[i].bot);
function newPl(name,sk,looks,ch){return{name:cleanChat(name||'',MAX_NAME)||'JUGADOR',x:.2+Math.random()*.6,y:.3+Math.random()*.6,sk:short(sk,64)?sk:'basic',looks:isObj(looks)&&safeSize(looks)?looks:{},ch:short(ch,48)?ch:'val',say:'',sayT:0}}
function rsys(R,m){R.chat.push({s:1,m,t:Date.now()});R.chat=R.chat.slice(-40)}
function rmsg(room,roomName,type,data,only){for(const t of room.clients)if(!only||t===only)send(t,{op:'msg',room:roomName,type,data,peer:'server'})}
function rdeny(room,roomName,s,id,m){rmsg(room,roomName,'deny',{to:id,m},s)}
function rollRound(R){const ids=humans(R);R.nextMap=pick1(MAPS);R.map=R.nextMap;R.botKiller=!!R.botMode;const prev=R.hist.length?(R.hist[R.hist.length-1].ids||[]):[];let pool=ids.filter(i=>!prev.includes(i));if(!pool.length)pool=ids.slice();R.nextKiller=R.botKiller?null:(pool.length?pick1(pool):null);R.kid=null;R.killerId=null}
function createRoomState(roomName,s,d){
 const code=roomName.slice(3).toUpperCase();if(!short(d.code,5)||d.code.toUpperCase()!==code||!short(d.host,40)||!isObj(d.pl)||!isObj(d.pl[d.host]))return null;
 const hp=d.pl[d.host],R={code,nm:cleanChat(d.nm||'',48)||('Sala de '+cleanChat(hp.name||'',MAX_NAME)),host:d.host,pub:d.pub!==false,map:MAPS[0],nextMap:MAPS[0],nextKiller:null,killerId:null,mode:d.mode==='r'?'r':'s',nk:1,st:'lobby',pl:{},votes:{},mapVotes:{},roles:{},picks:{},conf:{},hist:[],chat:[],ready:false,cdEnd:0,cd:null,selEnd:0,selLeft:null,solo:false,botMode:!!d.botMode,botKiller:false,kid:null,closed:false,srv:1,pp:{},now:0};
 R.pl[d.host]=newPl(hp.name,hp.sk,hp.looks,hp.ch);R.pp[d.host]=s.pid;s.pids=s.pids||{};s.pids[roomName]=d.host;
 rsys(R,'Sala creada. Comparte el código '+code+'.');rollRound(R);return R}
function roomBroadcast(room,roomName){const R=room.R;if(!R)return;const now=Date.now();R.now=now;R.cd=R.st==='lobby'&&R.cdEnd?Math.max(0,R.cdEnd-now):null;R.selLeft=R.st==='sel'&&R.selEnd?Math.max(0,R.selEnd-now):null;rmsg(room,roomName,'st',R)}
function selUpdate(room,roomName){const R=room.R;rmsg(room,roomName,'characterSelectUpdate',{picks:R.picks,conf:R.conf,roles:R.roles,selLeft:R.selEnd?Math.max(0,R.selEnd-Date.now()):0})}
function startCharacterSelect(room,roomName){const R=room.R,ids=humans(R);if(!ids.length)return;
 for(const id of Object.keys(R.pl))if(R.pl[id].bot)delete R.pl[id];
 R.votes={};R.mapVotes={};R.roles={};R.picks={};R.conf={};R.killerId=null;R.kid=null;
 if(R.mode==='s'){
  if(R.botMode){R.kid=pick1(KILL_IDS);R.botKiller=true;R.nextKiller=null;ids.forEach(i=>R.roles[i]='s');const bid='botk';R.pl[bid]={name:'BOT / IA',x:.5,y:.5,sk:'basic',looks:{},ch:R.kid,say:'',sayT:0,bot:1};R.roles[bid]='k';R.picks[bid]=R.kid;R.conf[bid]=1;R.killerId=bid;R.hist.push({ids:[],names:['BOT / IA']})}
  else{const prev=R.hist.length?R.hist[R.hist.length-1].ids||[]:[];let pool=ids.filter(i=>!prev.includes(i));if(!pool.length)pool=ids.slice();R.killerId=R.nextKiller&&pool.includes(R.nextKiller)?R.nextKiller:pick1(pool);R.nextKiller=R.killerId;ids.forEach(i=>R.roles[i]=i===R.killerId?'k':'s');R.hist.push({ids:[R.killerId],names:[R.pl[R.killerId].name]})}
 }else{ids.forEach(i=>R.roles[i]='s');R.nextKiller=pick1(ids)}
 // cada superviviente arranca con un personaje libre distinto (sin choques); solo falta confirmar
 for(const id of ids){if(R.roles[id]==='k'||R.picks[id])continue;const pref=R.pl[id].ch,list=roleList(R,id),used=new Set(Object.values(R.picks));const c=list.includes(pref)&&!used.has(pref)?pref:list.find(x=>!used.has(x));if(c){R.picks[id]=c;R.pl[id].ch=c}}
 R.hist=R.hist.slice(-20);R.st='sel';R.ready=false;R.cdEnd=0;R.cd=null;R.selEnd=Date.now()+SELECT_MS;
 rsys(R,'DATA SELECT: 10 segundos para elegir. El asesino debe confirmar su personaje.');
 roomBroadcast(room,roomName);
 rmsg(room,roomName,'startCharacterSelect',{killerId:R.killerId,killerIsBot:!!(R.killerId&&R.pl[R.killerId]&&R.pl[R.killerId].bot),killerCharacter:R.kid,map:R.map,players:Object.keys(R.pl).map(id=>({id,name:R.pl[id].name,role:R.roles[id]||'s',bot:!!R.pl[id].bot})),time:SELECT_MS,state:R})}
function startMatch(room,roomName,why){const R=room.R;R.st='play';R.selEnd=0;R.selLeft=null;rsys(R,why);roomBroadcast(room,roomName);rmsg(room,roomName,'startMatch',{map:R.map,picks:R.picks,roles:R.roles})}
function cancelSelect(room,roomName,why){const R=room.R;R.st='lobby';R.ready=false;R.cdEnd=0;R.cd=null;R.selEnd=0;R.picks={};R.conf={};R.roles={};R.killerId=null;R.votes={};R.mapVotes={};for(const id of Object.keys(R.pl))if(R.pl[id].bot)delete R.pl[id];rollRound(R);rsys(R,why);roomBroadcast(room,roomName)}
function roleList(R,id){return R.mode==='r'?ALL_IDS:R.roles[id]==='k'?KILL_IDS:SURV_IDS}
function checkAllConfirmed(room,roomName){const R=room.R;if(R.st!=='sel')return;const ids=Object.keys(R.pl);if(ids.length&&ids.every(i=>R.conf[i]&&R.picks[i]))startMatch(room,roomName,'Todos confirmaron. ¡Comienza la ronda!')}
function selectCharacter(room,roomName,s,id,c,confirmed){const R=room.R;if(R.st!=='sel'||!R.pl[id])return;
 if(c!=null&&c!==R.picks[id]){if(R.conf[id])return;if(!roleList(R,id).includes(c))return rdeny(room,roomName,s,id,'Ese personaje no es de tu rol');
  if(Object.entries(R.picks).some(([k,v])=>v===c&&k!==id))return rdeny(room,roomName,s,id,'Personaje ocupado');R.picks[id]=c;R.pl[id].ch=c}
 if(confirmed&&R.picks[id])R.conf[id]=1;
 selUpdate(room,roomName);checkAllConfirmed(room,roomName);roomBroadcast(room,roomName)}
function dropPlayer(room,roomName,id,why){const R=room.R;const p=R&&R.pl[id];if(!p)return;delete R.pl[id];delete R.votes[id];delete R.picks[id];delete R.conf[id];delete R.roles[id];delete R.pp[id];rsys(R,p.name+why);
 if(R.st==='sel'){if(id===R.killerId)cancelSelect(room,roomName,'El asesino salió. Se sortean nuevo asesino y mapa.');else checkAllConfirmed(room,roomName)}
 else if(R.st==='lobby'&&R.nextKiller===id)rollRound(R)}
function roomAct(room,roomName,s,a){const R=room.R;
 if(a.t==='join'){const id=a.id,nm=cleanChat(a.name||'',MAX_NAME);
  if(R.closed)return rdeny(room,roomName,s,id,'Sala cerrada');if(R.st!=='lobby')return rdeny(room,roomName,s,id,'La partida ya comenzó');
  if(R.pl[id])return;if(humans(R).length>=MAX_PLAYERS)return rdeny(room,roomName,s,id,'SALA COMPLETA');
  if(!nm||Object.values(R.pl).some(p=>p.name.toLowerCase()===nm.toLowerCase()))return rdeny(room,roomName,s,id,'Ese nombre está vacío o ya se usa en la sala');
  R.pl[id]=newPl(nm,a.sk,a.looks,a.ch);R.pp[id]=s.pid;s.pids=s.pids||{};s.pids[roomName]=id;rsys(R,nm+' entró');rollRound(R);return roomBroadcast(room,roomName)}
 const id=a.id;if(!s.pids||s.pids[roomName]!==id||!R.pl[id])return;const p=R.pl[id];
 if(a.t==='mv'){p.x=Math.min(.97,Math.max(.03,+a.x||0));p.y=Math.min(.97,Math.max(.03,+a.y||0));return}
 if(a.t==='chat'){const m=cleanChat(a.m||'',140);if(m){p.say=m;p.sayT=Date.now();R.chat.push({id,n:p.name,m,t:Date.now()});R.chat=R.chat.slice(-40)}}
 else if(a.t==='look'){if(isObj(a.looks)&&safeSize(a.looks))p.looks=a.looks}
 else if(a.t==='pick')return selectCharacter(room,roomName,s,id,a.c,false);
 else if(a.t==='conf')return selectCharacter(room,roomName,s,id,null,true);
 else if(a.t==='leave'){dropPlayer(room,roomName,id,' salió')}
 roomBroadcast(room,roomName)}
function roomAdmin(room,roomName,s,d){const R=room.R;if(s!==room.host||!isObj(d))return;
 if(d.a==='bot'){R.botMode=!!d.v;if(R.st==='lobby'){R.cdEnd=0;rollRound(R)}rsys(R,R.botMode?'MODO BOT activado: todos son supervivientes contra el asesino IA.':'MODO BOT desactivado: un jugador será el asesino.')}
 else if(d.a==='kick'&&short(d.id,40)&&d.id!==R.host&&R.pl[d.id]){for(const t of room.clients)if(t.pids&&t.pids[roomName]===d.id)rdeny(room,roomName,t,d.id,'Fuiste expulsado/a por el anfitrión');dropPlayer(room,roomName,d.id,' fue expulsado')}
 else if(d.a==='lobby'){R.st='lobby';R.ready=false;R.cdEnd=0;R.cd=null;R.selEnd=0;R.picks={};R.conf={};R.roles={};R.votes={};R.mapVotes={};R.killerId=null;R.kid=null;R.botKiller=false;for(const id of Object.keys(R.pl))if(R.pl[id].bot)delete R.pl[id];rollRound(R)}
 else if(d.a==='pub'){R.pub=!!d.v}
 roomBroadcast(room,roomName)}
function roomTick(room,roomName){const R=room.R;if(!R||R.closed)return;const now=Date.now();
 if(R.st==='lobby'){const ok=humans(R).length>=2;R.ready=ok;if(ok&&!R.cdEnd)R.cdEnd=now+COUNTDOWN_MS;if(!ok)R.cdEnd=0;if(R.cdEnd&&now>=R.cdEnd)return startCharacterSelect(room,roomName)}
 else if(R.st==='sel'&&R.selEnd&&now>=R.selEnd){const kid=R.killerId;
  if(kid&&R.pl[kid]&&!R.pl[kid].bot&&!R.picks[kid])return cancelSelect(room,roomName,'Selección cancelada: '+R.pl[kid].name+' no eligió asesino a tiempo. Se sortearon nuevo asesino y mapa.');
  for(const id of Object.keys(R.pl)){if(!R.picks[id]){const free=roleList(R,id).find(c=>!Object.entries(R.picks).some(([k,v])=>k!==id&&v===c));if(free){R.picks[id]=free;R.pl[id].ch=free}}R.conf[id]=1}
  selUpdate(room,roomName);return startMatch(room,roomName,'Tiempo terminado. Comienza la ronda.')}
 roomBroadcast(room,roomName);
 if(room.host){const pr={r:{c:R.code,n:R.nm.slice(0,48),h:(R.pl[R.host]&&R.pl[R.host].name||'Anfitrión').slice(0,24),p:humans(R).length,m:R.map,md:R.mode,s:R.st==='lobby'?'esperando':'en partida',pub:!!R.pub}},js=JSON.stringify(pr);if(js!==room.presJs){room.presJs=js;room.host.pres=pr;presenceDirty=true}}}
setInterval(()=>{for(const [name,room] of rooms)if(room.R)roomTick(room,name)},200).unref();
function leaveRoom(s,room){const entry=rooms.get(room);if(!entry)return;entry.clients.delete(s);s.rooms.delete(room);
 if(entry.R){const R=entry.R;if(entry.host===s){R.closed=true;rmsg(entry,room,'st',R);if(s.pres)s.pres=null;presenceDirty=true;entry.R=null}else{const id=s.pids&&s.pids[room];if(id&&R.pl[id]&&R.pp[id]===s.pid){dropPlayer(entry,room,id,' se desconectó');roomBroadcast(entry,room)}}}
 if(s.pids)delete s.pids[room];if(entry.host===s)entry.host=null;if(!entry.clients.size)rooms.delete(room);}
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
 // LAST MAN STANDING: el anfitrión avisa y el servidor lo anuncia a TODA la sala (también a quienes ya volvieron al Lobby)
 if(m.type==='lms'){const d=m.data;if(s===room.host&&isObj(d)&&short(d.c,8)&&isFinite(+d.d)&&isFinite(+d.o)){const out={c:d.c,d:Math.max(0,Math.min(600,+d.d)),o:Math.max(0,Math.min(600,+d.o)),t:Math.max(0,Math.min(600,+d.t||0))};room.lms={...out,at:Date.now()};for(const t of room.clients)send(t,{op:'msg',room:m.room,type:'lmsStarted',data:out,peer:'server'})}return}
 // flujo de sala con autoridad del servidor
 if(m.type==='st'&&s===room.host&&!room.R){if(!validState('st',m.data))return;const R=createRoomState(m.room,s,m.data);if(R){room.R=R;roomBroadcast(room,m.room)}return}
 if(room.R){
  if(m.type==='act'){if(validAct(m.data))roomAct(room,m.room,s,m.data);return}
  if(m.type==='selectCharacter'){const d=m.data,id=s.pids&&s.pids[m.room];if(id&&(d.characterId==null||short(d.characterId,48)))selectCharacter(room,m.room,s,id,d.characterId==null?null:d.characterId,!!d.confirmed);return}
  if(m.type==='adm'){roomAdmin(room,m.room,s,m.data);return}
  if(m.type==='st'||m.type==='deny')return;
 }
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
