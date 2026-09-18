const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 5000;
const server = http.createServer((req, res) => {
  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";
  const file = path.join(__dirname, urlPath);
  if (!file.startsWith(__dirname) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404); return res.end("Not found");
  }
  const ext = path.extname(file);
  const types = {".html":"text/html; charset=utf-8",".js":"text/javascript; charset=utf-8",".css":"text/css; charset=utf-8",".json":"application/json"};
  res.writeHead(200, {"Content-Type": types[ext] || "application/octet-stream", "Cache-Control":"no-cache"});
  fs.createReadStream(file).pipe(res);
});

const wss = new WebSocketServer({ server });
const waiting = [];
const matches = new Map();
const clients = new Map();

function cleanName(name) {
  const s = String(name || "Player").trim().slice(0,20);
  return s || "Player";
}
function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function removeFromQueue(ws) {
  const i = waiting.indexOf(ws);
  if (i >= 0) waiting.splice(i,1);
}
function winner(board) {
  const lines=[[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
  for(const [a,b,c] of lines) if(board[a] && board[a]===board[b] && board[a]===board[c]) return board[a];
  return board.includes("") ? null : "tie";
}
function finishMatch(match, result) {
  match.gameOver = true;
  send(match.x.ws,{type:"gameOver",board:match.board,result});
  send(match.o.ws,{type:"gameOver",board:match.board,result});
}
function createMatch(a,b) {
  removeFromQueue(a); removeFromQueue(b);
  const id = Math.random().toString(36).slice(2,10) + Date.now().toString(36);
  const x = {ws:a,name:clients.get(a).name,symbol:"X"};
  const o = {ws:b,name:clients.get(b).name,symbol:"O"};
  const match = {id,x,o,board:["","","","","","","","",""],turn:"X",gameOver:false,rematch:new Set()};
  matches.set(id,match);
  clients.get(a).matchId=id; clients.get(b).matchId=id;
  send(a,{type:"matchFound",matchId:id,opponentName:o.name,symbol:"X",yourTurn:true});
  send(b,{type:"matchFound",matchId:id,opponentName:x.name,symbol:"O",yourTurn:false});
}
function findWaitingPair() {
  while(waiting.length>=2) {
    const a=waiting.shift(), b=waiting.shift();
    if(!a || !b || a.readyState!==1 || b.readyState!==1) {
      if(a && a.readyState===1) waiting.unshift(a);
      if(b && b.readyState===1) waiting.unshift(b);
      continue;
    }
    if(a===b) { waiting.unshift(a); continue; }
    createMatch(a,b);
  }
}
function leaveMatch(ws, notify=true) {
  const c=clients.get(ws); if(!c || !c.matchId)return;
  const match=matches.get(c.matchId); if(!match){c.matchId=null;return;}
  const other = match.x.ws===ws ? match.o.ws : match.x.ws;
  matches.delete(match.id);
  c.matchId=null;
  if(other && clients.has(other)) clients.get(other).matchId=null;
  if(notify) send(other,{type:"opponentLeft"});
}
function startRematch(match) {
  match.board=["","","","","","","","",""];
  match.turn="X"; match.gameOver=false; match.rematch.clear();
  send(match.x.ws,{type:"rematchStart",matchId:match.id,symbol:"X",yourTurn:true});
  send(match.o.ws,{type:"rematchStart",matchId:match.id,symbol:"O",yourTurn:false});
}

wss.on("connection", ws => {
  clients.set(ws,{name:"Player",matchId:null});
  send(ws,{type:"serverReady"});
  ws.on("message", raw => {
    let m; try{m=JSON.parse(raw.toString())}catch{return send(ws,{type:"error",message:"Invalid message"})}
    const c=clients.get(ws);
    if(!c)return;

    if(m.type==="joinQueue"){
      c.name=cleanName(m.name);
      if(c.matchId)return send(ws,{type:"error",message:"You are already in a match."});
      removeFromQueue(ws);
      waiting.push(ws);
      send(ws,{type:"waiting"});
      findWaitingPair();
      return;
    }

    if(m.type==="leaveQueue"){removeFromQueue(ws);return}

    if(m.type==="leaveMatch"){
      if(c.matchId===m.matchId) leaveMatch(ws,true);
      return;
    }

    if(m.type==="move"){
      const match=matches.get(c.matchId);
      if(!match || match.id!==m.matchId)return send(ws,{type:"error",message:"Match not found."});
      if(match.gameOver)return;
      const player = match.x.ws===ws ? "X" : match.o.ws===ws ? "O" : null;
      if(!player)return send(ws,{type:"error",message:"You are not in this match."});
      const index=Number(m.index);
      if(!Number.isInteger(index)||index<0||index>8)return send(ws,{type:"error",message:"Invalid move."});
      if(player!==match.turn)return send(ws,{type:"error",message:"Not your turn."});
      if(match.board[index])return send(ws,{type:"error",message:"That cell is already occupied."});
      match.board[index]=player;
      const result=winner(match.board);
      if(result){finishMatch(match,result);return}
      match.turn=player==="X"?"O":"X";
      send(match.x.ws,{type:"move",board:match.board,nextTurn:match.turn,gameOver:false});
      send(match.o.ws,{type:"move",board:match.board,nextTurn:match.turn,gameOver:false});
      return;
    }

    if(m.type==="rematchRequest"){
      const match=matches.get(c.matchId);
      if(!match || !match.gameOver)return send(ws,{type:"error",message:"Rematch is available after the match ends."});
      match.rematch.add(ws);
      const other=match.x.ws===ws?match.o.ws:match.x.ws;
      send(other,{type:"rematchRequested"});
      return;
    }

    if(m.type==="rematchAccept"){
      const match=matches.get(c.matchId);
      if(!match || !match.gameOver)return send(ws,{type:"error",message:"Rematch is not available."});
      const other=match.x.ws===ws?match.o.ws:match.x.ws;
      if(!match.rematch.has(other))return send(ws,{type:"error",message:"No rematch request is waiting."});
      match.rematch.add(ws);
      if(match.rematch.size>=2)startRematch(match);
      return;
    }
  });

  ws.on("close",()=>{removeFromQueue(ws);leaveMatch(ws,true);clients.delete(ws)});
});

server.listen(PORT,()=>console.log(`OX server running on port ${PORT}`));
        
