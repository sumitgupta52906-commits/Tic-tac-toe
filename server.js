const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 5000;
const ROOT = __dirname;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json"
};

const server = http.createServer((req, res) => {
  let urlPath = (req.url || "/").split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";

  let decoded;
  try {
    decoded = decodeURIComponent(urlPath);
  } catch {
    res.writeHead(400);
    return res.end("Bad request");
  }

  const file = path.resolve(ROOT, "." + decoded);

  if (
    !file.startsWith(ROOT + path.sep) &&
    file !== path.join(ROOT, "index.html")
  ) {
    res.writeHead(404);
    return res.end("Not found");
  }

  if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404);
    return res.end("Not found");
  }

  const ext = path.extname(file);
  res.writeHead(200, {
    "Content-Type": MIME[ext] || "application/octet-stream",
    "Cache-Control": "no-cache"
  });

  fs.createReadStream(file).pipe(res);
});

const wss = new WebSocketServer({
  server,
  perMessageDeflate: false,
  clientTracking: true
});

const waiting = [];
const matches = new Map();
const clients = new Map();

function cleanName(name) {
  const s = String(name || "Player").trim().slice(0, 20);
  return s || "Player";
}

function cleanChatMessage(msg) {
  return String(msg || "").trim().slice(0, 50);
}

function send(ws, obj) {
  if (!ws || ws.readyState !== 1) return false;
  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch {
    return false;
  }
}

function removeFromQueue(ws) {
  const i = waiting.indexOf(ws);
  if (i !== -1) waiting.splice(i, 1);
}

function getPlayerSymbol(match, ws) {
  if (match.x.ws === ws) return "X";
  if (match.o.ws === ws) return "O";
  return null;
}

function getOpponent(match, ws) {
  return match.x.ws === ws ? match.o.ws : match.x.ws;
}

const WINNING_LINES = [
  [0,1,2],[3,4,5],[6,7,8],
  [0,3,6],[1,4,7],[2,5,8],
  [0,4,8],[2,4,6]
];

function winner(board) {
  for (const [a,b,c] of WINNING_LINES) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a];
    }
  }
  return board.includes("") ? null : "tie";
}

function finishMatch(match, result) {
  match.gameOver = true;
  const message = { type: "gameOver", board: match.board, result };
  send(match.x.ws, message);
  send(match.o.ws, message);
}

function createMatch(a, b) {
  removeFromQueue(a);
  removeFromQueue(b);

  const ac = clients.get(a);
  const bc = clients.get(b);
  if (!ac || !bc) return;

  const id = Math.random().toString(36).slice(2, 10) + Date.now().toString(36);

  const match = {
    id,
    x: { ws: a, name: ac.name, symbol: "X" },
    o: { ws: b, name: bc.name, symbol: "O" },
    board: ["","","","","","","","",""],
    turn: "X",
    gameOver: false,
    rematch: new Set()
  };

  matches.set(id, match);
  ac.matchId = id;
  bc.matchId = id;

  send(a, {
    type: "matchFound",
    matchId: id,
    opponentName: match.o.name,
    symbol: "X",
    yourTurn: true
  });

  send(b, {
    type: "matchFound",
    matchId: id,
    opponentName: match.x.name,
    symbol: "O",
    yourTurn: false
  });
}

function findWaitingPair() {
  // Remove closed sockets first, then pair in FIFO order.
  for (let i = waiting.length - 1; i >= 0; i--) {
    if (!waiting[i] || waiting[i].readyState !== 1) waiting.splice(i, 1);
  }

  while (waiting.length >= 2) {
    const a = waiting.shift();
    const b = waiting.shift();

    if (a && b && a !== b && a.readyState === 1 && b.readyState === 1) {
      createMatch(a, b);
    } else {
      if (a && a.readyState === 1) waiting.unshift(a);
      if (b && b.readyState === 1 && b !== a) waiting.unshift(b);
      break;
    }
  }
}

function leaveMatch(ws, notify = true) {
  const client = clients.get(ws);
  if (!client || !client.matchId) return;

  const match = matches.get(client.matchId);
  if (!match) {
    client.matchId = null;
    return;
  }

  const other = getOpponent(match, ws);
  matches.delete(match.id);
  client.matchId = null;

  if (other && clients.has(other)) {
    clients.get(other).matchId = null;
  }

  if (notify && other && other.readyState === 1) {
    send(other, { type: "opponentLeft" });
  }
}

function startRematch(match) {
  match.board = ["","","","","","","","",""];
  match.turn = "X";
  match.gameOver = false;
  match.rematch.clear();

  send(match.x.ws, {
    type: "rematchStart",
    matchId: match.id,
    symbol: "X",
    yourTurn: true
  });

  send(match.o.ws, {
    type: "rematchStart",
    matchId: match.id,
    symbol: "O",
    yourTurn: false
  });
}

wss.on("connection", ws => {
  clients.set(ws, { name: "Player", matchId: null });

  send(ws, { type: "serverReady" });

  ws.on("message", raw => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "Invalid message" });
    }

    const client = clients.get(ws);
    if (!client) return;

    if (m.type === "joinQueue") {
      client.name = cleanName(m.name);

      if (client.matchId) {
        return send(ws, {
          type: "error",
          message: "You are already in a match."
        });
      }

      removeFromQueue(ws);
      waiting.push(ws);
      send(ws, { type: "waiting" });
      findWaitingPair();
      return;
    }

    if (m.type === "leaveQueue") {
      removeFromQueue(ws);
      return;
    }

    if (m.type === "ping") {
      send(ws, { type: "pong" });
      return;
    }

    if (m.type === "leaveMatch") {
      if (client.matchId === m.matchId) leaveMatch(ws, true);
      return;
    }

    if (m.type === "chat") {
      const match = matches.get(client.matchId);
      if (!match || match.id !== m.matchId) return;

      const message = cleanChatMessage(m.message);
      if (!message) return;

      const opponent = getOpponent(match, ws);
      if (opponent && opponent.readyState === 1) {
        send(opponent, {
          type: "chat",
          message,
          from: client.name
        });
      }
      return;
    }

    if (m.type === "move") {
      const match = matches.get(client.matchId);

      if (!match || match.id !== m.matchId) {
        return send(ws, {
          type: "error",
          message: "Match not found."
        });
      }

      if (match.gameOver) return;

      const player = getPlayerSymbol(match, ws);
      if (!player) {
        return send(ws, {
          type: "error",
          message: "You are not in this match."
        });
      }

      const index = Number(m.index);
      if (!Number.isInteger(index) || index < 0 || index > 8) {
        return send(ws, {
          type: "error",
          message: "Invalid move."
        });
      }

      if (player !== match.turn) {
        return send(ws, {
          type: "error",
          message: "Not your turn."
        });
      }

      if (match.board[index]) {
        return send(ws, {
          type: "error",
          message: "That cell is already occupied."
        });
      }

      // No timer, polling, artificial delay, or database round-trip.
      match.board[index] = player;

      const result = winner(match.board);
      if (result) {
        finishMatch(match, result);
        return;
      }

      match.turn = player === "X" ? "O" : "X";

      const moveMessage = {
        type: "move",
        board: match.board,
        nextTurn: match.turn,
        gameOver: false
      };

      send(match.x.ws, moveMessage);
      send(match.o.ws, moveMessage);
      return;
    }

    if (m.type === "rematchRequest") {
      const match = matches.get(client.matchId);

      if (!match || !match.gameOver) {
        return send(ws, {
          type: "error",
          message: "Rematch is available after the match ends."
        });
      }

      match.rematch.add(ws);

      const opponent = getOpponent(match, ws);
      send(opponent, { type: "rematchRequested" });
      return;
    }

    if (m.type === "rematchAccept") {
      const match = matches.get(client.matchId);

      if (!match || !match.gameOver) {
        return send(ws, {
          type: "error",
          message: "Rematch is not available."
        });
      }

      const opponent = getOpponent(match, ws);

      if (!match.rematch.has(opponent)) {
        return send(ws, {
          type: "error",
          message: "No rematch request is waiting."
        });
      }

      match.rematch.add(ws);

      if (match.rematch.size >= 2) {
        startRematch(match);
      }
      return;
    }
  });

  ws.on("close", () => {
    removeFromQueue(ws);
    leaveMatch(ws, true);
    clients.delete(ws);
  });

  ws.on("error", () => {
    removeFromQueue(ws);
  });
});

server.listen(PORT, () => {
  console.log(`OX server running on port ${PORT}`);
})
              ;
