const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 5000;
const HOST = "0.0.0.0";
const ROOT = __dirname;
const INDEX_FILE = path.join(ROOT, "index.html");

// ============================================================
// SETTINGS
// ============================================================

const MAX_WS_PAYLOAD = 8192;

// Heartbeat keeps dead WebSocket connections from staying in RAM.
const HEARTBEAT_INTERVAL = 30000;

// Chat protection.
// Normal players won't notice this.
const CHAT_COOLDOWN = 200;

// General message protection.
const MESSAGE_WINDOW = 10000;
const MAX_MESSAGES_PER_WINDOW = 60;

// ============================================================
// HTTP SERVER
// ============================================================

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".webp": "image/webp"
};

// Cache index.html in RAM.
// This avoids synchronous disk reading on every game-page request.
let indexCache = null;

try {
  indexCache = fs.readFileSync(INDEX_FILE);
  console.log("index.html loaded into memory.");
} catch (err) {
  console.error("Could not load index.html:", err.message);
}

const server = http.createServer((req, res) => {
  let urlPath = (req.url || "/").split("?")[0];

  if (urlPath === "/") {
    urlPath = "/index.html";
  }

  let decoded;

  try {
    decoded = decodeURIComponent(urlPath);
  } catch {
    res.writeHead(400);
    return res.end("Bad request");
  }

  const file = path.resolve(ROOT, "." + decoded);

  // Security: prevent path traversal.
  if (
    !file.startsWith(ROOT + path.sep) &&
    file !== INDEX_FILE
  ) {
    res.writeHead(404);
    return res.end("Not found");
  }

  // Fast path for index.html.
  if (file === INDEX_FILE && indexCache) {
    res.writeHead(200, {
      "Content-Type": MIME[".html"],
      "Cache-Control": "no-cache",
      "Content-Length": indexCache.length
    });

    return res.end(indexCache);
  }

  fs.stat(file, (statErr, stats) => {
    if (statErr || !stats.isFile()) {
      res.writeHead(404);
      return res.end("Not found");
    }

    const ext = path.extname(file);

    res.writeHead(200, {
      "Content-Type": MIME[ext] || "application/octet-stream",
      "Cache-Control": "no-cache"
    });

    const stream = fs.createReadStream(file);

    stream.on("error", () => {
      if (!res.headersSent) {
        res.writeHead(500);
      }
      res.end("Server error");
    });

    stream.pipe(res);
  });
});

// ============================================================
// WEBSOCKET SERVER
// ============================================================

const wss = new WebSocketServer({
  server,

  // Compression disabled because OX messages are tiny.
  // This saves CPU and RAM when many players are connected.
  perMessageDeflate: false,

  clientTracking: true,

  // Current OX messages are very small.
  // This prevents abnormally large WebSocket messages.
  maxPayload: MAX_WS_PAYLOAD
});

// ============================================================
// DATA
// ============================================================

const waiting = [];
let waitingHead = 0;

// Allows fast removal from matchmaking queue.
const waitingIndex = new Map();

// All connected players.
const clients = new Map();

// Active matches.
const matches = new Map();

// ============================================================
// HELPERS
// ============================================================

function cleanName(name) {
  if (typeof name !== "string") {
    return "Player";
  }

  const s = name.trim().slice(0, 20);

  return s || "Player";
}

function cleanChatMessage(msg) {
  if (typeof msg !== "string") {
    return "";
  }

  return msg.trim().slice(0, 50);
}

function send(ws, obj) {
  if (!ws || ws.readyState !== 1) {
    return false;
  }

  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch {
    return false;
  }
}

function removeFromQueue(ws) {
  const index = waitingIndex.get(ws);

  if (index === undefined) {
    return;
  }

  waiting[index] = null;
  waitingIndex.delete(ws);

  compactWaitingQueue();
}

function addToQueue(ws) {
  if (!ws || ws.readyState !== 1) {
    return;
  }

  if (waitingIndex.has(ws)) {
    return;
  }

  const index = waiting.length;

  waiting.push(ws);
  waitingIndex.set(ws, index);
}

function compactWaitingQueue() {
  // Don't constantly rebuild the array.
  // Only compact after it has grown enough.
  if (
    waitingHead > 1024 &&
    waitingHead * 2 > waiting.length
  ) {
    const newQueue = [];

    for (let i = waitingHead; i < waiting.length; i++) {
      const ws = waiting[i];

      if (
        ws &&
        ws.readyState === 1 &&
        clients.has(ws)
      ) {
        waitingIndex.set(ws, newQueue.length);
        newQueue.push(ws);
      } else if (ws) {
        waitingIndex.delete(ws);
      }
    }

    waiting.length = 0;

    for (const ws of newQueue) {
      waiting.push(ws);
    }

    waitingHead = 0;
  }
}

function dequeuePlayer() {
  while (waitingHead < waiting.length) {
    const index = waitingHead++;
    const ws = waiting[index];

    if (!ws) {
      continue;
    }

    waitingIndex.delete(ws);
    waiting[index] = null;

    if (
      ws.readyState === 1 &&
      clients.has(ws)
    ) {
      return ws;
    }
  }

  compactWaitingQueue();

  return null;
}

function getPlayerSymbol(match, ws) {
  if (match.x.ws === ws) {
    return "X";
  }

  if (match.o.ws === ws) {
    return "O";
  }

  return null;
}

function getOpponent(match, ws) {
  if (match.x.ws === ws) {
    return match.o.ws;
  }

  if (match.o.ws === ws) {
    return match.x.ws;
  }

  return null;
}

// ============================================================
// GAME LOGIC
// ============================================================

const WINNING_LINES = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8],
  [0, 3, 6],
  [1, 4, 7],
  [2, 5, 8],
  [0, 4, 8],
  [2, 4, 6]
];

function winner(board) {
  for (const [a, b, c] of WINNING_LINES) {
    if (
      board[a] &&
      board[a] === board[b] &&
      board[a] === board[c]
    ) {
      return board[a];
    }
  }

  return board.includes("") ? null : "tie";
}

// ============================================================
// MATCH MANAGEMENT
// ============================================================

function createMatch(a, b) {
  removeFromQueue(a);
  removeFromQueue(b);

  const ac = clients.get(a);
  const bc = clients.get(b);

  if (!ac || !bc) {
    return;
  }

  if (
    a.readyState !== 1 ||
    b.readyState !== 1
  ) {
    return;
  }

  const id =
    crypto.randomBytes(8).toString("hex") +
    Date.now().toString(36);

  const match = {
    id,

    x: {
      ws: a,
      name: ac.name,
      symbol: "X"
    },

    o: {
      ws: b,
      name: bc.name,
      symbol: "O"
    },

    board: [
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      ""
    ],

    turn: "X",
    gameOver: false,

    // Players who requested/accepted rematch.
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
  while (true) {
    const a = dequeuePlayer();

    if (!a) {
      break;
    }

    const b = dequeuePlayer();

    if (!b) {
      // Put player back at the front.
      waiting[--waitingHead] = a;
      waitingIndex.set(a, waitingHead);
      break;
    }

    if (
      a.readyState !== 1 ||
      !clients.has(a)
    ) {
      continue;
    }

    if (
      b.readyState !== 1 ||
      !clients.has(b)
    ) {
      // Put valid player back.
      if (
        a.readyState === 1 &&
        clients.has(a)
      ) {
        waiting[--waitingHead] = a;
        waitingIndex.set(a, waitingHead);
      }

      continue;
    }

    createMatch(a, b);
  }

  compactWaitingQueue();
}

function finishMatch(match, result) {
  if (!match || match.gameOver) {
    return;
  }

  match.gameOver = true;

  const message = {
    type: "gameOver",
    board: match.board,
    result
  };

  send(match.x.ws, message);
  send(match.o.ws, message);
}

function leaveMatch(ws, notify = true) {
  const client = clients.get(ws);

  if (!client || !client.matchId) {
    return;
  }

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

  if (
    notify &&
    other &&
    other.readyState === 1
  ) {
    send(other, {
      type: "opponentLeft"
    });
  }
}

function startRematch(match) {
  if (!match) {
    return;
  }

  match.board = [
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    ""
  ];

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

// ============================================================
// CONNECTION
// ============================================================

wss.on("connection", ws => {
  // WebSocket heartbeat state.
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  const client = {
    name: "Player",
    matchId: null,

    // Chat anti-spam.
    lastChatAt: 0,

    // General message rate protection.
    messageWindowStart: Date.now(),
    messageCount: 0
  };

  clients.set(ws, client);

  send(ws, {
    type: "serverReady"
  });

  // ==========================================================
  // MESSAGE
  // ==========================================================

  ws.on("message", raw => {
    const currentClient = clients.get(ws);

    if (!currentClient) {
      return;
    }

    // General rate protection.
    const now = Date.now();

    if (
      now - currentClient.messageWindowStart >
      MESSAGE_WINDOW
    ) {
      currentClient.messageWindowStart = now;
      currentClient.messageCount = 0;
    }

    currentClient.messageCount++;

    if (
      currentClient.messageCount >
      MAX_MESSAGES_PER_WINDOW
    ) {
      return;
    }

    let m;

    try {
      m = JSON.parse(raw.toString());
    } catch {
      return send(ws, {
        type: "error",
        message: "Invalid message"
      });
    }

    if (!m || typeof m.type !== "string") {
      return;
    }

    // ========================================================
    // JOIN QUEUE
    // ========================================================

    if (m.type === "joinQueue") {
      currentClient.name = cleanName(m.name);

      if (currentClient.matchId) {
        return send(ws, {
          type: "error",
          message: "You are already in a match."
        });
      }

      removeFromQueue(ws);
      addToQueue(ws);

      send(ws, {
        type: "waiting"
      });

      findWaitingPair();

      return;
    }

    // ========================================================
    // LEAVE QUEUE
    // ========================================================

    if (m.type === "leaveQueue") {
      removeFromQueue(ws);
      return;
    }

    // ========================================================
    // PING
    // ========================================================

    if (m.type === "ping") {
      send(ws, {
        type: "pong"
      });

      return;
    }

    // ========================================================
    // LEAVE MATCH
    // ========================================================

    if (m.type === "leaveMatch") {
      if (
        currentClient.matchId &&
        currentClient.matchId === m.matchId
      ) {
        leaveMatch(ws, true);
      }

      return;
    }

    // ========================================================
    // CHAT
    // ========================================================

    if (m.type === "chat") {
      const match = matches.get(
        currentClient.matchId
      );

      if (
        !match ||
        match.id !== m.matchId
      ) {
        return;
      }

      // Small cooldown prevents chat flooding.
      if (
        now - currentClient.lastChatAt <
        CHAT_COOLDOWN
      ) {
        return;
      }

      const message = cleanChatMessage(
        m.message
      );

      if (!message) {
        return;
      }

      currentClient.lastChatAt = now;

      const opponent = getOpponent(
        match,
        ws
      );

      if (
        opponent &&
        opponent.readyState === 1
      ) {
        send(opponent, {
          type: "chat",
          message,
          from: currentClient.name
        });
      }

      return;
    }

    // ========================================================
    // MOVE
    // ========================================================

    if (m.type === "move") {
      const match = matches.get(
        currentClient.matchId
      );

      if (
        !match ||
        match.id !== m.matchId
      ) {
        return send(ws, {
          type: "error",
          message: "Match not found."
        });
      }

      if (match.gameOver) {
        return;
      }

      const player = getPlayerSymbol(
        match,
        ws
      );

      if (!player) {
        return send(ws, {
          type: "error",
          message: "You are not in this match."
        });
      }

      const index = Number(m.index);

      if (
        !Number.isInteger(index) ||
        index < 0 ||
        index > 8
      ) {
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

      // Server remains authoritative.
      match.board[index] = player;

      const result = winner(
        match.board
      );

      if (result) {
        finishMatch(
          match,
          result
        );

        return;
      }

      match.turn =
        player === "X"
          ? "O"
          : "X";

      const moveMessage = {
        type: "move",
        board: match.board,
        nextTurn: match.turn,
        gameOver: false
      };

      send(
        match.x.ws,
        moveMessage
      );

      send(
        match.o.ws,
        moveMessage
      );

      return;
    }

    // ========================================================
    // REMATCH REQUEST
    // ========================================================

    if (m.type === "rematchRequest") {
      const match = matches.get(
        currentClient.matchId
      );

      if (
        !match ||
        !match.gameOver
      ) {
        return send(ws, {
          type: "error",
          message:
            "Rematch is available after the match ends."
        });
      }

      // Don't repeatedly notify opponent.
      if (match.rematch.has(ws)) {
        return;
      }

      match.rematch.add(ws);

      const opponent = getOpponent(
        match,
        ws
      );

      if (opponent) {
        send(opponent, {
          type: "rematchRequested"
        });
      }

      return;
    }

    // ========================================================
    // REMATCH ACCEPT
    // ========================================================

    if (m.type === "rematchAccept") {
      const match = matches.get(
        currentClient.matchId
      );

      if (
        !match ||
        !match.gameOver
      ) {
        return send(ws, {
          type: "error",
          message:
            "Rematch is not available."
        });
      }

      const opponent = getOpponent(
        match,
        ws
      );

      if (
        !opponent ||
        !match.rematch.has(opponent)
      ) {
        return send(ws, {
          type: "error",
          message:
            "No rematch request is waiting."
        });
      }

      match.rematch.add(ws);

      if (match.rematch.size >= 2) {
        startRematch(match);
      }

      return;
    }
  });

  // ==========================================================
  // CLOSE
  // ==========================================================

  ws.on("close", () => {
    removeFromQueue(ws);

    leaveMatch(ws, true);

    clients.delete(ws);
  });

  // ==========================================================
  // ERROR
  // ==========================================================

  ws.on("error", err => {
    // Don't crash the Node process because of one bad socket.
    console.error(
      "WebSocket error:",
      err.message
    );

    removeFromQueue(ws);
  });
});

// ============================================================
// HEARTBEAT
// ============================================================

const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try {
        ws.terminate();
      } catch {}

      continue;
    }

    ws.isAlive = false;

    try {
      ws.ping();
    } catch {}
  }
}, HEARTBEAT_INTERVAL);

// Don't keep Node alive only because of this timer.
if (heartbeatTimer.unref) {
  heartbeatTimer.unref();
}

// ============================================================
// SERVER START
// ============================================================

server.listen(
  PORT,
  HOST,
  () => {
    console.log(
      `OX server running on ${HOST}:${PORT}`
    );

    console.log(
      "Multiplayer WebSocket server is ready."
    );
  }
);

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

let shuttingDown = false;

function shutdown() {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  console.log(
    "OX server shutting down..."
  );

  clearInterval(
    heartbeatTimer
  );

  for (const ws of wss.clients) {
    try {
      ws.close(
        1001,
        "Server shutting down"
      );
    } catch {}
  }

  server.close(() => {
    process.exit(0);
  });

  // Safety fallback.
  setTimeout(() => {
    process.exit(0);
  }, 5000).unref();
}

process.on(
  "SIGTERM",
  shutdown
);

process.on(
  "SIGINT",
  shutdown
);
