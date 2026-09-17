const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const HOST = "0.0.0.0";

const server = http.createServer((req, res) => {
    if (req.url === "/" || req.url === "/index.html") {
        const filePath = path.join(__dirname, "index.html");

        fs.readFile(filePath, (err, data) => {
            if (err) {
                res.writeHead(500, { "Content-Type": "text/plain" });
                res.end("Could not load index.html");
                return;
            }

            res.writeHead(200, {
                "Content-Type": "text/html; charset=utf-8",
                "Cache-Control": "no-cache"
            });
            res.end(data);
        });
        return;
    }

    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
});

const wss = new WebSocket.Server({ server });

const waitingQueue = [];
const queuedPlayers = new Set();
const matches = new Map();

function send(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function removeFromQueue(ws) {
    queuedPlayers.delete(ws);

    const index = waitingQueue.indexOf(ws);
    if (index !== -1) {
        waitingQueue.splice(index, 1);
    }
}

function getMatchForPlayer(ws) {
    for (const match of matches.values()) {
        if (match.players.some(player => player.ws === ws)) {
            return match;
        }
    }
    return null;
}

function createMatch(player1, player2) {
    removeFromQueue(player1.ws);
    removeFromQueue(player2.ws);

    const matchId = crypto.randomUUID();

    // Randomly decide who gets X
    const firstIsX = Math.random() < 0.5;

    const p1 = {
        ws: player1.ws,
        name: player1.name,
        symbol: firstIsX ? "X" : "O"
    };

    const p2 = {
        ws: player2.ws,
        name: player2.name,
        symbol: firstIsX ? "O" : "X"
    };

    const match = {
        id: matchId,
        players: [p1, p2],
        board: ["", "", "", "", "", "", "", ""],
        turn: "X",
        gameOver: false
    };

    matches.set(matchId, match);

    send(p1.ws, {
        type: "matchFound",
        matchId,
        opponentName: p2.name,
        symbol: p1.symbol,
        yourTurn: p1.symbol === "X"
    });

    send(p2.ws, {
        type: "matchFound",
        matchId,
        opponentName: p1.name,
        symbol: p2.symbol,
        yourTurn: p2.symbol === "X"
    });
}

function addToQueue(ws, name) {
    if (ws.readyState !== WebSocket.OPEN) return;

    name = String(name || "Player").trim().slice(0, 20) || "Player";

    // Already waiting
    if (queuedPlayers.has(ws)) {
        send(ws, { type: "waiting" });
        return;
    }

    // Ignore duplicate joinQueue if player is already in a match
    if (getMatchForPlayer(ws)) {
        return;
    }

    const player = { ws, name };

    // Find another connected player
    while (waitingQueue.length > 0) {
        const opponentWs = waitingQueue.shift();

        if (
            opponentWs &&
            opponentWs !== ws &&
            opponentWs.readyState === WebSocket.OPEN &&
            queuedPlayers.has(opponentWs)
        ) {
            queuedPlayers.delete(opponentWs);
            createMatch(player, {
                ws: opponentWs,
                name: opponentWs.__playerName || "Player"
            });
            return;
        }
    }

    ws.__playerName = name;
    waitingQueue.push(ws);
    queuedPlayers.add(ws);

    send(ws, { type: "waiting" });
}

function handleMove(ws, msg) {
    const match = matches.get(msg.matchId);

    if (!match) {
        send(ws, {
            type: "error",
            message: "Match not found."
        });
        return;
    }

    if (match.gameOver) return;

    const player = match.players.find(p => p.ws === ws);

    if (!player) {
        send(ws, {
            type: "error",
            message: "You are not part of this match."
        });
        return;
    }

    if (player.symbol !== match.turn) {
        send(ws, {
            type: "error",
            message: "Not your turn."
        });
        return;
    }

    const index = Number(msg.index);

    if (!Number.isInteger(index) || index < 0 || index > 8) {
        send(ws, {
            type: "error",
            message: "Invalid move."
        });
        return;
    }

    if (match.board[index] !== "") {
        send(ws, {
            type: "error",
            message: "That cell is already occupied."
        });
        return;
    }

    match.board[index] = player.symbol;

    const winner = checkWinner(match.board);

    if (winner) {
        match.gameOver = true;

        for (const p of match.players) {
            send(p.ws, {
                type: "move",
                board: match.board,
                nextTurn: winner,
                gameOver: true
            });
        }

        // IMPORTANT:
        // Your existing index.html expects result to be X/O/draw.
        for (const p of match.players) {
            send(p.ws, {
                type: "gameOver",
                board: match.board,
                result: winner
            });
        }

        return;
    }

    if (!match.board.includes("")) {
        match.gameOver = true;

        for (const p of match.players) {
            send(p.ws, {
                type: "move",
                board: match.board,
                nextTurn: null,
                gameOver: true
            });

            send(p.ws, {
                type: "gameOver",
                board: match.board,
                result: "draw"
            });
        }

        return;
    }

    match.turn = player.symbol === "X" ? "O" : "X";

    for (const p of match.players) {
        send(p.ws, {
            type: "move",
            board: match.board,
            nextTurn: match.turn,
            gameOver: false
        });
    }
}

function checkWinner(board) {
    const conditions = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [0, 3, 6],
        [1, 4, 7],
        [2, 5, 8],
        [0, 4, 8],
        [2, 4, 6]
    ];

    for (const [a, b, c] of conditions) {
        if (
            board[a] &&
            board[a] === board[b] &&
            board[a] === board[c]
        ) {
            return board[a];
        }
    }

    return null;
}

function leaveMatch(ws, matchId) {
    const match = matches.get(matchId);

    if (!match) return;

    const isPlayer = match.players.some(p => p.ws === ws);

    if (!isPlayer) return;

    matches.delete(matchId);

    const opponent = match.players.find(p => p.ws !== ws);

    if (opponent) {
        send(opponent.ws, {
            type: "opponentLeft"
        });
    }
}

wss.on("connection", ws => {
    ws.__playerName = "Player";

    ws.on("message", rawMessage => {
        let msg;

        try {
            msg = JSON.parse(rawMessage.toString());
        } catch {
            send(ws, {
                type: "error",
                message: "Invalid message."
            });
            return;
        }

        if (msg.type === "joinQueue") {
            addToQueue(ws, msg.name);
            return;
        }

        if (msg.type === "move") {
            handleMove(ws, msg);
            return;
        }

        if (msg.type === "leaveMatch") {
            leaveMatch(ws, msg.matchId);
            return;
        }
    });

    ws.on("close", () => {
        removeFromQueue(ws);

        const match = getMatchForPlayer(ws);

        if (match) {
            matches.delete(match.id);

            const opponent = match.players.find(p => p.ws !== ws);

            if (opponent) {
                send(opponent.ws, {
                    type: "opponentLeft"
                });
            }
        }
    });
});

server.listen(PORT, HOST, () => {
    console.log(`OX server running on port ${PORT}`);
});