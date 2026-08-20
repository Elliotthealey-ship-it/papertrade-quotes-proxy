const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

// Stocks/ETFs the app tracks live. Crypto stays simulated client-side.
const LIVE_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "SPY", "QQQ"];

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.get("/", (req, res) => {
  res.send("PaperTrade quotes proxy is running.");
});

// REST endpoint — used once on connect to seed today's open/previous-close
// (the WebSocket trade feed below only carries live price, not that).
app.get("/api/quotes", async (req, res) => {
  const symbolsParam = req.query.symbols;
  if (!symbolsParam) {
    return res.status(400).json({ error: "Missing symbols query parameter" });
  }
  const key = process.env.FINNHUB_KEY;
  if (!key) {
    return res.status(500).json({ error: "Server missing FINNHUB_KEY" });
  }
  const symbols = symbolsParam.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean).slice(0, 30);

  const quotes = await Promise.all(
    symbols.map(async (symbol) => {
      try {
        const r = await fetch(
          `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}&token=${key}`,
          {
            headers: {
              "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
              Accept: "application/json",
            },
          }
        );
        if (r.status === 401 || r.status === 403) return { symbol, error: "Invalid API key" };
        if (r.status === 429) return { symbol, error: "Rate limited" };
        if (!r.ok) return { symbol, error: `Upstream error (${r.status})` };
        const data = await r.json();
        if (!data || typeof data.c !== "number" || data.c <= 0) return { symbol, error: "No data" };
        return { symbol, c: data.c, pc: data.pc };
      } catch (err) {
        return { symbol, error: "Fetch failed: " + String(err) };
      }
    })
  );

  res.json({ quotes });
});

// --- Real-time relay ---------------------------------------------------
// One persistent connection to Finnhub's trade WebSocket (key stays here,
// server-side), fanned out to every connected browser client. Finnhub only
// sends a message when an actual trade happens, so updates only arrive
// while the relevant market is open and trading.

const clients = new Set();
const latest = {}; // symbol -> { price, t }
let finnhubSocket = null;
let reconnectDelay = 2000;

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

function connectFinnhub() {
  const key = process.env.FINNHUB_KEY;
  if (!key) {
    console.error("Missing FINNHUB_KEY — cannot start the live trade feed.");
    return;
  }
  finnhubSocket = new WebSocket(`wss://ws.finnhub.io?token=${key}`);

  finnhubSocket.on("open", () => {
    reconnectDelay = 2000;
    LIVE_SYMBOLS.forEach((symbol) => {
      finnhubSocket.send(JSON.stringify({ type: "subscribe", symbol }));
    });
    console.log(`Connected to Finnhub — subscribed to ${LIVE_SYMBOLS.length} symbols.`);
  });

  finnhubSocket.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "trade" && Array.isArray(msg.data)) {
        msg.data.forEach((t) => {
          latest[t.s] = { price: t.p, t: t.t };
          broadcast({ type: "trade", symbol: t.s, price: t.p, t: t.t });
        });
      }
    } catch (err) {
      // ignore malformed messages
    }
  });

  finnhubSocket.on("close", () => {
    console.log(`Finnhub connection closed — reconnecting in ${reconnectDelay}ms.`);
    setTimeout(connectFinnhub, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30000);
  });

  finnhubSocket.on("error", (err) => {
    console.error("Finnhub websocket error:", err.message);
  });
}
connectFinnhub();

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

// Keepalive: some hosts/proxies close a WebSocket after a period of no
// traffic. Finnhub only sends a message when a trade actually happens, so
// quiet moments in the market could otherwise look "idle" and get the
// connection dropped even though nothing is wrong. Pinging periodically
// keeps traffic flowing and lets us clean up genuinely dead connections.
function heartbeat() {
  this.isAlive = true;
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", heartbeat);
  clients.add(ws);
  ws.send(
    JSON.stringify({
      type: "snapshot",
      quotes: Object.entries(latest).map(([symbol, v]) => ({ symbol, price: v.price, t: v.t })),
    })
  );
  ws.on("close", () => clients.delete(ws));
});

const keepaliveInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on("close", () => clearInterval(keepaliveInterval));

server.listen(PORT, () => {
  console.log(`Quotes proxy (REST + WebSocket relay) listening on port ${PORT}`);
});
