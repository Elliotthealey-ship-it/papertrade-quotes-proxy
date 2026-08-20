const express = require("express");
const app = express();
const PORT = process.env.PORT || 3000;

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

app.get("/api/quotes", async (req, res) => {
  const symbolsParam = req.query.symbols;
  if (!symbolsParam) {
    return res.status(400).json({ error: "Missing symbols query parameter" });
  }

  const key = process.env.FINNHUB_KEY;
  if (!key) {
    return res.status(500).json({ error: "Server missing FINNHUB_KEY" });
  }

  const symbols = symbolsParam
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean)
    .slice(0, 30); // simple abuse guard

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
        if (r.status === 401 || r.status === 403) {
          return { symbol, error: "Invalid API key" };
        }
        if (r.status === 429) {
          const limit = r.headers.get("x-ratelimit-limit");
          const remaining = r.headers.get("x-ratelimit-remaining");
          const reset = r.headers.get("x-ratelimit-reset");
          return {
            symbol,
            error: `Rate limited (limit=${limit ?? "?"}, remaining=${remaining ?? "?"}, reset=${reset ?? "?"})`,
          };
        }
        if (!r.ok) {
          return { symbol, error: `Upstream error (${r.status})` };
        }
        const data = await r.json();
        if (!data || typeof data.c !== "number" || data.c <= 0) {
          return { symbol, error: "No data" };
        }
        return { symbol, c: data.c, pc: data.pc };
      } catch (err) {
        return { symbol, error: "Fetch failed: " + String(err) };
      }
    })
  );

  res.json({ quotes });
});

app.listen(PORT, () => {
  console.log(`Quotes proxy listening on port ${PORT}`);
});
