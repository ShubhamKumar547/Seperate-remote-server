const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { Server } = require("socket.io");

const { LRUCache } = require("lru-cache");

const app = express();

const PORT = 3000;
const server = http.createServer(app);

app.get("/active", (req, res) => {
  console.log("got the request");
  // res.status(200);
});

const ioSender = new Server(server, {
  cors: {
    origin: "*", 
    methods: ["GET", "POST"],
  },
});

ioSender.on("connection", (socket) => {
  console.log("✅ New client connected:", socket.id);

  socket.on("disconnect", () => {
    console.log("❌ Client disconnected:", socket.id);
  });
});


const lastProcessedTrades = new LRUCache({
  max: 121, // max number of items
  ttl: 1000 * 60 * 10, // time to live in ms (optional)
});

async function startDataStream() {
  console.log("🚀 Starting Binance WebSocket Data Stream");

  try {
    // await redisPub.connect();
    // console.log("✅ Connected to Redis");

    // Define coins and their streams
    const symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    const streams = symbols.map((s) => `${s.toLowerCase()}@aggTrade`).join("/");
    const wsUrl = `wss://stream.binance.com:9443/stream?streams=${streams}`;

    const ws = new WebSocket(wsUrl);

    ws.on("open", () => {
      console.log("🔗 Connected to Binance WebSocket");
    });

    ws.on("message", (raw) => {
      try {
        let msg = JSON.parse(raw);
        // console.log(msg);
        if (!msg.data || !msg.stream) return;

        const {
          s: symbol,
          p: priceStr,
          q: quantityStr,
          T: timestamp,
          a: tradeId,
          m: isBuyerMaker,
        } = msg.data;

        
        if (!symbol || !priceStr || !quantityStr || !timestamp || !tradeId) {
          console.warn("⚠️ Incomplete trade data:", msg.data);
          return;
        }

        
        const price = parseFloat(priceStr);
        const quantity = parseFloat(quantityStr);

       
        if (lastProcessedTrades[tradeId]) return;
        lastProcessedTrades[tradeId] = true;

      
        const tradeData = {
          event: "aggTrade",
          symbol,
          price,
          quantity,
          timestamp,
          tradeId,
          isBuyerMaker,
          exchange: "binance",
          receivedAt: Date.now(),
        };
        // console.log(tradeData);

        
        ioSender.emit(`TRADE_${symbol}`, JSON.stringify(tradeData));
      } catch (error) {
        console.error("❌ Trade processing error:", error);
      }
    });

    ws.on("error", (err) => {
      console.error("❌ WebSocket error:", err.message);
      ioSender.emit("SYSTEM_STATUS", "WEBSOCKET_ERROR");
    });

    ws.on("close", () => {
      console.warn("🔌 WebSocket disconnected. Reconnecting in 3s...");
      ioSender.emit("SYSTEM_STATUS", "WEBSOCKET_DISCONNECTED");
      setTimeout(startDataStream, 20000);
    });

  
    process.on("SIGINT", async () => {
      console.log("🛑 Gracefully shutting down...");
      ws.close();

      process.exit(0);
    });
  } catch (error) {
    console.error("❌ Initialization failed:", error);
    process.exit(1);
  }
}
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

startDataStream().catch(console.error);
