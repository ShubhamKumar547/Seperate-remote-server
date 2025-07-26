const express = require("express");
const http = require("http");
require("dotenv").config();
const { Server } = require("socket.io");
const { io } = require("socket.io-client");

const app = express();

const server = http.createServer(app);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/active", (req, res) => {
  console.log("got the request");
  // res.status(200);
});

const ioSender = new Server(server, {
  cors: {
    origin: "*", // Adjust this to your frontend URL
    methods: ["GET", "POST"],
  },
});

ioSender.on("connection", (socket) => {
  console.log("✅ New client connected:", socket.id);

  socket.on("disconnect", () => {
    console.log("❌ Client disconnected:", socket.id);
  });
});

const ioListener = io(process.env.CANDLE_GENERATOR_URL);

// Configuration
const CANDLE_LIMIT = 120; // Store last 120 candles
const INTERVAL = "60s"; // 1-minute candles

// Data storage with complete history
const candleHistory = {
  BTCUSDT: [],
  ETHUSDT: [],
  SOLUSDT: [],
};

// Store complete candle data with original timestamps
function storeCandle(symbol, candle) {
  const buffer = candleHistory[symbol];
  buffer.push({
    open: parseFloat(candle.open),
    high: parseFloat(candle.high),
    low: parseFloat(candle.low),
    close: parseFloat(candle.close),
    volume: parseFloat(candle.volume),
    interval: candle.interval || INTERVAL,
    trades: candle.trades || 0,
    "candle-generated-timestamp": candle.endTimeISO, // From generator
  });

  // Maintain fixed size
  if (buffer.length > CANDLE_LIMIT) buffer.shift();
}

// Process incoming candles
function processCandle(symbol, message) {
  try {
    const candle = JSON.parse(message);
    storeCandle(symbol, candle);
    console.log(`📈 Stored ${symbol} candle: ${candle.endTimeISO}`);
  } catch (error) {
    console.error(`Error processing ${symbol} candle:`, error);
  }
}

// Initialize data processor
function startDataProcessor() {
  console.log("✅ Data Processor service started");

  ioListener.on("CANDLE_BTCUSDT_60s", (msg) => processCandle("BTCUSDT", msg));
  ioListener.on("CANDLE_ETHUSDT_60s", (msg) => processCandle("ETHUSDT", msg));
  ioListener.on("CANDLE_SOLUSDT_60s", (msg) => processCandle("SOLUSDT", msg));
}

// Format data as requested with last candle's generation time
function formatCandleData(symbol) {
  const candles = candleHistory[symbol];
  const lastCandle = candles.length > 0 ? candles[candles.length - 1] : null;

  return {
    open: candles.map((c) => c.open),
    high: candles.map((c) => c.high),
    low: candles.map((c) => c.low),
    close: candles.map((c) => c.close),
    volume: candles.map((c) => c.volume),
    interval: INTERVAL,
    trades: candles.map((c) => c.trades),
    "data-last-updated-timestamp": lastCandle
      ? lastCandle["candle-generated-timestamp"]
      : null,
  };
}

// WebSocket handler
ioSender.on("connection", (ws) => {
  console.log("New client connected");

  ws.on("message", (message) => {
    try {
      const msg = JSON.parse(message);

      if (msg.type === "get-candles") {
        const response = {
          BTCUSDT: formatCandleData("BTCUSDT"),
          ETHUSDT: formatCandleData("ETHUSDT"),
          SOLUSDT: formatCandleData("SOLUSDT"),
          server_timestamp: new Date().toISOString(), // When data was sent
        };
        ws.send(JSON.stringify(response));
      }
    } catch (error) {
      console.error("WebSocket error:", error);
      ws.send(JSON.stringify({ error: "Invalid request format" }));
    }
  });

  ws.on("close", () => console.log("Client disconnected"));
});

// Start services
startDataProcessor();
const PORT = process.env.PORT || 6000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`WebSocket server running on ws://localhost:${PORT}`);
});

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("🛑 Shutting down gracefully...");
  // await Promise.all([redisSub.quit(), redisPub.quit()]);
  server.close(() => {
    console.log("Server closed");
    process.exit(0);
  });
});

// requested data format

// {
//   "BTCUSDT": {
//     "open": [array of 120 values],
//     "high": [array of 120 values],
//     "low": [array of 120 values],
//     "close": [array of 120 values],
//     "volume": [array of 120 values],
//     "interval": "60s",
//     "trades": [array of 120 values],
//     "data-last-updated-timestamp": "2023-11-15T14:01:59.999Z" // Last candle's end time
//   },
//   "ETHUSDT": { ... },
//   "SOLUSDT": { ... },
//   "server_timestamp": "2023-11-15T14:02:03.456Z" // When data was sent
// }
