const express = require("express");
const http = require("http");
require("dotenv").config();

const { Server } = require("socket.io");
const { io } = require("socket.io-client");

const app = express();

const PORT = 5051;
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

const ioListener = io(process.env.DATA_STREAMS_URL);

// Configuration
const CANDLE_INTERVAL_SECONDS = 60; // 1-minute candles
const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

// Candle generation state
const candleState = {};

async function startCandleGenerator() {
  console.log("🕯️ Starting Candle Generator");

  try {
    // await Promise.all([redisSub.connect(), redisPub.connect()]);
    console.log("✅ Connected to Redis");

    // Initialize candle state for each symbol
    SYMBOLS.forEach((symbol) => {
      candleState[symbol] = {
        currentCandle: null,
        nextCandleTime: null,
        lastClose: null,
      };

      // Subscribe to trade channels
      ioListener.on(`TRADE_${symbol}`, (message) => {
        processTrade(symbol, JSON.parse(message));
      });
    });

    // Start candle interval scheduler
    setInterval(generateCandles, 1000); // Check every second

    console.log("👂 Listening for trades...");
  } catch (error) {
    console.error("❌ Failed to initialize:", error);
    process.exit(1);
  }
}

function processTrade(symbol, trade) {
  const { timestamp, price, quantity } = trade;

  // Initialize candle if needed
  if (!candleState[symbol].currentCandle) {
    const candleStart =
      Math.floor(timestamp / (CANDLE_INTERVAL_SECONDS * 1000)) *
      (CANDLE_INTERVAL_SECONDS * 1000);

    candleState[symbol] = {
      currentCandle: {
        symbol,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: quantity,
        startTime: candleStart,
        startTimeISO: new Date(candleStart).toISOString(),
        trades: 1,
        firstTradeId: trade.tradeId || null,
        firstTradeTime: timestamp,
        firstTradeTimeISO: new Date(timestamp).toISOString(),
      },
      nextCandleTime: candleStart + CANDLE_INTERVAL_SECONDS * 1000,
      lastClose: null,
    };
    return;
  }

  // Update current candle
  const candle = candleState[symbol].currentCandle;
  candle.high = Math.max(candle.high, price);
  candle.low = Math.min(candle.low, price);
  candle.close = price;
  candle.volume += quantity;
  candle.trades++;
  candle.lastTradeId = trade.tradeId || null;
  candle.lastTradeTime = timestamp;
  candle.lastTradeTimeISO = new Date(timestamp).toISOString();
}

function generateCandles() {
  const now = Date.now();

  SYMBOLS.forEach((symbol) => {
    const state = candleState[symbol];

    // If it's time to finalize the candle
    if (state.currentCandle && now >= state.nextCandleTime) {
      const completedCandle = {
        ...state.currentCandle,
        endTime: state.nextCandleTime - 1,
        endTimeISO: new Date(state.nextCandleTime - 1).toISOString(),
        interval: CANDLE_INTERVAL_SECONDS,
        generatedAt: now,
        generatedAtISO: new Date(now).toISOString(),
        // Calculate duration in milliseconds
        durationMs: state.nextCandleTime - 1 - state.currentCandle.startTime,
      };

      // Publish the completed candle
      ioSender.emit(
        `CANDLE_${symbol}_${CANDLE_INTERVAL_SECONDS}s`,
        JSON.stringify(completedCandle)
      );

      // console.log(`📊 New ${CANDLE_INTERVAL_SECONDS}s candle for ${symbol}:`, {
      //   time: completedCandle.startTimeISO,
      //   o: completedCandle.open,
      //   h: completedCandle.high,
      //   l: completedCandle.low,
      //   c: completedCandle.close,
      //   v: completedCandle.volume,
      //   trades: completedCandle.trades,
      //   duration: `${completedCandle.durationMs}ms`
      // });

      // Store last close and reset for next candle
      state.lastClose = completedCandle.close;
      state.currentCandle = null;
    }
  });
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  console.log("🛑 Shutting down candle generator...");
  // await Promise.all([redisSub.quit(), redisPub.quit()]);
  process.exit(0);
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

startCandleGenerator().catch(console.error);
