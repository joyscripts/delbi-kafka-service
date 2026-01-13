import express from "express";
import cors from "cors";
import { config } from "./config/env";
import { logger } from "./utils/logger";
import { getDatabase, closeDatabase } from "./services/database";
import { KafkaConsumerService } from "./services/kafka-consumer";
import notificationRoutes from "./routes/notifications";
import healthRoutes from "./routes/health";

const app = express();
const kafkaConsumer = new KafkaConsumerService();

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use("/api/notifications", notificationRoutes);
app.use("/api/health", healthRoutes);

// Root route
app.get("/", (req, res) => {
  res.json({
    message: "Kafka Notification Service",
    version: "1.0.0",
  });
});

// Initialize database
getDatabase();

// Start Kafka consumer
kafkaConsumer.start().catch((error) => {
  logger.error("Failed to start Kafka consumer:", error);
  // Don't exit - the API can still work without Kafka
});

// Graceful shutdown
const shutdown = async () => {
  logger.info("Shutting down gracefully...");

  try {
    await kafkaConsumer.stop();
  } catch (error) {
    logger.error("Error stopping Kafka consumer:", error);
  }

  closeDatabase();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// Start server
const server = app.listen(config.port, () => {
  logger.info(`Server running on port ${config.port}`);
  logger.info(`Environment: ${config.nodeEnv}`);
});

// Handle unhandled promise rejections
process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", promise, "reason:", reason);
});

export default app;

