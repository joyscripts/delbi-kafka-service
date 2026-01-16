import express from "express";
import cors from "cors";
import { config } from "./config/env";
import { logger } from "./utils/logger";
import { getDatabase, closeDatabase } from "./services/database";
import { KafkaConsumerService } from "./services/kafka-consumer";
import notificationRoutes from "./routes/notifications";
import healthRoutes, { setKafkaConsumer } from "./routes/health";

const app = express();
const kafkaConsumer = new KafkaConsumerService();

// Make Kafka consumer available to health check
setKafkaConsumer(kafkaConsumer);

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

// Start Kafka consumer with retry logic
const startKafkaConsumer = async (retryCount = 0, maxRetries = 5) => {
  try {
    await kafkaConsumer.start();
    logger.info("Kafka consumer started successfully");
  } catch (error) {
    logger.error(`Failed to start Kafka consumer (attempt ${retryCount + 1}/${maxRetries}):`, error);
    
    if (retryCount < maxRetries) {
      const delay = Math.min(1000 * Math.pow(2, retryCount), 30000); // Exponential backoff, max 30s
      logger.info(`Retrying Kafka consumer start in ${delay}ms...`);
      setTimeout(() => {
        startKafkaConsumer(retryCount + 1, maxRetries);
      }, delay);
    } else {
      logger.error("Max retries reached. Kafka consumer will not start. API will continue without Kafka.");
      logger.error("Please check:");
      logger.error("1. Kafka broker is running and accessible");
      logger.error("2. Network connectivity to broker");
      logger.error("3. Firewall rules allow port 9092");
      logger.error(`4. Broker address: ${config.kafka.broker}`);
    }
  }
};

startKafkaConsumer();

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

