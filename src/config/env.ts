import dotenv from "dotenv";

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || "3000", 10),
  nodeEnv: process.env.NODE_ENV || "development",
  kafka: {
    broker: process.env.KAFKA_BROKER || "delopt.interactivedns.com:9092",
    clientId: process.env.KAFKA_CLIENT_ID || "kafka-notification-service",
  },
  database: {
    path: process.env.DATABASE_PATH || "./data/notifications.db",
  },
  logLevel: process.env.LOG_LEVEL || "info",
};

