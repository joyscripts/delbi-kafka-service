import Database from "better-sqlite3";
import { config } from "../config/env";
import { logger } from "../utils/logger";
import * as fs from "fs";
import * as path from "path";

let db: Database.Database | null = null;

export const getDatabase = (): Database.Database => {
  if (db) {
    return db;
  }

  // Ensure data directory exists
  const dbDir = path.dirname(config.database.path);
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
    logger.info(`Created database directory: ${dbDir}`);
  }

  db = new Database(config.database.path);
  db.pragma("journal_mode = WAL"); // Enable Write-Ahead Logging for better concurrency

  logger.info(`Connected to database: ${config.database.path}`);

  // Initialize schema
  initializeSchema();

  return db;
};

const initializeSchema = () => {
  if (!db) return;

  // Tokens table: stores Expo push tokens and their Kafka subscriptions
  db.exec(`
    CREATE TABLE IF NOT EXISTS tokens (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      expoPushToken TEXT NOT NULL,
      username TEXT NOT NULL,
      topic TEXT NOT NULL,
      server TEXT NOT NULL,
      groupId TEXT NOT NULL,
      partition INTEGER NOT NULL,
      notificationType TEXT, -- 'h', 'd', 'm', or NULL
      registeredAt TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(expoPushToken, topic, notificationType)
    );

    CREATE INDEX IF NOT EXISTS idx_topic ON tokens(topic);
    CREATE INDEX IF NOT EXISTS idx_username ON tokens(username);
    CREATE INDEX IF NOT EXISTS idx_expoPushToken ON tokens(expoPushToken);
  `);

  logger.info("Database schema initialized");
};

export const closeDatabase = () => {
  if (db) {
    db.close();
    db = null;
    logger.info("Database connection closed");
  }
};

