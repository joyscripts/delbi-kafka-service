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

  // Check if table exists with old schema (without username in UNIQUE constraint)
  const tableInfo = db.prepare(`
    SELECT sql FROM sqlite_master 
    WHERE type='table' AND name='tokens'
  `).get() as { sql: string } | undefined;

  const hasOldSchema = tableInfo && 
    tableInfo.sql.includes("UNIQUE(expoPushToken, topic, notificationType)") &&
    !tableInfo.sql.includes("UNIQUE(username, expoPushToken, topic, notificationType)");

  if (hasOldSchema) {
    logger.warn("Detected old database schema. Migrating to new schema...");
    // Create new table with correct schema
    db.exec(`
      CREATE TABLE IF NOT EXISTS tokens_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        expoPushToken TEXT NOT NULL,
        username TEXT NOT NULL,
        topic TEXT NOT NULL,
        server TEXT NOT NULL,
        groupId TEXT NOT NULL,
        partition INTEGER NOT NULL,
        notificationType TEXT,
        registeredAt TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(username, expoPushToken, topic, notificationType)
      );

      -- Migrate data, keeping only one entry per (username, token, topic, notificationType)
      INSERT OR IGNORE INTO tokens_new 
      SELECT * FROM tokens
      WHERE id IN (
        SELECT MAX(id) 
        FROM tokens 
        GROUP BY username, expoPushToken, topic, notificationType
      );

      -- Drop old table and rename new one
      DROP TABLE tokens;
      ALTER TABLE tokens_new RENAME TO tokens;
    `);
    logger.info("Database migration completed");
  } else {
    // Create table with new schema (or if it already exists with correct schema, this is a no-op)
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
        UNIQUE(username, expoPushToken, topic, notificationType)
      );
    `);
  }

  // Create indexes (these are idempotent)
  db.exec(`
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

