import { getDatabase } from "./database";
import { TokenRegistrationRequest, TokenRecord, TokenUnregisterRequest } from "../types";
import { logger } from "../utils/logger";

export class TokenManager {
  /**
   * Register a push token with Kafka subscription data
   */
  static registerToken(request: TokenRegistrationRequest): void {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        INSERT INTO tokens (
          expoPushToken, username, topic, server, groupId, partition, notificationType, registeredAt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(expoPushToken, topic, notificationType) DO UPDATE SET
          username = excluded.username,
          server = excluded.server,
          groupId = excluded.groupId,
          partition = excluded.partition,
          registeredAt = datetime('now')
      `);

      stmt.run(
        request.expoPushToken,
        request.username,
        request.subscriptionData.topic,
        request.subscriptionData.server,
        request.subscriptionData.groupId,
        request.subscriptionData.partition,
        request.notificationType || null
      );

      logger.info(`Token registered: ${request.expoPushToken.substring(0, 20)}... for user ${request.username}, topic ${request.subscriptionData.topic}`);
    } catch (error) {
      logger.error("Error registering token:", error);
      throw error;
    }
  }

  /**
   * Unregister a specific token
   */
  static unregisterToken(request: TokenUnregisterRequest): number {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        DELETE FROM tokens
        WHERE expoPushToken = ? AND username = ?
      `);

      const result = stmt.run(request.expoPushToken, request.username);
      logger.info(`Unregistered token: ${request.expoPushToken.substring(0, 20)}... for user ${request.username}`);
      return result.changes;
    } catch (error) {
      logger.error("Error unregistering token:", error);
      throw error;
    }
  }

  /**
   * Unregister all tokens for a user
   */
  static unregisterAllUserTokens(username: string): number {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        DELETE FROM tokens
        WHERE username = ?
      `);

      const result = stmt.run(username);
      logger.info(`Unregistered all tokens for user: ${username}`);
      return result.changes;
    } catch (error) {
      logger.error("Error unregistering all tokens:", error);
      throw error;
    }
  }

  /**
   * Get all tokens for a specific topic
   */
  static getTokensByTopic(topic: string): TokenRecord[] {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        SELECT * FROM tokens
        WHERE topic = ?
      `);

      const rows = stmt.all(topic) as TokenRecord[];
      return rows;
    } catch (error) {
      logger.error("Error getting tokens by topic:", error);
      throw error;
    }
  }

  /**
   * Get all tokens for a user
   */
  static getTokensByUsername(username: string): TokenRecord[] {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        SELECT * FROM tokens
        WHERE username = ?
      `);

      const rows = stmt.all(username) as TokenRecord[];
      return rows;
    } catch (error) {
      logger.error("Error getting tokens by username:", error);
      throw error;
    }
  }

  /**
   * Get all unique topics
   */
  static getAllTopics(): string[] {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        SELECT DISTINCT topic FROM tokens
      `);

      const rows = stmt.all() as { topic: string }[];
      return rows.map((row) => row.topic);
    } catch (error) {
      logger.error("Error getting all topics:", error);
      throw error;
    }
  }
}

