import { getDatabase } from "./database";
import { TokenRegistrationRequest, TokenRecord, TokenUnregisterRequest } from "../types";
import { logger } from "../utils/logger";

export class TokenManager {
  /**
   * Register a push token with Kafka subscription data
   * 
   * Flow:
   * 1. User A logs in with token A → INSERT (new entry)
   * 2. User A logs in again with token A → UPDATE (same user, re-registration)
   * 3. User B logs in with token A → INSERT (different user, allowed because User A should have logged out and deregistered)
   * 
   * Deduplication is based on (username, token, topic, notificationType).
   * Same token can exist for different users (they deregister on logout).
   */
  static registerToken(request: TokenRegistrationRequest): void {
    const db = getDatabase();

    try {
      // Check if this user already has this token for this topic/notificationType
      const checkStmt = db.prepare(`
        SELECT id FROM tokens
        WHERE username = ? AND expoPushToken = ? AND topic = ? AND notificationType = ?
      `);
      
      const existing = checkStmt.get(
        request.username,
        request.expoPushToken,
        request.subscriptionData.topic,
        request.notificationType || null
      ) as { id: number } | undefined;

      // Insert or update based on (username, token, topic, notificationType)
      // This allows:
      // - Same user re-registering → UPDATE
      // - Different user with same token → INSERT (new entry)
      const stmt = db.prepare(`
        INSERT INTO tokens (
          expoPushToken, username, topic, server, groupId, partition, notificationType, registeredAt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(username, expoPushToken, topic, notificationType) DO UPDATE SET
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

      if (existing) {
        logger.info(
          `Token re-registered: ${request.expoPushToken.substring(0, 20)}... ` +
          `for user ${request.username}, topic ${request.subscriptionData.topic}`
        );
      } else {
        logger.info(
          `Token registered: ${request.expoPushToken.substring(0, 20)}... ` +
          `for user ${request.username}, topic ${request.subscriptionData.topic}`
        );
      }
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
   * Delete a token by token string (for cleanup of invalid tokens)
   */
  static deleteTokenByTokenString(expoPushToken: string): number {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`
        DELETE FROM tokens
        WHERE expoPushToken = ?
      `);

      const result = stmt.run(expoPushToken);
      if (result.changes > 0) {
        logger.info(`Deleted invalid token: ${expoPushToken.substring(0, 20)}...`);
      }
      return result.changes;
    } catch (error) {
      logger.error("Error deleting token by token string:", error);
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

  /**
   * Clean up duplicate entries for the same user
   * Keeps the most recent entry for each (username, token, topic, notificationType) combination
   * This is useful for migration or edge cases where duplicates might exist
   */
  static cleanupDuplicateUserEntries(): number {
    const db = getDatabase();

    try {
      // Find and delete duplicate entries, keeping the most recent one
      const stmt = db.prepare(`
        DELETE FROM tokens
        WHERE id NOT IN (
          SELECT MAX(id)
          FROM tokens
          GROUP BY username, expoPushToken, topic, notificationType
        )
      `);

      const result = stmt.run();
      if (result.changes > 0) {
        logger.info(`Cleaned up ${result.changes} duplicate token entries`);
      }
      return result.changes;
    } catch (error) {
      logger.error("Error cleaning up duplicate entries:", error);
      throw error;
    }
  }
}

