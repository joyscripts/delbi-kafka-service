import { TokenManager } from "./token-manager";
import { PushNotificationService } from "./push-notification";
import { logger } from "../utils/logger";
import { getDatabase } from "./database";
import { TokenRecord } from "../types";

/**
 * Test Notification Service
 * Sends test notifications to all registered tokens for testing purposes
 */
export class TestNotificationService {
  /**
   * Get all registered tokens
   */
  private static getAllTokens(): TokenRecord[] {
    const db = getDatabase();

    try {
      const stmt = db.prepare(`SELECT * FROM tokens`);
      const rows = stmt.all() as TokenRecord[];
      return rows;
    } catch (error) {
      logger.error("Error getting all tokens:", error);
      return [];
    }
  }

  /**
   * Send test notification to all registered tokens
   */
  static async sendTestNotification(username?: string): Promise<{
    success: boolean;
    sent: number;
    total: number;
    message: string;
  }> {
    try {
      let tokens: TokenRecord[];

      if (username) {
        // Send to specific user's tokens
        tokens = TokenManager.getTokensByUsername(username);
        logger.info(
          `[Test Notification] Sending test notification to user: ${username}`
        );
      } else {
        // Send to all tokens
        tokens = this.getAllTokens();
        logger.info(
          `[Test Notification] Sending test notification to all registered tokens`
        );
      }

      if (tokens.length === 0) {
        const message = username
          ? `No tokens found for user: ${username}`
          : "No tokens registered in the system";
        logger.warn(`[Test Notification] ${message}`);
        return {
          success: false,
          sent: 0,
          total: 0,
          message,
        };
      }

      // Get unique tokens (users may have multiple devices/topics)
      const uniqueTokens = Array.from(
        new Set(tokens.map((t) => t.expoPushToken))
      );

      const timestamp = new Date().toLocaleString();
      const title = "🧪 Test Notification";
      const body = `This is a test notification sent at ${timestamp}`;

      // Send notification to all unique tokens
      await PushNotificationService.sendNotifications(
        uniqueTokens,
        title,
        body,
        {
          test: true,
          timestamp: new Date().toISOString(),
          username: username || "all",
          screen: "/protected",
        }
      );

      const message = username
        ? `Test notification sent to ${uniqueTokens.length} device(s) for user: ${username}`
        : `Test notification sent to ${uniqueTokens.length} device(s)`;

      logger.info(`[Test Notification] ${message}`);
      return {
        success: true,
        sent: uniqueTokens.length,
        total: tokens.length,
        message,
      };
    } catch (error) {
      logger.error(
        "[Test Notification] Error sending test notification:",
        error
      );
      return {
        success: false,
        sent: 0,
        total: 0,
        message: `Error: ${
          error instanceof Error ? error.message : "Unknown error"
        }`,
      };
    }
  }
}
