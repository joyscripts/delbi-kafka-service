import { getDatabase } from "./database";
import { logger } from "../utils/logger";

export interface NotificationRecord {
  id: number;
  username: string;
  title: string;
  message: string;
  type?: string;
  topic?: string;
  kafkaOffset?: string;
  isRead: number; // 0 or 1 (SQLite boolean)
  createdAt: string;
  readAt?: string;
}

export class NotificationManager {
  /**
   * Save a notification to the database
   */
  static saveNotification(
    username: string,
    title: string,
    message: string,
    type?: string,
    topic?: string,
    kafkaOffset?: string
  ): number {
    try {
      const db = getDatabase();
      const stmt = db.prepare(`
        INSERT INTO notifications (username, title, message, type, topic, kafkaOffset)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      const result = stmt.run(username, title, message, type || null, topic || null, kafkaOffset || null);
      logger.debug(`[NotificationManager] Saved notification for ${username}, ID: ${result.lastInsertRowid}`);
      return Number(result.lastInsertRowid);
    } catch (error) {
      logger.error("[NotificationManager] Error saving notification:", error);
      throw error;
    }
  }

  /**
   * Get notifications for a user with pagination
   */
  static getNotifications(
    username: string,
    page: number = 1,
    limit: number = 20
  ): { notifications: NotificationRecord[]; total: number; hasMore: boolean } {
    try {
      const db = getDatabase();
      const offset = (page - 1) * limit;

      // Get total count
      const countStmt = db.prepare(`
        SELECT COUNT(*) as total FROM notifications WHERE username = ?
      `);
      const total = (countStmt.get(username) as { total: number }).total;

      // Get paginated notifications
      const stmt = db.prepare(`
        SELECT * FROM notifications
        WHERE username = ?
        ORDER BY createdAt DESC
        LIMIT ? OFFSET ?
      `);
      
      const notifications = stmt.all(username, limit, offset) as NotificationRecord[];
      
      const hasMore = offset + notifications.length < total;

      return {
        notifications,
        total,
        hasMore,
      };
    } catch (error) {
      logger.error("[NotificationManager] Error fetching notifications:", error);
      throw error;
    }
  }

  /**
   * Mark notification as read
   */
  static markAsRead(notificationId: number, username: string): boolean {
    try {
      const db = getDatabase();
      const stmt = db.prepare(`
        UPDATE notifications
        SET isRead = 1, readAt = datetime('now')
        WHERE id = ? AND username = ?
      `);
      
      const result = stmt.run(notificationId, username);
      return result.changes > 0;
    } catch (error) {
      logger.error("[NotificationManager] Error marking notification as read:", error);
      throw error;
    }
  }

  /**
   * Mark all notifications as read for a user
   */
  static markAllAsRead(username: string): number {
    try {
      const db = getDatabase();
      const stmt = db.prepare(`
        UPDATE notifications
        SET isRead = 1, readAt = datetime('now')
        WHERE username = ? AND isRead = 0
      `);
      
      const result = stmt.run(username);
      return result.changes;
    } catch (error) {
      logger.error("[NotificationManager] Error marking all notifications as read:", error);
      throw error;
    }
  }

  /**
   * Get unread count for a user
   */
  static getUnreadCount(username: string): number {
    try {
      const db = getDatabase();
      const stmt = db.prepare(`
        SELECT COUNT(*) as count FROM notifications
        WHERE username = ? AND isRead = 0
      `);
      
      const result = stmt.get(username) as { count: number };
      return result.count;
    } catch (error) {
      logger.error("[NotificationManager] Error getting unread count:", error);
      return 0;
    }
  }
}
