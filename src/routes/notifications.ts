import { Router, Request, Response } from "express";
import { TokenManager } from "../services/token-manager";
import { NotificationManager } from "../services/notification-manager";
import {
  TokenRegistrationRequest,
  TokenUnregisterRequest,
} from "../types";
import { logger } from "../utils/logger";
import { KafkaConsumerService } from "../services/kafka-consumer";

const router = Router();
let kafkaConsumer: KafkaConsumerService | null = null;

/**
 * Set the Kafka consumer instance (called from index.ts)
 */
export const setKafkaConsumer = (consumer: KafkaConsumerService) => {
  kafkaConsumer = consumer;
};

/**
 * POST /api/notifications/register-token
 * Register Expo push token with Kafka subscription data
 */
router.post("/register-token", async (req: Request, res: Response) => {
  try {
    const request: TokenRegistrationRequest = req.body;

    // Validate request
    if (
      !request.expoPushToken ||
      !request.subscriptionData ||
      !request.username
    ) {
      return res.status(400).json({
        success: false,
        error: "Missing required fields: expoPushToken, subscriptionData, username",
      });
    }

    if (
      !request.subscriptionData.topic ||
      !request.subscriptionData.server ||
      !request.subscriptionData.groupId ||
      request.subscriptionData.partition === undefined
    ) {
      return res.status(400).json({
        success: false,
        error: "Invalid subscriptionData: missing topic, server, groupId, or partition",
      });
    }

    // Register token
    TokenManager.registerToken(request);

    // Check if this is a new topic and restart consumer if needed
    if (kafkaConsumer) {
      kafkaConsumer.checkAndRestartIfNeeded(request.subscriptionData.topic);
    } else {
      logger.warn("[Notifications] Kafka consumer not available, cannot check for new topics");
    }

    res.json({
      success: true,
      message: "Token registered successfully",
    });
  } catch (error) {
    logger.error("Error in register-token endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * POST /api/notifications/unregister-token
 * Unregister a specific push token
 */
router.post("/unregister-token", async (req: Request, res: Response) => {
  try {
    const request: TokenUnregisterRequest = req.body;

    // Validate request
    if (!request.expoPushToken || !request.username) {
      return res.status(400).json({
        success: false,
        error: "Missing required fields: expoPushToken, username",
      });
    }

    // Unregister token
    const deletedCount = TokenManager.unregisterToken(request);

    res.json({
      success: true,
      message: "Token unregistered successfully",
      deletedCount,
    });
  } catch (error) {
    logger.error("Error in unregister-token endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * POST /api/notifications/unregister-all
 * Unregister all tokens for a user
 */
router.post("/unregister-all", async (req: Request, res: Response) => {
  try {
    const { username } = req.body;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: username",
      });
    }

    // Unregister all tokens
    const deletedCount = TokenManager.unregisterAllUserTokens(username);

    res.json({
      success: true,
      message: "All tokens unregistered successfully",
      deletedCount,
    });
  } catch (error) {
    logger.error("Error in unregister-all endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * GET /api/notifications/subscriptions/:username
 * Get all subscriptions for a user (for debugging)
 */
router.get("/subscriptions/:username", async (req: Request, res: Response) => {
  try {
    const username = Array.isArray(req.params.username) 
      ? req.params.username[0] 
      : req.params.username;

    const tokens = TokenManager.getTokensByUsername(username);

    res.json({
      success: true,
      subscriptions: tokens,
    });
  } catch (error) {
    logger.error("Error in subscriptions endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * GET /api/notifications/list/:username
 * Get notifications for a user with pagination
 */
router.get("/list/:username", async (req: Request, res: Response) => {
  try {
    const username = Array.isArray(req.params.username) 
      ? req.params.username[0] 
      : req.params.username;
    
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 20;

    if (page < 1) {
      return res.status(400).json({
        success: false,
        error: "Page must be greater than 0",
      });
    }

    if (limit < 1 || limit > 100) {
      return res.status(400).json({
        success: false,
        error: "Limit must be between 1 and 100",
      });
    }

    const result = NotificationManager.getNotifications(username, page, limit);

    res.json({
      success: true,
      notifications: result.notifications.map(notif => ({
        id: String(notif.id),
        title: notif.title,
        message: notif.message,
        type: notif.type || undefined,
        timestamp: notif.createdAt,
        createdAt: notif.createdAt,
        isRead: notif.isRead === 1,
      })),
      total: result.total,
      hasMore: result.hasMore,
      nextPage: result.hasMore ? page + 1 : undefined,
    });
  } catch (error) {
    logger.error("Error in notifications list endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * POST /api/notifications/:id/view
 * Mark a notification as viewed/read
 */
router.post("/:id/view", async (req: Request, res: Response) => {
  try {
    const notificationId = parseInt(req.params.id as string);
    const { username } = req.body;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: username",
      });
    }

    if (isNaN(notificationId)) {
      return res.status(400).json({
        success: false,
        error: "Invalid notification ID",
      });
    }

    const updated = NotificationManager.markAsRead(notificationId, username);

    if (!updated) {
      return res.status(404).json({
        success: false,
        error: "Notification not found or already read",
      });
    }

    res.json({
      success: true,
      message: "Notification marked as read",
    });
  } catch (error) {
    logger.error("Error in mark notification as viewed endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * POST /api/notifications/mark-all-read
 * Mark all notifications as read for a user
 */
router.post("/mark-all-read", async (req: Request, res: Response) => {
  try {
    const { username } = req.body;

    if (!username) {
      return res.status(400).json({
        success: false,
        error: "Missing required field: username",
      });
    }

    const count = NotificationManager.markAllAsRead(username);

    res.json({
      success: true,
      message: "All notifications marked as read",
      count,
    });
  } catch (error) {
    logger.error("Error in mark all as read endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

/**
 * GET /api/notifications/unread-count/:username
 * Get unread notification count for a user
 */
router.get("/unread-count/:username", async (req: Request, res: Response) => {
  try {
    const username = Array.isArray(req.params.username) 
      ? req.params.username[0] 
      : req.params.username;

    const count = NotificationManager.getUnreadCount(username);

    res.json({
      success: true,
      count,
    });
  } catch (error) {
    logger.error("Error in unread count endpoint:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
    });
  }
});

export default router;

