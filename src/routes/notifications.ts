import { Router, Request, Response } from "express";
import { TokenManager } from "../services/token-manager";
import {
  TokenRegistrationRequest,
  TokenUnregisterRequest,
} from "../types";
import { logger } from "../utils/logger";

const router = Router();

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

export default router;

