import cron from "node-cron";
import { TestNotificationService } from "./test-notification";
import { logger } from "../utils/logger";
import { config } from "../config/env";

/**
 * Test Cron Job Service
 * Sends test notifications every minute for testing purposes
 */
export class TestCronService {
  private static cronJob: ReturnType<typeof cron.schedule> | null = null;
  private static isEnabled = config.nodeEnv === "development";

  /**
   * Start the test cron job (runs every minute)
   * Only enabled in development mode by default
   */
  static start(): void {
    if (!this.isEnabled) {
      logger.info(
        "[Test Cron] Test cron job is disabled (only runs in development mode)"
      );
      return;
    }

    if (this.cronJob) {
      logger.warn("[Test Cron] Test cron job is already running");
      return;
    }

    // Run every minute: "* * * * *"
    this.cronJob = cron.schedule("* * * * *", async () => {
      logger.info("[Test Cron] Running test notification job...");
      try {
        const result = await TestNotificationService.sendTestNotification();
        if (result.success) {
          logger.info(
            `[Test Cron] Test notification sent successfully. Sent: ${result.sent}, Total tokens: ${result.total}`
          );
        } else {
          logger.warn(
            `[Test Cron] Test notification failed: ${result.message}`
          );
        }
      } catch (error) {
        logger.error("[Test Cron] Error in test cron job:", error);
      }
    });

    logger.info(
      "[Test Cron] Test cron job started - will send test notifications every minute"
    );
    logger.warn("[Test Cron] ⚠️  REMEMBER TO DISABLE THIS IN PRODUCTION!");
  }

  /**
   * Stop the test cron job
   */
  static stop(): void {
    if (this.cronJob) {
      this.cronJob.stop();
      this.cronJob = null;
      logger.info("[Test Cron] Test cron job stopped");
    }
  }

  /**
   * Enable the test cron job (even in production - use with caution!)
   */
  static enable(): void {
    this.isEnabled = true;
    logger.warn("[Test Cron] Test cron job manually enabled");
  }

  /**
   * Disable the test cron job
   */
  static disable(): void {
    this.isEnabled = false;
    if (this.cronJob) {
      this.stop();
    }
    logger.info("[Test Cron] Test cron job disabled");
  }
}
