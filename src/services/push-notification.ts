import { Expo } from "expo-server-sdk";
import { TokenRecord } from "../types";
import { logger } from "../utils/logger";
import { TokenManager } from "./token-manager";

// Create a new Expo SDK client
const expo = new Expo();

export class PushNotificationService {
  /**
   * Send push notification to a list of tokens
   */
  static async sendNotifications(
    tokens: string[],
    title: string,
    body: string,
    data?: Record<string, any>
  ): Promise<void> {
    if (tokens.length === 0) {
      logger.warn("No tokens provided for push notification");
      return;
    }

    // Filter out invalid Expo push tokens
    const validTokens = tokens.filter((token) => Expo.isExpoPushToken(token));

    if (validTokens.length === 0) {
      logger.warn("No valid Expo push tokens found");
      return;
    }

    if (validTokens.length < tokens.length) {
      logger.warn(
        `Filtered out ${tokens.length - validTokens.length} invalid tokens`
      );
    }

    // Create messages
    const messages = validTokens.map((token) => ({
      to: token,
      sound: "default" as const,
      title,
      body,
      data: data || {},
    }));

    // Send in chunks (Expo allows up to 100 messages per request)
    const chunks = expo.chunkPushNotifications(messages);
    const tickets = [];

    try {
      logger.info(
        `[Expo] Sending ${validTokens.length} notifications in ${chunks.length} chunk(s)`
      );

      for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
        const chunk = chunks[chunkIndex];
        try {
          logger.info(
            `[Expo] Sending chunk ${chunkIndex + 1}/${chunks.length} with ${
              chunk.length
            } notification(s)`
          );
          const ticketChunk = await expo.sendPushNotificationsAsync(chunk);
          tickets.push(...ticketChunk);

          // Log Expo response for this chunk
          logger.info(
            `[Expo] Received response for chunk ${chunkIndex + 1}:`,
            JSON.stringify(ticketChunk, null, 2)
          );
        } catch (error) {
          logger.error(
            `[Expo] Error sending push notification chunk ${chunkIndex + 1}:`,
            error
          );
        }
      }

      // Log all tickets for debugging
      logger.info(`[Expo] Total tickets received: ${tickets.length}`);

      // Check for errors in tickets
      const errors: any[] = [];
      const success: any[] = [];

      for (let i = 0; i < tickets.length; i++) {
        const ticket = tickets[i];
        const token = validTokens[i];

        if (ticket.status === "error") {
          errors.push({
            token: token.substring(0, 30) + "...",
            error: ticket.message,
            details: ticket.details,
          });
          logger.error(
            `[Expo] Notification ${i + 1} FAILED for token ${token.substring(
              0,
              30
            )}...:`,
            {
              status: ticket.status,
              message: ticket.message,
              details: ticket.details,
            }
          );
        } else if (ticket.status === "ok") {
          success.push({
            token: token, // Store full token for cleanup
            id: ticket.id,
          });
          logger.info(
            `[Expo] Notification ${i + 1} ACCEPTED by Expo - Ticket ID: ${
              ticket.id
            } for token ${token.substring(0, 30)}...`
          );
        } else {
          logger.warn(
            `[Expo] Notification ${i + 1} has unknown status:`,
            ticket
          );
        }
      }

      if (errors.length > 0) {
        logger.warn(
          `[Expo] Failed to send ${errors.length} notifications:`,
          JSON.stringify(errors, null, 2)
        );
      }

      logger.info(
        `[Expo] Summary: ${success.length} accepted, ${errors.length} failed out of ${tickets.length} total`
      );

      // Check receipts after a short delay to see delivery status
      // Note: Receipts are only available after Expo processes the notification
      if (success.length > 0) {
        // Map ticket IDs to tokens for cleanup
        const ticketIdToTokenMap = new Map<string, string>();
        for (let i = 0; i < tickets.length; i++) {
          const ticket = tickets[i];
          if (ticket.status === "ok" && ticket.id) {
            ticketIdToTokenMap.set(ticket.id, validTokens[i]);
          }
        }

        setTimeout(async () => {
          try {
            const receiptIds = success
              .map((s: any) => s.id)
              .filter((id: string) => id);

            if (receiptIds.length > 0) {
              logger.info(
                `[Expo] Checking receipts for ${receiptIds.length} notification(s)...`
              );
              const receiptIdChunks =
                expo.chunkPushNotificationReceiptIds(receiptIds);

              for (const chunk of receiptIdChunks) {
                try {
                  const receipts = await expo.getPushNotificationReceiptsAsync(
                    chunk
                  );

                  // Check for delivery errors and clean up invalid tokens
                  for (const [receiptId, receipt] of Object.entries(receipts)) {
                    if (receipt.status === "error") {
                      const errorDetails = (receipt as any).details;
                      const errorMessage = (receipt as any).message;

                      logger.error(
                        `[Expo] Notification delivery FAILED for receipt ${receiptId}:`,
                        {
                          error: errorMessage,
                          details: errorDetails,
                        }
                      );

                      // Handle DeviceNotRegistered - token is invalid, remove it
                      if (
                        errorDetails?.error === "DeviceNotRegistered" ||
                        errorMessage?.includes("not registered")
                      ) {
                        const token = ticketIdToTokenMap.get(receiptId);
                        if (token) {
                          logger.warn(
                            `[Expo] Removing invalid token (DeviceNotRegistered): ${token.substring(
                              0,
                              30
                            )}...`
                          );
                          try {
                            const deletedCount =
                              TokenManager.deleteTokenByTokenString(token);
                            logger.info(
                              `[Expo] Removed ${deletedCount} invalid token(s) from database`
                            );
                          } catch (deleteError) {
                            logger.error(
                              `[Expo] Error removing invalid token:`,
                              deleteError
                            );
                          }
                        }
                      }
                    } else if (receipt.status === "ok") {
                      logger.info(
                        `[Expo] Notification delivered successfully (receipt: ${receiptId})`
                      );
                    }
                  }
                } catch (error) {
                  logger.error("[Expo] Error checking receipts:", error);
                }
              }
            }
          } catch (error) {
            logger.error("[Expo] Error in receipt check:", error);
          }
        }, 5000); // Wait 5 seconds before checking receipts
      }
    } catch (error) {
      logger.error("[Expo] Error sending push notifications:", error);
      throw error;
    }
  }

  /**
   * Send notification to tokens from a topic
   */
  static async sendNotificationToTopic(
    tokenRecords: TokenRecord[],
    title: string,
    body: string,
    data?: Record<string, any>
  ): Promise<void> {
    // Extract tokens and deduplicate by token string
    // This prevents sending duplicate notifications when the same token is registered multiple times
    const tokenSet = new Set<string>();
    const tokens: string[] = [];
    
    for (const record of tokenRecords) {
      if (!tokenSet.has(record.expoPushToken)) {
        tokenSet.add(record.expoPushToken);
        tokens.push(record.expoPushToken);
      }
    }
    
    if (tokenRecords.length > tokens.length) {
      logger.warn(
        `Deduplicated tokens: ${tokenRecords.length} records -> ${tokens.length} unique tokens`
      );
    }
    
    await this.sendNotifications(tokens, title, body, data);
  }

  /**
   * Parse Kafka message and send notification
   */
  static async sendNotificationFromKafkaMessage(
    tokenRecords: TokenRecord[],
    kafkaMessage: {
      topic: string;
      partition: number;
      offset: string;
      value: string | Buffer;
    }
  ): Promise<void> {
    try {
      // Parse message value (assuming JSON format)
      let messageData: any;
      const value =
        typeof kafkaMessage.value === "string"
          ? kafkaMessage.value
          : kafkaMessage.value.toString("utf-8");

      try {
        messageData = JSON.parse(value);
      } catch {
        // If not JSON, send simple notification
        await this.sendNotificationToTopic(
          tokenRecords,
          "New Update",
          "You have a new data update",
          {
            raw: value,
            topic: kafkaMessage.topic,
            partition: kafkaMessage.partition,
            offset: kafkaMessage.offset,
          }
        );
        return;
      }

      // Create user-friendly notification
      const title = messageData.title || "Delbi Update";

      // Extract formatted notification body from DELOPT-DELBI data
      let body = messageData.body || messageData.message;
      let timestamp = "";

      if (!body && messageData["DELOPT-DELBI"]) {
        const delbiData = messageData["DELOPT-DELBI"];

        // Try to extract from Mall data first
        if (
          delbiData.Mall &&
          Array.isArray(delbiData.Mall) &&
          delbiData.Mall.length > 0
        ) {
          const mall = delbiData.Mall[0]; // Use first mall
          if (
            mall.Count &&
            Array.isArray(mall.Count) &&
            mall.Count.length > 0
          ) {
            // Get the latest count entry (last in array)
            const latestCount = mall.Count[mall.Count.length - 1];
            const name = mall.Name || mall.Code || "Unknown";
            const inCount = latestCount.IN || 0;
            const outCount = latestCount.OUT || 0;
            const occ = inCount - outCount;
            timestamp = latestCount.DateTime || "";

            body = `Name:${name} | IN: ${inCount} | OUT: ${outCount} | OCC: ${occ} |`;
            if (timestamp) {
              body += `\nTime: ${timestamp}`;
            }
          }
        }
        // Fallback to Zone data
        else if (
          delbiData.Zone &&
          Array.isArray(delbiData.Zone) &&
          delbiData.Zone.length > 0
        ) {
          const zone = delbiData.Zone[0]; // Use first zone
          if (
            zone.Count &&
            Array.isArray(zone.Count) &&
            zone.Count.length > 0
          ) {
            // Get the latest count entry (last in array)
            const latestCount = zone.Count[zone.Count.length - 1];
            const name = zone.Name || "Unknown";
            const inCount = latestCount.IN || 0;
            const outCount = latestCount.OUT || 0;
            const occ = inCount - outCount;
            timestamp = latestCount.DateTime || "";

            body = `Name:${name} | IN: ${inCount} | OUT: ${outCount} | OCC: ${occ} |`;
            if (timestamp) {
              body += `\nTime: ${timestamp}`;
            }
          }
        }

        // Fallback if no data extracted
        if (!body) {
          if (delbiData.Mall && Array.isArray(delbiData.Mall)) {
            body = `${delbiData.Mall.length} mall(s) updated`;
          } else if (delbiData.Zone && Array.isArray(delbiData.Zone)) {
            body = `${delbiData.Zone.length} zone(s) updated`;
          } else {
            body = "New occupancy data available";
          }
        }
      }

      // Final fallback
      if (!body) {
        body = "New data update available";
      }

      // Limit body length to 256 characters (notification service limit)
      if (body.length > 256) {
        body = body.substring(0, 253) + "...";
      }

      // Send notification with minimal data (FCM has 4KB limit for entire payload)
      // Only include essential metadata, not the full JSON object
      const minimalData = {
        topic: kafkaMessage.topic,
        partition: kafkaMessage.partition,
        offset: kafkaMessage.offset,
        // Don't include the entire DELOPT-DELBI object - it's too large
        // The app can fetch the full data when needed
      };

      await this.sendNotificationToTopic(
        tokenRecords,
        title,
        body,
        minimalData
      );
    } catch (error) {
      logger.error("Error processing Kafka message for notification:", error);
      throw error;
    }
  }
}
