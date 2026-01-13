import { Expo } from "expo-server-sdk";
import { TokenRecord } from "../types";
import { logger } from "../utils/logger";

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
      for (const chunk of chunks) {
        try {
          const ticketChunk = await expo.sendPushNotificationsAsync(chunk);
          tickets.push(...ticketChunk);
        } catch (error) {
          logger.error("Error sending push notification chunk:", error);
        }
      }

      // Check for errors in tickets
      const errors: any[] = [];
      for (let i = 0; i < tickets.length; i++) {
        const ticket = tickets[i];
        if (ticket.status === "error") {
          errors.push({
            token: validTokens[i],
            error: ticket.message,
          });
        }
      }

      if (errors.length > 0) {
        logger.warn(`Failed to send ${errors.length} notifications:`, errors);
      }

      logger.info(
        `Sent ${tickets.length - errors.length} push notifications successfully`
      );
    } catch (error) {
      logger.error("Error sending push notifications:", error);
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
    const tokens = tokenRecords.map((record) => record.expoPushToken);
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
        // If not JSON, use raw value as body
        messageData = { raw: value };
      }

      // Extract title and body from message data
      // Adjust these fields based on your actual Kafka message structure
      const title = messageData.title || "New Notification";
      const body = messageData.body || messageData.message || value;

      // Send notification with message data
      await this.sendNotificationToTopic(tokenRecords, title, body, {
        ...messageData,
        topic: kafkaMessage.topic,
        partition: kafkaMessage.partition,
        offset: kafkaMessage.offset,
      });
    } catch (error) {
      logger.error("Error processing Kafka message for notification:", error);
      throw error;
    }
  }
}

