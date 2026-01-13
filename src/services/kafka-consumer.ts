import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import { config } from "../config/env";
import { logger } from "../utils/logger";
import { TokenManager } from "./token-manager";
import { PushNotificationService } from "./push-notification";

export class KafkaConsumerService {
  private kafka: Kafka;
  private consumer: Consumer | null = null;
  private isRunning = false;

  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.broker.split(","),
    });
  }

  /**
   * Initialize and start the Kafka consumer
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("Kafka consumer is already running");
      return;
    }

    try {
      // Create consumer with explicit commit settings to prevent duplicates
      this.consumer = this.kafka.consumer({
        groupId: "user-group",
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        allowAutoTopicCreation: false,
      });

      await this.consumer.connect();
      logger.info("Connected to Kafka broker");

      // Subscribe to all active topics
      await this.subscribeToTopics();

      // Start consuming messages with explicit auto-commit settings
      await this.consumer.run({
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 1,
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
        },
      });

      this.isRunning = true;
      logger.info("Kafka consumer started");

      // Note: KafkaJS doesn't support dynamic subscription changes while running
      // If new topics are added, the service needs to be restarted to subscribe to them
    } catch (error) {
      logger.error("Error starting Kafka consumer:", error);
      throw error;
    }
  }

  /**
   * Subscribe to all active topics from the database
   */
  private async subscribeToTopics(): Promise<void> {
    if (!this.consumer) return;

    const topics = TokenManager.getAllTopics();

    if (topics.length === 0) {
      logger.info("No topics to subscribe to");
      return;
    }

    // Get unique topics
    const uniqueTopics = [...new Set(topics)];

    logger.info(`Subscribing to topics: ${uniqueTopics.join(", ")}`);

    await this.consumer.subscribe({
      topics: uniqueTopics,
      fromBeginning: false, // Start from latest messages
    });
  }

  /**
   * Handle incoming Kafka message
   */
  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    try {
      const { topic, partition, message } = payload;
      const offset = message.offset;
      const value = message.value?.toString() || "";

      logger.debug(
        `Received message from topic: ${topic}, partition: ${partition}, offset: ${offset}`
      );

      // Get all tokens subscribed to this topic
      const tokenRecords = TokenManager.getTokensByTopic(topic);

      if (tokenRecords.length === 0) {
        logger.debug(`No tokens found for topic: ${topic}`);
        return;
      }

      logger.info(
        `Processing message for topic ${topic}, sending to ${tokenRecords.length} token(s)`
      );

      // Send push notification
      await PushNotificationService.sendNotificationFromKafkaMessage(
        tokenRecords,
        {
          topic,
          partition,
          offset,
          value,
        }
      );
    } catch (error) {
      logger.error("Error handling Kafka message:", error);
      // Don't throw - we want to continue processing other messages
    }
  }

  /**
   * Stop the Kafka consumer
   */
  async stop(): Promise<void> {
    if (!this.isRunning || !this.consumer) {
      return;
    }

    try {
      await this.consumer.disconnect();
      this.isRunning = false;
      this.consumer = null;
      logger.info("Kafka consumer stopped");
    } catch (error) {
      logger.error("Error stopping Kafka consumer:", error);
      throw error;
    }
  }

  /**
   * Get consumer status
   */
  getStatus(): { isRunning: boolean } {
    return { isRunning: this.isRunning };
  }
}
