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
      connectionTimeout: 10000, // 10 seconds
      requestTimeout: 30000, // 30 seconds
      retry: {
        initialRetryTime: 100,
        retries: 8,
        maxRetryTime: 30000,
        multiplier: 2,
        restartOnFailure: async (e) => {
          logger.error("Kafka connection failed, will retry:", e);
          return true; // Always retry on failure
        },
      },
      // Add connection options for better error handling
      logLevel: config.logLevel === "debug" ? 4 : 1, // 1 = ERROR, 4 = DEBUG
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
        sessionTimeout: 60000,
        heartbeatInterval: 6000,
        allowAutoTopicCreation: false,
        maxInFlightRequests: 1,
        retry: {
          initialRetryTime: 100,
          : 8,
          maxRetryTime: 30000,
        },
      });

      // Add connection error handlers
      this.consumer.on("consumer.connect", () => {
        logger.info("Kafka consumer connected");
      });

      this.consumer.on("consumer.disconnect", () => {
        logger.warn("Kafka consumer disconnected");
        this.isRunning = false;
      });

      this.consumer.on("consumer.crash", (event: any) => {
        logger.error("Kafka consumer crashed:", event.payload?.error || event);
        this.isRunning = false;
        // Auto-restart after delay
        setTimeout(() => {
          logger.info("Attempting to restart Kafka consumer...");
          this.start().catch((error) => {
            logger.error("Failed to restart Kafka consumer:", error);
          });
        }, 10000); // Wait 10 seconds before retry
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
   * Uses DEFAULT_TOPICS as base and merges with any additional topics from database
   */
  private async subscribeToTopics(): Promise<void> {
    if (!this.consumer) return;
    
    // Known topics that will always be subscribed to
    const DEFAULT_TOPICS = ["53_1290", "18_228"];

    // Get topics from database (may be empty if no tokens registered yet)
    const dbTopics = TokenManager.getAllTopics();

    // Merge default topics with database topics and remove duplicates
    const uniqueTopics = [...new Set([...DEFAULT_TOPICS, ...dbTopics])];

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

  /**
   * Test Kafka broker connectivity
   */
  async testConnection(): Promise<{ success: boolean; error?: string }> {
    try {
      const admin = this.kafka.admin();
      await admin.connect();
      const metadata = await admin.describeCluster();
      await admin.disconnect();
      
      logger.info("Kafka connection test successful", {
        clusterId: metadata.clusterId,
        brokers: metadata.brokers.length,
        controller: metadata.controller,
      });
      
      return { success: true };
    } catch (error: any) {
      const errorMessage = error.message || String(error);
      logger.error("Kafka connection test failed:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }
}
