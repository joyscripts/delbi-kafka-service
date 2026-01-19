import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import { config } from "../config/env";
import { logger } from "../utils/logger";
import { TokenManager } from "./token-manager";
import { PushNotificationService } from "./push-notification";

export class KafkaConsumerService {
  private kafka: Kafka;
  private consumer: Consumer | null = null;
  private isRunning = false;
  private currentlySubscribedTopics: Set<string> = new Set();
  private restartTimeout: NodeJS.Timeout | null = null;
  private isRestarting = false;

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
      // Create consumer with optimized settings to prevent rebalancing issues
      // Key settings:
      // - sessionTimeout: How long broker waits for heartbeat before considering consumer dead
      // - heartbeatInterval: How often to send heartbeats (should be < sessionTimeout/3)
      // - rebalanceTimeout: How long to wait during rebalancing
      // Note: KafkaJS doesn't support maxPollInterval, but heartbeats continue automatically
      // during message processing, so long operations won't cause rebalancing as long as
      // they complete within sessionTimeout (30s). Expo API calls are typically < 5s.
      this.consumer = this.kafka.consumer({
        groupId: "user-group",
        sessionTimeout: 30000, // 30 seconds - reduced for faster failure detection
        heartbeatInterval: 3000, // 3 seconds - send heartbeat every 3s (should be < sessionTimeout/3)
        rebalanceTimeout: 60000, // 60 seconds - time to wait during rebalancing
        allowAutoTopicCreation: false,
        maxInFlightRequests: 1, // Process one message at a time to maintain order
        retry: {
          initialRetryTime: 100,
          retries: 8,
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

      // Note: KafkaJS doesn't expose rebalancing events directly
      // Rebalancing is handled automatically by the library
      // The heartbeatInterval ensures heartbeats continue during processing

      await this.consumer.connect();
      logger.info("Connected to Kafka broker");

      // Subscribe to all active topics
      await this.subscribeToTopics();
      
      // Log subscription status
      logger.info(`[Kafka] Consumer subscribed and ready to receive messages`);

      // Start consuming messages with optimized commit settings
      // Note: We process messages asynchronously to avoid blocking heartbeats
      // The await in handleMessage ensures we don't process the next message until current one completes
      // This maintains message order while allowing heartbeats to continue
      await this.consumer.run({
        autoCommit: true,
        autoCommitInterval: 10000, // 10 seconds - commit every 10s or after each message
        autoCommitThreshold: 1, // Commit after each message
        eachMessage: async (payload: EachMessagePayload) => {
          // Process message - this is async but we await it to maintain order
          // Heartbeats continue in the background via heartbeatInterval
          try {
            logger.info(`[Kafka] Received message from topic: ${payload.topic}, partition: ${payload.partition}, offset: ${payload.message.offset}, size: ${payload.message.value?.length || 0} bytes`);
            await this.handleMessage(payload);
          } catch (error) {
            // Log error but don't throw - we want to continue processing
            // The error is already logged in handleMessage
            logger.error("Error in eachMessage handler:", error);
          }
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
   * Only subscribes to topics that have registered tokens (no hardcoded defaults)
   */
  private async subscribeToTopics(): Promise<void> {
    if (!this.consumer) return;

    // Get topics from database (may be empty if no tokens registered yet)
    const dbTopics = TokenManager.getAllTopics();

    if (dbTopics.length === 0) {
      logger.warn(`[Kafka] No topics found in database - consumer will not subscribe to any topics`);
      this.currentlySubscribedTopics.clear();
      return;
    }

    // Remove duplicates and sort for consistent logging
    const uniqueTopics = [...new Set(dbTopics)].sort();

    logger.info(`[Kafka] Subscribing to ${uniqueTopics.length} topic(s) from database: ${uniqueTopics.join(", ")}`);

    await this.consumer.subscribe({
      topics: uniqueTopics,
      fromBeginning: false, // Start from latest messages
    });

    // Update our tracking of subscribed topics
    this.currentlySubscribedTopics = new Set(uniqueTopics);
    
    logger.info(`[Kafka] Successfully subscribed to all topics`);
  }

  /**
   * Handle incoming Kafka message
   * 
   * Note: This method is awaited, which means we process messages sequentially.
   * However, heartbeats continue automatically in the background via heartbeatInterval.
   * Processing should complete within sessionTimeout (30s) to avoid rebalancing.
   * Expo API calls are typically fast (< 5s), so this should not be an issue.
   */
  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const startTime = Date.now();
    try {
      const { topic, partition, message } = payload;
      const offset = message.offset;
      const value = message.value?.toString() || "";

      // Log every message received from Kafka with topic information
      logger.info(
        `[Kafka Message] Topic: ${topic} | Partition: ${partition} | Offset: ${offset} | Size: ${value.length} bytes`
      );

      // Get all tokens subscribed to this topic
      const tokenRecords = TokenManager.getTokensByTopic(topic);

      // Log at WARN level so we can see if this is the issue
      if (tokenRecords.length === 0) {
        logger.warn(`[Kafka] No tokens found for topic: ${topic} - message will be ignored`);
        return;
      }

      logger.info(
        `[Kafka] Processing message for topic ${topic}, sending to ${tokenRecords.length} token(s)`
      );

      // Send push notification
      // This is async and may take time (Expo API calls), but heartbeats continue in background
      await PushNotificationService.sendNotificationFromKafkaMessage(
        tokenRecords,
        {
          topic,
          partition,
          offset,
          value,
        }
      );

      const duration = Date.now() - startTime;
      if (duration > 10000) {
        logger.warn(
          `Message processing took ${duration}ms (${(duration / 1000).toFixed(1)}s) - consider optimizing if this happens frequently`
        );
      } else {
        logger.debug(`Message processed in ${duration}ms`);
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(
        `Error handling Kafka message (took ${duration}ms):`,
        error
      );
      // Don't throw - we want to continue processing other messages
      // The error is logged, and we'll move on to the next message
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
      this.currentlySubscribedTopics.clear();
      logger.info("Kafka consumer stopped");
    } catch (error) {
      logger.error("Error stopping Kafka consumer:", error);
      throw error;
    }
  }

  /**
   * Restart the consumer to pick up new topics
   * This is called when a new topic is registered
   */
  async restart(): Promise<void> {
    if (this.isRestarting) {
      logger.debug("[Kafka] Restart already in progress, skipping");
      return;
    }

    this.isRestarting = true;
    logger.info("[Kafka] Restarting consumer to pick up new topics...");

    try {
      // Stop current consumer
      await this.stop();
      
      // Small delay to ensure clean shutdown
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Start with new topics
      await this.start();
      
      logger.info("[Kafka] Consumer restarted successfully");
    } catch (error) {
      logger.error("[Kafka] Error restarting consumer:", error);
      this.isRestarting = false;
      throw error;
    } finally {
      this.isRestarting = false;
    }
  }

  /**
   * Check if a topic is new (not currently subscribed)
   * If new, schedule a debounced restart
   */
  checkAndRestartIfNeeded(newTopic: string): void {
    // Check if this topic is already subscribed
    if (this.currentlySubscribedTopics.has(newTopic)) {
      logger.debug(`[Kafka] Topic "${newTopic}" is already subscribed, no restart needed`);
      return;
    }

    logger.info(`[Kafka] New topic detected: "${newTopic}". Scheduling consumer restart...`);

    // Clear any existing restart timeout
    if (this.restartTimeout) {
      clearTimeout(this.restartTimeout);
    }

    // Debounce: Wait 2 seconds before restarting (in case multiple topics are registered quickly)
    this.restartTimeout = setTimeout(async () => {
      try {
        // Double-check that we still need to restart (topic might have been added during wait)
        const dbTopics = TokenManager.getAllTopics();
        const hasNewTopics = dbTopics.some(topic => !this.currentlySubscribedTopics.has(topic));
        
        if (hasNewTopics) {
          logger.info(`[Kafka] Restarting consumer to subscribe to new topic(s)...`);
          await this.restart();
        } else {
          logger.debug(`[Kafka] No new topics found, skipping restart`);
        }
      } catch (error) {
        logger.error("[Kafka] Error during scheduled restart:", error);
      } finally {
        this.restartTimeout = null;
      }
    }, 2000); // 2 second debounce
  }

  /**
   * Get currently subscribed topics (for debugging)
   */
  getSubscribedTopics(): string[] {
    return Array.from(this.currentlySubscribedTopics).sort();
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
