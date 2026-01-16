import { Router, Request, Response } from "express";
import { KafkaConsumerService } from "../services/kafka-consumer";

const router = Router();

// Get Kafka consumer instance (you'll need to pass this or make it accessible)
let kafkaConsumerInstance: KafkaConsumerService | null = null;

export const setKafkaConsumer = (consumer: KafkaConsumerService) => {
  kafkaConsumerInstance = consumer;
};

/**
 * GET /api/health
 * Health check endpoint
 */
router.get("/", async (req: Request, res: Response) => {
  const health: any = {
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "kafka-notification-service",
  };

  // Check Kafka connection if consumer is available
  if (kafkaConsumerInstance) {
    const kafkaStatus = kafkaConsumerInstance.getStatus();
    health.kafka = {
      isRunning: kafkaStatus.isRunning,
    };

    // Test connection if requested
    if (req.query.test === "true") {
      const connectionTest = await kafkaConsumerInstance.testConnection();
      health.kafka.connectionTest = connectionTest;
    }
  }

  res.json(health);
});

export default router;
