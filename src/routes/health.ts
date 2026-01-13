import { Router, Request, Response } from "express";

const router = Router();

/**
 * GET /api/health
 * Health check endpoint
 */
router.get("/", (req: Request, res: Response) => {
  // Basic health check
  // In production, you might want to check:
  // - Database connection
  // - Kafka connection
  // - Other dependencies
  res.json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "kafka-notification-service",
  });
});

export default router;

