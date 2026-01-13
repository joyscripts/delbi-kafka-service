# Kafka Notification Service

A Node.js Express service that bridges Kafka message consumption and Expo push notifications for the Delbi mobile app.

## Features

- ✅ REST API for token registration/unregistration
- ✅ SQLite database for token storage
- ✅ Kafka consumer using kafkajs
- ✅ Expo push notification integration
- ✅ Support for multiple devices per user
- ✅ Support for multiple notification types (hourly/daily/monthly)

## Prerequisites

- Node.js 18+
- npm or yarn
- Access to Kafka broker
- Expo push notification credentials (optional for development)

## Installation

1. Install dependencies:
```bash
npm install
```

2. Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```

3. Update `.env` with your configuration:
```env
PORT=3000
NODE_ENV=development
KAFKA_BROKER=delopt.interactivedns.com:9092
KAFKA_CLIENT_ID=kafka-notification-service
DATABASE_PATH=./data/notifications.db
LOG_LEVEL=info
```

## Development

Run in development mode with auto-reload:
```bash
npm run dev
```

Build TypeScript:
```bash
npm run build
```

Run production build:
```bash
npm run build
npm start
```

## API Endpoints

### Register Token
**POST** `/api/notifications/register-token`

Register an Expo push token with Kafka subscription data.

Request body:
```json
{
  "expoPushToken": "ExponentPushToken[xxxxx]",
  "subscriptionData": {
    "server": "delopt.interactivedns.com:9092",
    "topic": "18_228",
    "groupId": "user-group",
    "partition": 0
  },
  "username": "Koskii",
  "notificationType": "h"
}
```

### Unregister Token
**POST** `/api/notifications/unregister-token`

Unregister a specific push token.

Request body:
```json
{
  "expoPushToken": "ExponentPushToken[xxxxx]",
  "username": "Koskii"
}
```

### Unregister All Tokens
**POST** `/api/notifications/unregister-all`

Unregister all tokens for a user.

Request body:
```json
{
  "username": "Koskii"
}
```

### Get User Subscriptions
**GET** `/api/notifications/subscriptions/:username`

Get all subscriptions for a user (for debugging).

### Health Check
**GET** `/api/health`

Health check endpoint.

## Database

The service uses SQLite to store token mappings. The database file is created automatically at the path specified in `DATABASE_PATH` environment variable.

Schema:
- `tokens` table stores Expo push tokens and their Kafka subscriptions
- Indexes on `topic`, `username`, and `expoPushToken` for fast lookups

## Kafka Consumer

The service automatically:
1. Connects to the Kafka broker
2. Subscribes to all topics that have registered tokens
3. Consumes messages and sends push notifications
4. Refreshes topic subscriptions every 5 minutes

## Project Structure

```
kafka-notification-service/
├── src/
│   ├── index.ts                 # Express app entry point
│   ├── config/
│   │   └── env.ts              # Environment configuration
│   ├── routes/
│   │   ├── notifications.ts    # Notification API routes
│   │   └── health.ts           # Health check route
│   ├── services/
│   │   ├── database.ts         # SQLite database setup
│   │   ├── token-manager.ts    # Token storage operations
│   │   ├── kafka-consumer.ts   # Kafka consumer service
│   │   └── push-notification.ts # Expo push notification service
│   ├── types/
│   │   └── index.ts            # TypeScript types
│   └── utils/
│       └── logger.ts           # Logging utility
├── data/                       # Database files (gitignored)
├── .env                        # Environment variables (gitignored)
├── package.json
├── tsconfig.json
└── README.md
```

## Environment Variables

- `PORT` - Server port (default: 3000)
- `NODE_ENV` - Environment (development/production)
- `KAFKA_BROKER` - Kafka broker address (comma-separated for multiple)
- `KAFKA_CLIENT_ID` - Kafka client ID
- `DATABASE_PATH` - SQLite database file path
- `LOG_LEVEL` - Logging level (error/warn/info/debug)

## License

ISC

