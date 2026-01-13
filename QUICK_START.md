# Quick Start Guide

## Installation

1. Navigate to the service directory:
```bash
cd kafka-notification-service
```

2. Install dependencies (if not already done):
```bash
npm install
```

3. Create `.env` file:
```bash
# Copy from .env.example or create manually
PORT=3000
NODE_ENV=development
KAFKA_BROKER=delopt.interactivedns.com:9092
KAFKA_CLIENT_ID=kafka-notification-service
DATABASE_PATH=./data/notifications.db
LOG_LEVEL=info
```

## Running the Service

### Development Mode (with auto-reload):
```bash
npm run dev
```

### Production Mode:
```bash
npm run build
npm start
```

The service will start on port 3000 (or the port specified in `.env`).

## Testing the API

### 1. Health Check
```bash
curl http://localhost:3000/api/health
```

### 2. Register a Token
```bash
curl -X POST http://localhost:3000/api/notifications/register-token \
  -H "Content-Type: application/json" \
  -d '{
    "expoPushToken": "ExponentPushToken[test-token]",
    "subscriptionData": {
      "server": "delopt.interactivedns.com:9092",
      "topic": "18_228",
      "groupId": "user-group",
      "partition": 0
    },
    "username": "testuser",
    "notificationType": "h"
  }'
```

### 3. Get User Subscriptions
```bash
curl http://localhost:3000/api/notifications/subscriptions/testuser
```

### 4. Unregister Token
```bash
curl -X POST http://localhost:3000/api/notifications/unregister-token \
  -H "Content-Type: application/json" \
  -d '{
    "expoPushToken": "ExponentPushToken[test-token]",
    "username": "testuser"
  }'
```

## How It Works

1. **Token Registration**: Mobile app registers Expo push tokens with Kafka subscription data
2. **Kafka Consumer**: Service subscribes to all topics that have registered tokens
3. **Message Processing**: When a Kafka message arrives:
   - Service looks up all tokens subscribed to that topic
   - Formats a push notification
   - Sends notification via Expo Push Notification Service
4. **Database**: SQLite stores all token mappings persistently

## Next Steps

1. Update the mobile app's `push-token-registration.ts` to point to this service
2. Test with real Kafka messages
3. Configure Kafka broker connection (SASL/SSL if needed)
4. Deploy the service to your server

## Troubleshooting

- **Kafka connection errors**: Check `KAFKA_BROKER` in `.env`
- **Database errors**: Ensure the `data/` directory is writable
- **Push notification errors**: Verify Expo tokens are valid
- **Check logs**: Service logs to console (configure log level in `.env`)

