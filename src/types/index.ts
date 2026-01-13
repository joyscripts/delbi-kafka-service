export interface SubscriptionData {
  server: string;
  partition: number;
  groupId: string;
  topic: string;
}

export interface TokenRegistrationRequest {
  expoPushToken: string;
  subscriptionData: SubscriptionData;
  username: string;
  notificationType?: "h" | "d" | "m"; // hourly, daily, monthly
}

export interface TokenUnregisterRequest {
  expoPushToken: string;
  username: string;
}

export interface TokenRecord {
  id: number;
  expoPushToken: string;
  username: string;
  topic: string;
  server: string;
  groupId: string;
  partition: number;
  notificationType: string | null;
  registeredAt: string;
}

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: string;
  value: string | Buffer;
  timestamp: string;
}

