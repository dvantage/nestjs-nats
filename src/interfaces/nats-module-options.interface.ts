import type { NatsStreamOptionsInterface } from './nats-stream-options.interface.js';

export interface NatsModuleOptionsInterface {
  servers: string[];
  token: string;
  connectionName: string;
  maxReconnectAttempts: number;
  streams: NatsStreamOptionsInterface[];
  durablePrefix: string;
  eventErrorMessage: string;
  sendMessageErrorMessage: string;
  sendMessageByBuilderErrorMessage: string;
  sendMessageTimeoutMilliseconds?: number;
  sendMessageByBuilderTimeoutMilliseconds?: number;
}
