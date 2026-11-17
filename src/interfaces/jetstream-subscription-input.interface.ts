import type { JetStreamEventHandler } from '../types/jetstream-event-handler.type.js';

export interface JetStreamSubscriptionInputInterface {
  subject: string;
  durableName: string;
  handler: JetStreamEventHandler;
}
