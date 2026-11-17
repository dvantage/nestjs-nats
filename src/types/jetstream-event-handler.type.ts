import type { JetStreamEventContextInterface } from '../interfaces/jetstream-event-context.interface.js';

export type JetStreamEventHandler = (
  payload: unknown,
  jetStreamEventContext: JetStreamEventContextInterface,
) => void | Promise<void>;
