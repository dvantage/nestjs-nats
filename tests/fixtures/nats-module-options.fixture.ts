import type { NatsModuleOptionsInterface } from '../../src/interfaces/nats-module-options.interface.js';

export const natsModuleOptionsFixture: NatsModuleOptionsInterface = {
  servers: ['nats://127.0.0.1:4222'],
  token: 'nats-token',
  connectionName: 'ecommerce-main-events',
  maxReconnectAttempts: 10,
  streams: [
    { name: 'ecommerce-events-stream', subjects: ['event.ecommerce.>'] },
    { name: 'main-events-stream', subjects: ['event.main.>'] },
  ],
  durablePrefix: 'ecommerce-main-events',
  eventErrorMessage: 'Ecommerce - event transmission error',
  sendMessageErrorMessage: 'Ecommerce to Main - message transmission error',
  sendMessageByBuilderErrorMessage: 'Ecommerce to Main - builder message transmission error',
};
