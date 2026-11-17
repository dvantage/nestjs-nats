import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  JetStreamApiCodes,
  JetStreamApiError,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect } from '@nats-io/transport-node';
import { InternalServerErrorException, ServiceUnavailableException } from '@nestjs/common';
import type { MockInstance } from 'vitest';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ensureOwnedNatsStream } from '../src/helpers/nats-stream.helper.js';
import type { JetStreamEventContextInterface } from '../src/interfaces/jetstream-event-context.interface.js';
import { NatsService } from '../src/nats.service.js';
import type { JetStreamEventHandler } from '../src/types/jetstream-event-handler.type.js';
import { natsModuleOptionsFixture as natsModuleOptions } from './fixtures/nats-module-options.fixture.js';

vi.mock(import('@nats-io/transport-node'), () => ({
  connect: vi.fn(),
}));

vi.mock(import('@nats-io/jetstream'), async (importOriginal) => ({
  ...(await importOriginal()),
  jetstream: vi.fn(),
  jetstreamManager: vi.fn(),
}));

vi.mock(import('../src/helpers/nats-stream.helper.js'), () => ({
  ensureOwnedNatsStream: vi.fn(),
}));

const userRemovedSubject = 'event.main.user.removed';
const userRemovedDurableName = 'ecommerce-main-events-event_main_user_removed';

const createNatsConnection = () => ({
  isClosed: vi.fn(() => false),
  closed: vi.fn(() => new Promise<void>(() => undefined)),
  close: vi.fn(() => Promise.resolve()),
  drain: vi.fn(() => Promise.resolve()),
});

const createJetStreamMessage = (payload: unknown) => ({
  subject: userRemovedSubject,
  json: vi.fn(() => payload),
  ack: vi.fn(),
  term: vi.fn(),
});

const createConsumerMessages = (jetStreamMessages: ReturnType<typeof createJetStreamMessage>[]) => {
  const consumerMessagesStop = Promise.withResolvers<void>();

  return {
    stop: vi.fn(() => consumerMessagesStop.resolve()),
    closed: vi.fn(() => consumerMessagesStop.promise),
    async *[Symbol.asyncIterator]() {
      for (const jetStreamMessage of jetStreamMessages) {
        yield jetStreamMessage;
      }

      await consumerMessagesStop.promise;
    },
  };
};

describe('NatsService', () => {
  const createdNatsServices: NatsService[] = [];
  let consoleLogSpy: MockInstance<typeof console.log>;

  const createContext = ({
    jetStreamMessages = [],
    existingConsumerConfig = null,
  }: {
    jetStreamMessages?: ReturnType<typeof createJetStreamMessage>[];
    existingConsumerConfig?: Record<string, unknown> | null;
  } = {}) => {
    const streamConnection = createNatsConnection();
    const natsConnection = createNatsConnection();
    const consumerMessages = createConsumerMessages(jetStreamMessages);
    const consumer = {
      consume: vi.fn(() => Promise.resolve(consumerMessages)),
    };
    const jetStreamClient = {
      publish: vi.fn(() => Promise.resolve({ seq: 1 })),
      consumers: {
        get: vi.fn(() => Promise.resolve(consumer)),
      },
    };
    const jetStreamManager = {
      streams: {
        find: vi.fn(() => Promise.resolve('main-events-stream')),
      },
      consumers: {
        info: vi.fn(),
        add: vi.fn(() => Promise.resolve({})),
      },
    };

    if (existingConsumerConfig === null) {
      jetStreamManager.consumers.info.mockRejectedValue(
        new JetStreamApiError({
          code: 404,
          err_code: JetStreamApiCodes.ConsumerNotFound,
          description: 'consumer not found',
        }),
      );
    } else {
      jetStreamManager.consumers.info.mockResolvedValue({ config: existingConsumerConfig });
    }

    vi.mocked(connect)
      .mockResolvedValueOnce(streamConnection as never)
      .mockResolvedValue(natsConnection as never);
    vi.mocked(jetstreamManager).mockResolvedValue(jetStreamManager as never);
    vi.mocked(jetstream).mockReturnValue(jetStreamClient as never);

    const natsService = new NatsService(natsModuleOptions);
    createdNatsServices.push(natsService);

    return { natsService, streamConnection, natsConnection, consumerMessages, jetStreamClient, jetStreamManager };
  };

  const registerUserRemovedHandler = async (natsService: NatsService, handler: JetStreamEventHandler) => {
    await natsService.registerJetStreamSubscription({
      subject: userRemovedSubject,
      durableName: userRemovedDurableName,
      handler,
    });
  };

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => undefined);
  });

  afterEach(async () => {
    for (const createdNatsService of createdNatsServices) {
      await createdNatsService.onModuleDestroy();
    }

    createdNatsServices.length = 0;
    vi.restoreAllMocks();
    vi.resetAllMocks();
  });

  it('ensures every configured stream on a separate connection before it connects for events', async () => {
    const context = createContext();

    await context.natsService.onModuleInit();

    expect(connect).toHaveBeenNthCalledWith(1, {
      servers: natsModuleOptions.servers,
      token: natsModuleOptions.token,
    });
    expect(ensureOwnedNatsStream).toHaveBeenCalledWith(context.jetStreamManager, 'ecommerce-events-stream', [
      'event.ecommerce.>',
    ]);
    expect(ensureOwnedNatsStream).toHaveBeenCalledWith(context.jetStreamManager, 'main-events-stream', [
      'event.main.>',
    ]);
    expect(context.streamConnection.drain).toHaveBeenCalled();
    expect(connect).toHaveBeenNthCalledWith(2, {
      servers: natsModuleOptions.servers,
      token: natsModuleOptions.token,
      name: natsModuleOptions.connectionName,
      maxReconnectAttempts: natsModuleOptions.maxReconnectAttempts,
    });
  });

  it('closes the stream connection when a stream cannot be ensured', async () => {
    const context = createContext();
    vi.mocked(ensureOwnedNatsStream).mockRejectedValue(new Error('authorization violation'));

    await expect(context.natsService.onModuleInit()).rejects.toThrow('authorization violation');
    expect(context.streamConnection.drain).toHaveBeenCalled();
    expect(connect).toHaveBeenCalledTimes(1);
  });

  it('creates the pull consumer and reads events only after startJetStreamConsumers', async () => {
    const jetStreamMessage = createJetStreamMessage({ userId: 5 });
    const context = createContext({ jetStreamMessages: [jetStreamMessage] });
    const handledEvents: { payload: unknown; subject: string }[] = [];
    const handler = vi.fn((payload: unknown, jetStreamEventContext: JetStreamEventContextInterface) => {
      handledEvents.push({ payload, subject: jetStreamEventContext.subject });
    });

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, handler);

    expect(context.jetStreamManager.consumers.add).not.toHaveBeenCalled();
    expect(handler).not.toHaveBeenCalled();

    await context.natsService.startJetStreamConsumers();

    expect(context.jetStreamManager.consumers.add).toHaveBeenCalledWith('main-events-stream', {
      durable_name: userRemovedDurableName,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      filter_subject: userRemovedSubject,
    });
    await vi.waitFor(() => expect(jetStreamMessage.ack).toHaveBeenCalled());
    expect(handledEvents).toEqual([{ payload: { userId: 5 }, subject: userRemovedSubject }]);
  });

  it('acknowledges the message after the handler finishes', async () => {
    const handlingSteps: string[] = [];
    const jetStreamMessage = createJetStreamMessage({ userId: 5 });
    jetStreamMessage.ack.mockImplementation(() => handlingSteps.push('ack'));
    const context = createContext({ jetStreamMessages: [jetStreamMessage] });

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, async () => {
      await Promise.resolve();
      handlingSteps.push('handler');
    });
    await context.natsService.startJetStreamConsumers();

    await vi.waitFor(() => expect(handlingSteps).toEqual(['handler', 'ack']));
  });

  it('terminates the message when the handler throws', async () => {
    const jetStreamMessage = createJetStreamMessage({ userId: 5 });
    const context = createContext({ jetStreamMessages: [jetStreamMessage] });

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, () => {
      throw new InternalServerErrorException('Payee unlink failed');
    });
    await context.natsService.startJetStreamConsumers();

    await vi.waitFor(() => expect(jetStreamMessage.term).toHaveBeenCalled());
    expect(jetStreamMessage.ack).not.toHaveBeenCalled();
  });

  it('reads from an existing pull consumer without creating it again', async () => {
    const context = createContext({
      existingConsumerConfig: {
        filter_subject: userRemovedSubject,
        deliver_policy: DeliverPolicy.All,
        ack_policy: AckPolicy.Explicit,
      },
    });

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, () => undefined);
    await context.natsService.startJetStreamConsumers();

    expect(context.jetStreamManager.consumers.add).not.toHaveBeenCalled();
    expect(context.jetStreamClient.consumers.get).toHaveBeenCalledWith('main-events-stream', userRemovedDurableName);
  });

  it('does not read from a push consumer with the same durable name', async () => {
    const context = createContext({
      existingConsumerConfig: {
        deliver_subject: 'main-ecommerce-messages.5SY6T2IAV9X3VIESRLIRXR',
        filter_subject: userRemovedSubject,
        deliver_policy: DeliverPolicy.All,
        ack_policy: AckPolicy.Explicit,
      },
    });

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, () => undefined);
    await context.natsService.startJetStreamConsumers();

    expect(context.jetStreamManager.consumers.add).not.toHaveBeenCalled();
    expect(context.jetStreamClient.consumers.get).not.toHaveBeenCalled();
    expect(consoleLogSpy).toHaveBeenCalledWith(
      'JetStream subscription failed, retry scheduled',
      userRemovedSubject,
      expect.any(InternalServerErrorException),
    );
  });

  it('rejects a second handler for the same durable name', async () => {
    const context = createContext();

    await registerUserRemovedHandler(context.natsService, () => undefined);

    await expect(registerUserRemovedHandler(context.natsService, () => undefined)).rejects.toBeInstanceOf(
      InternalServerErrorException,
    );
  });

  it('refuses to publish without a NATS connection', async () => {
    const context = createContext();

    await expect(
      context.natsService.publishJetStreamMessage('event.ecommerce.order.created', { id: 1 }),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('publishes the payload as JSON through JetStream', async () => {
    const context = createContext();

    await context.natsService.onModuleInit();
    await context.natsService.publishJetStreamMessage('event.ecommerce.order.created', { id: 1 });

    expect(context.jetStreamClient.publish).toHaveBeenCalledWith('event.ecommerce.order.created', '{"id":1}');
  });

  it('stops the consumers and closes the connection on shutdown', async () => {
    const context = createContext();

    await context.natsService.onModuleInit();
    await registerUserRemovedHandler(context.natsService, () => undefined);
    await context.natsService.startJetStreamConsumers();
    await context.natsService.onModuleDestroy();

    expect(context.consumerMessages.stop).toHaveBeenCalled();
    expect(context.natsConnection.close).toHaveBeenCalled();
  });
});
