import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

import type { JetStreamManager } from '@nats-io/jetstream';
import { AckPolicy, DeliverPolicy, JetStreamApiCodes, JetStreamApiError, jetstreamManager } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { connect } from '@nats-io/transport-node';
import type { INestMicroservice } from '@nestjs/common';
import { Controller, HttpException, HttpStatus, UseFilters } from '@nestjs/common';
import { Ctx, MessagePattern, NatsContext, Payload, Transport } from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import type { JetStreamEventContextInterface, NatsModuleOptionsInterface } from '../src/index.js';
import {
  HttpToRpcExceptionFilter,
  JetStreamEvent,
  NatsModule,
  NatsService,
  NatsTransportService,
  readNatsRequestHeaders,
} from '../src/index.js';

const natsUrl = process.env.NATS_URL ?? 'nats://127.0.0.1:4222';
const natsToken = process.env.NATS_TOKEN ?? '';
const e2eSubjectPrefix = `nestjs-nats-e2e-${randomUUID().slice(0, 8)}`;
const eventsStreamName = `${e2eSubjectPrefix}-events`;
const createdSubject = `${e2eSubjectPrefix}.event.created`;
const failedSubject = `${e2eSubjectPrefix}.event.failed`;
const pushedSubject = `${e2eSubjectPrefix}.event.pushed`;
const durablePrefix = `${e2eSubjectPrefix}-consumer`;
const echoHeadersPattern = `${e2eSubjectPrefix}.rpc.echoHeaders`;
const echoPayloadPattern = `${e2eSubjectPrefix}.rpc.echoPayload`;
const notFoundPattern = `${e2eSubjectPrefix}.rpc.notFound`;
const slowReplyPattern = `${e2eSubjectPrefix}.rpc.slowReply`;

const natsModuleOptions: NatsModuleOptionsInterface = {
  servers: [natsUrl],
  token: natsToken,
  connectionName: `${e2eSubjectPrefix}-connection`,
  maxReconnectAttempts: 3,
  streams: [{ name: eventsStreamName, subjects: [createdSubject, failedSubject, pushedSubject] }],
  durablePrefix,
  eventErrorMessage: 'E2E - event transmission error',
  sendMessageErrorMessage: 'E2E - message transmission error',
  sendMessageByBuilderErrorMessage: 'E2E - builder message transmission error',
  sendMessageTimeoutMilliseconds: 200,
};

const handledEvents: { payload: unknown; subject: string }[] = [];
const pushedEvents: unknown[] = [];
let failedEventCount = 0;

const toDurableName = (subject: string): string => `${durablePrefix}-${subject.replaceAll('.', '_')}`;

@Controller()
class E2eEventsController {
  @JetStreamEvent(createdSubject)
  onCreatedEvent(payload: unknown, jetStreamEventContext: JetStreamEventContextInterface): void {
    handledEvents.push({ payload, subject: jetStreamEventContext.subject });
  }

  @JetStreamEvent(failedSubject)
  onFailedEvent(): void {
    failedEventCount += 1;
    throw new HttpException('Event handling failed', HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @JetStreamEvent(pushedSubject)
  onPushedEvent(payload: unknown): void {
    pushedEvents.push(payload);
  }
}

@Controller()
@UseFilters(new HttpToRpcExceptionFilter())
class E2eRpcController {
  @MessagePattern(echoHeadersPattern, Transport.NATS)
  echoHeaders(@Ctx() natsContext: NatsContext) {
    return readNatsRequestHeaders(natsContext);
  }

  @MessagePattern(echoPayloadPattern, Transport.NATS)
  echoPayload(@Payload() payload: unknown) {
    return { payload };
  }

  @MessagePattern(notFoundPattern, Transport.NATS)
  throwProjectNotFound(): never {
    throw new HttpException('Project not found', HttpStatus.NOT_FOUND);
  }

  @MessagePattern(slowReplyPattern, Transport.NATS)
  async replySlowly(): Promise<boolean> {
    await sleep(500);
    return true;
  }
}

const createNatsMicroservice = async (): Promise<INestMicroservice> => {
  const testingModule = await Test.createTestingModule({
    imports: [NatsModule.forRoot(natsModuleOptions)],
    controllers: [E2eEventsController, E2eRpcController],
  }).compile();

  const natsMicroservice = testingModule.createNestMicroservice({
    transport: Transport.NATS,
    options: { servers: [natsUrl], token: natsToken },
    logger: ['error', 'warn'],
  });
  await natsMicroservice.listen();
  return natsMicroservice;
};

describe('NatsModule on a NATS server', () => {
  let adminConnection: NatsConnection;
  let adminJetStreamManager: JetStreamManager;
  let natsMicroservice: INestMicroservice;
  let natsTransportService: NatsTransportService;
  let consumerLookupErrorBeforeStart: unknown;

  beforeAll(async () => {
    adminConnection = await connect({ servers: [natsUrl], token: natsToken });
    adminJetStreamManager = await jetstreamManager(adminConnection);
    await adminJetStreamManager.streams.add({ name: eventsStreamName, subjects: [createdSubject] });

    natsMicroservice = await createNatsMicroservice();
    natsTransportService = natsMicroservice.get(NatsTransportService);

    await adminJetStreamManager.consumers.add(eventsStreamName, {
      durable_name: toDurableName(pushedSubject),
      deliver_subject: `${e2eSubjectPrefix}.pushed-delivery`,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      filter_subject: pushedSubject,
    });

    await natsTransportService.emitEvent(createdSubject, { orderId: 1 });
    await natsTransportService.emitEvent(failedSubject, { orderId: 2 });
    await natsTransportService.emitEvent(pushedSubject, { orderId: 3 });

    consumerLookupErrorBeforeStart = await adminJetStreamManager.consumers
      .info(eventsStreamName, toDurableName(createdSubject))
      .then(
        () => null,
        (e: unknown) => e,
      );

    await natsMicroservice.get(NatsService).startJetStreamConsumers();
  });

  afterAll(async () => {
    await natsMicroservice?.close();
    await adminJetStreamManager?.streams.delete(eventsStreamName);
    await adminConnection?.close();
  });

  it('adds the configured subjects to the existing stream', async () => {
    const streamInfo = await adminJetStreamManager.streams.info(eventsStreamName);

    expect(streamInfo.config.subjects).toEqual([createdSubject, failedSubject, pushedSubject]);
  });

  it('creates no consumer before startJetStreamConsumers', () => {
    expect(consumerLookupErrorBeforeStart).toBeInstanceOf(JetStreamApiError);
    expect((consumerLookupErrorBeforeStart as JetStreamApiError).code).toBe(JetStreamApiCodes.ConsumerNotFound);
  });

  it('passes the payload and the subject to the handler and acknowledges the message', async () => {
    await vi.waitFor(() => expect(handledEvents).toEqual([{ payload: { orderId: 1 }, subject: createdSubject }]), {
      timeout: 5_000,
    });
    await vi.waitFor(
      async () => {
        const consumerInfo = await adminJetStreamManager.consumers.info(
          eventsStreamName,
          toDurableName(createdSubject),
        );
        expect(consumerInfo.num_pending).toBe(0);
        expect(consumerInfo.num_ack_pending).toBe(0);
      },
      { timeout: 5_000 },
    );
  });

  it('terminates the message when the handler throws', async () => {
    await vi.waitFor(
      async () => {
        const consumerInfo = await adminJetStreamManager.consumers.info(eventsStreamName, toDurableName(failedSubject));
        expect(consumerInfo.delivered.consumer_seq).toBe(1);
        expect(consumerInfo.num_ack_pending).toBe(0);
        expect(consumerInfo.num_redelivered).toBe(0);
      },
      { timeout: 5_000 },
    );
    expect(failedEventCount).toBe(1);
  });

  it('does not read from a push consumer with the same durable name', async () => {
    await sleep(300);

    const consumerInfo = await adminJetStreamManager.consumers.info(eventsStreamName, toDurableName(pushedSubject));

    expect(pushedEvents).toEqual([]);
    expect(consumerInfo.config.deliver_subject).toBe(`${e2eSubjectPrefix}.pushed-delivery`);
  });

  it('sends the session user, the nginx flag and the request path as headers', async () => {
    const natsRequestHeaders = await natsTransportService.sendMessageByBuilder(
      {
        headers: { 'x-nginx-proxy': 'true' },
        path: '',
        originalUrl: '/ecommerce/orders?page=2',
        url: '/orders?page=2',
        session: { passport: { user: { id: 7, roles: ['client'] } } },
      },
      echoHeadersPattern,
      { projectId: 1 },
    );

    expect(natsRequestHeaders).toEqual({
      sessionUserData: { id: 7, roles: ['client'] },
      isReqFromNginx: true,
      reqPath: '/ecommerce/orders?page=2',
    });
  });

  it('sends an empty string when the message is missing', async () => {
    await expect(natsTransportService.sendMessage(echoPayloadPattern)).resolves.toEqual({ payload: '' });
  });

  it('forwards the HTTP status of the RPC handler error when the caller asks for it', async () => {
    await expect(natsTransportService.sendMessage(notFoundPattern, { projectId: 1 }, true)).rejects.toMatchObject({
      status: HttpStatus.NOT_FOUND,
      message: 'Project not found',
    });
  });

  it('hides the RPC handler error behind the configured text', async () => {
    await expect(natsTransportService.sendMessage(notFoundPattern, { projectId: 1 })).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageErrorMessage,
    });
  });

  it('limits sendMessage by its timeout', async () => {
    await expect(natsTransportService.sendMessage(slowReplyPattern, {})).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageErrorMessage,
    });
  });

  it('does not limit sendMessageByBuilder by the sendMessage timeout', async () => {
    await expect(natsTransportService.sendMessageByBuilder({ headers: {} }, slowReplyPattern, {})).resolves.toBe(true);
  });

  it('reads through the same pull consumer after a restart', async () => {
    await natsMicroservice.close();
    natsMicroservice = await createNatsMicroservice();
    await natsMicroservice.get(NatsService).startJetStreamConsumers();

    await natsMicroservice.get(NatsTransportService).emitEvent(createdSubject, { orderId: 4 });

    await vi.waitFor(
      () =>
        expect(handledEvents).toEqual([
          { payload: { orderId: 1 }, subject: createdSubject },
          { payload: { orderId: 4 }, subject: createdSubject },
        ]),
      { timeout: 5_000 },
    );
  });
});
