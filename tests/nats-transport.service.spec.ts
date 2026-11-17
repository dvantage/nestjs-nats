import type { MsgHdrs } from '@nats-io/nats-core';
import { HttpException, HttpStatus } from '@nestjs/common';
import type { NatsRecord } from '@nestjs/microservices';
import { map, NEVER, of, throwError, timer } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { TransportRpcStatus } from '../src/constants/transport-rpc-status.enum.js';
import type { NatsModuleOptionsInterface } from '../src/interfaces/nats-module-options.interface.js';
import type { NatsRequestInterface } from '../src/interfaces/nats-request.interface.js';
import { NatsTransportService } from '../src/nats-transport.service.js';
import { natsModuleOptionsFixture as natsModuleOptions } from './fixtures/nats-module-options.fixture.js';

describe('NatsTransportService', () => {
  const createContext = (moduleOptions: NatsModuleOptionsInterface = natsModuleOptions) => {
    const natsRpcClient = {
      send: vi.fn(),
    };

    const natsService = {
      publishJetStreamMessage: vi.fn(),
    };

    const natsTransportService = new NatsTransportService(natsRpcClient as never, natsService as never, moduleOptions);

    return { natsTransportService, natsRpcClient, natsService };
  };

  const readSentRecord = (natsRpcClient: { send: ReturnType<typeof vi.fn> }) => {
    const [pattern, record] = natsRpcClient.send.mock.calls[0] as [string, NatsRecord<unknown, MsgHdrs>];
    return { pattern, record };
  };

  beforeEach(() => {
    vi.spyOn(console, 'log').mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    [TransportRpcStatus.PERMISSION_DENIED, HttpStatus.FORBIDDEN],
    [TransportRpcStatus.NOT_FOUND, HttpStatus.NOT_FOUND],
    [TransportRpcStatus.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE],
    [TransportRpcStatus.UNKNOWN, HttpStatus.BAD_GATEWAY],
    [TransportRpcStatus.CANCELLED, 499],
  ])('forwards main service code %s as integer HTTP status %s', async (rpcStatus, httpStatus) => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(throwError(() => ({ code: rpcStatus, message: 'Main service error' })));

    const sendPromise = context.natsTransportService.sendMessage('rpc.main.storage.findById', 10, true);

    await expect(sendPromise).rejects.toBeInstanceOf(HttpException);
    await expect(sendPromise).rejects.toMatchObject({ status: httpStatus, message: 'Main service error' });
  });

  it('forwards an error without a known transport code as an internal server error', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(throwError(() => ({ code: 'TIMEOUT', message: 'Request timed out' })));

    const sendPromise = context.natsTransportService.sendMessage('rpc.main.storage.findById', 10, true);

    await expect(sendPromise).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: 'Request timed out',
    });
  });

  it('hides the main service error unless the caller forwards it', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(
      throwError(() => ({ code: TransportRpcStatus.NOT_FOUND, message: 'File not found' })),
    );

    const sendPromise = context.natsTransportService.sendMessage('rpc.main.storage.findById', 10);

    await expect(sendPromise).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageErrorMessage,
    });
  });

  it('sends an empty string when the message is missing', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(of(3));

    await expect(context.natsTransportService.sendMessage('rpc.main.users.count')).resolves.toBe(3);
    expect(context.natsRpcClient.send).toHaveBeenCalledWith('rpc.main.users.count', '');
  });

  it('limits sendMessage by its own timeout', async () => {
    const context = createContext({ ...natsModuleOptions, sendMessageTimeoutMilliseconds: 10 });
    context.natsRpcClient.send.mockReturnValue(NEVER);

    await expect(context.natsTransportService.sendMessage('rpc.main.users.count', 1)).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageErrorMessage,
    });
  });

  it('limits sendMessageByBuilder by its own timeout', async () => {
    const context = createContext({ ...natsModuleOptions, sendMessageByBuilderTimeoutMilliseconds: 10 });
    context.natsRpcClient.send.mockReturnValue(NEVER);

    const sendPromise = context.natsTransportService.sendMessageByBuilder(
      { headers: {} },
      'rpc.main.projects.findOne',
      { id: 1 },
    );

    await expect(sendPromise).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageByBuilderErrorMessage,
    });
  });

  it('does not limit sendMessage by the sendMessageByBuilder timeout', async () => {
    const context = createContext({ ...natsModuleOptions, sendMessageByBuilderTimeoutMilliseconds: 10 });
    context.natsRpcClient.send.mockReturnValue(timer(30).pipe(map(() => 'late reply')));

    await expect(context.natsTransportService.sendMessage('rpc.main.users.count', 1)).resolves.toBe('late reply');
  });

  it('sends the session user, the nginx flag and the first non-empty request path as headers', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(of({ id: 1 }));
    const req: NatsRequestInterface = {
      headers: { 'x-nginx-proxy': 'true' },
      path: '',
      originalUrl: '/ecommerce/orders?page=2',
      url: '/orders?page=2',
      session: { passport: { user: { id: 7, roles: ['client'] } } },
    };

    await context.natsTransportService.sendMessageByBuilder(req, 'rpc.main.projects.findOne', { id: 1 });

    const { pattern, record } = readSentRecord(context.natsRpcClient);
    expect(pattern).toBe('rpc.main.projects.findOne');
    expect(record.data).toEqual({ id: 1 });
    expect(record.headers?.get('user-session')).toBe('{"id":7,"roles":["client"]}');
    expect(record.headers?.get('nginx-request')).toBe('true');
    expect(record.headers?.get('req-path')).toBe('/ecommerce/orders?page=2');
  });

  it('sends a null session user, a false nginx flag and the url of a socket handshake request', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(of([]));

    await context.natsTransportService.sendMessageByBuilder(
      { headers: {}, url: '/socket.io/?EIO=4' },
      'rpc.main.projects.chat.findMany',
    );

    const { record } = readSentRecord(context.natsRpcClient);
    expect(record.headers?.get('user-session')).toBe('null');
    expect(record.headers?.get('nginx-request')).toBe('false');
    expect(record.headers?.get('req-path')).toBe('/socket.io/?EIO=4');
  });

  it('sends the root path when the request has no path', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(of([]));

    await context.natsTransportService.sendMessageByBuilder({ headers: {} }, 'rpc.main.projects.chat.findMany');

    const { record } = readSentRecord(context.natsRpcClient);
    expect(record.headers?.get('req-path')).toBe('/');
  });

  it('hides the RPC error of sendMessageByBuilder behind its configured text', async () => {
    const context = createContext();
    context.natsRpcClient.send.mockReturnValue(
      throwError(() => ({ code: TransportRpcStatus.NOT_FOUND, message: 'Project not found' })),
    );

    const sendPromise = context.natsTransportService.sendMessageByBuilder(
      { headers: {} },
      'rpc.main.projects.findOne',
      { id: 1 },
    );

    await expect(sendPromise).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.sendMessageByBuilderErrorMessage,
    });
  });

  it('throws the configured event error when the publication fails', async () => {
    const context = createContext();
    context.natsService.publishJetStreamMessage.mockRejectedValue(new Error('no NATS connection'));

    await expect(
      context.natsTransportService.emitEvent('event.ecommerce.order.created', { id: 1 }),
    ).rejects.toMatchObject({
      status: HttpStatus.INTERNAL_SERVER_ERROR,
      message: natsModuleOptions.eventErrorMessage,
    });
  });

  it('publishes the event payload through JetStream', async () => {
    const context = createContext();
    context.natsService.publishJetStreamMessage.mockResolvedValue(undefined);

    await context.natsTransportService.emitEvent('event.ecommerce.order.created', { id: 1 });

    expect(context.natsService.publishJetStreamMessage).toHaveBeenCalledWith('event.ecommerce.order.created', {
      id: 1,
    });
  });
});
