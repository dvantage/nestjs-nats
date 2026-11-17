import { headers } from '@nats-io/nats-core';
import { NatsContext } from '@nestjs/microservices';
import { describe, expect, it } from 'vitest';

import { readNatsRequestHeaders } from '../../src/helpers/nats-request-headers.helper.js';

describe('readNatsRequestHeaders', () => {
  it('returns null when the RPC request carries no headers', () => {
    const natsContext = new NatsContext(['rpc.ecommerce.order.findMany', undefined]);

    expect(readNatsRequestHeaders(natsContext)).toBeNull();
  });

  it('reads the session user, the nginx flag and the request path', () => {
    const natsHeaders = headers();
    natsHeaders.set('user-session', '{"id":7,"roles":["client"]}');
    natsHeaders.set('nginx-request', 'true');
    natsHeaders.set('req-path', '/ecommerce/orders');
    const natsContext = new NatsContext(['rpc.ecommerce.order.findMany', natsHeaders]);

    expect(readNatsRequestHeaders<{ id: number; roles: string[] }>(natsContext)).toEqual({
      sessionUserData: { id: 7, roles: ['client'] },
      isReqFromNginx: true,
      reqPath: '/ecommerce/orders',
    });
  });

  it('returns a null session user for a guest request', () => {
    const natsHeaders = headers();
    natsHeaders.set('user-session', 'null');
    natsHeaders.set('nginx-request', 'false');
    natsHeaders.set('req-path', '/');
    const natsContext = new NatsContext(['rpc.main.projects.chat.findMany', natsHeaders]);

    expect(readNatsRequestHeaders(natsContext)).toEqual({
      sessionUserData: null,
      isReqFromNginx: false,
      reqPath: '/',
    });
  });

  it('returns a null session user when the session header is empty', () => {
    const natsHeaders = headers();
    natsHeaders.set('nginx-request', 'true');
    const natsContext = new NatsContext(['rpc.main.projects.chat.findMany', natsHeaders]);

    expect(readNatsRequestHeaders(natsContext)).toEqual({
      sessionUserData: null,
      isReqFromNginx: true,
      reqPath: '',
    });
  });
});
