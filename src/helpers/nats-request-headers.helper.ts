import type { MsgHdrs } from '@nats-io/nats-core';
import type { NatsContext } from '@nestjs/microservices';

import type { NatsRequestHeadersInterface } from '../interfaces/nats-request-headers.interface.js';

export const readNatsRequestHeaders = <T>(natsContext: NatsContext): NatsRequestHeadersInterface<T> | null => {
  const natsHeaders = natsContext.getHeaders() as MsgHdrs | null | undefined;

  if (!natsHeaders) {
    return null;
  }

  const sessionUserDataValue = natsHeaders.get('user-session');
  let sessionUserData: T | null = null;

  if (sessionUserDataValue !== '') {
    sessionUserData = JSON.parse(sessionUserDataValue) as T | null;
  }

  return {
    sessionUserData,
    isReqFromNginx: natsHeaders.get('nginx-request') === 'true',
    reqPath: natsHeaders.get('req-path'),
  };
};
