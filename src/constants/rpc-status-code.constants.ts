import { HttpStatus } from '@nestjs/common';

import { TransportRpcStatus } from './transport-rpc-status.enum.js';

export const RpcStatusCode: Record<number, number> = {
  [TransportRpcStatus.CANCELLED]: 499,
  [TransportRpcStatus.UNKNOWN]: HttpStatus.BAD_GATEWAY,
  [TransportRpcStatus.INVALID_ARGUMENT]: HttpStatus.BAD_REQUEST,
  [TransportRpcStatus.DEADLINE_EXCEEDED]: HttpStatus.GATEWAY_TIMEOUT,
  [TransportRpcStatus.NOT_FOUND]: HttpStatus.NOT_FOUND,
  [TransportRpcStatus.ALREADY_EXISTS]: HttpStatus.CONFLICT,
  [TransportRpcStatus.PERMISSION_DENIED]: HttpStatus.FORBIDDEN,
  [TransportRpcStatus.RESOURCE_EXHAUSTED]: HttpStatus.TOO_MANY_REQUESTS,
  [TransportRpcStatus.FAILED_PRECONDITION]: HttpStatus.PRECONDITION_FAILED,
  [TransportRpcStatus.ABORTED]: HttpStatus.GONE,
  [TransportRpcStatus.OUT_OF_RANGE]: HttpStatus.PAYLOAD_TOO_LARGE,
  [TransportRpcStatus.UNIMPLEMENTED]: HttpStatus.NOT_IMPLEMENTED,
  [TransportRpcStatus.INTERNAL]: HttpStatus.INTERNAL_SERVER_ERROR,
  [TransportRpcStatus.UNAVAILABLE]: HttpStatus.SERVICE_UNAVAILABLE,
  [TransportRpcStatus.UNAUTHENTICATED]: HttpStatus.UNAUTHORIZED,
};
