import { HttpException, HttpStatus } from '@nestjs/common';
import { lastValueFrom } from 'rxjs';
import { describe, expect, it } from 'vitest';

import { TransportRpcStatus } from '../../src/constants/transport-rpc-status.enum.js';
import { HttpToRpcExceptionFilter } from '../../src/filters/http-to-rpc-exception.filter.js';

describe('HttpToRpcExceptionFilter', () => {
  it.each([
    [HttpStatus.NOT_FOUND, TransportRpcStatus.NOT_FOUND],
    [HttpStatus.SERVICE_UNAVAILABLE, TransportRpcStatus.UNAVAILABLE],
    [HttpStatus.HTTP_VERSION_NOT_SUPPORTED, TransportRpcStatus.UNAVAILABLE],
    [499, TransportRpcStatus.CANCELLED],
    [451, TransportRpcStatus.UNKNOWN],
  ])('maps HTTP status %s to RPC code %s', async (httpStatus, rpcStatus) => {
    const httpToRpcExceptionFilter = new HttpToRpcExceptionFilter();

    const rpcError = lastValueFrom(httpToRpcExceptionFilter.catch(new HttpException('Project not found', httpStatus)));

    await expect(rpcError).rejects.toEqual({ code: rpcStatus, message: 'Project not found', details: undefined });
  });

  it('passes the array details of the HTTP error to the RPC error', async () => {
    const httpToRpcExceptionFilter = new HttpToRpcExceptionFilter();
    const httpException = new HttpException(
      { message: 'Invalid payload', details: ['projectId must be a number'] },
      HttpStatus.BAD_REQUEST,
    );

    const rpcError = lastValueFrom(httpToRpcExceptionFilter.catch(httpException));

    await expect(rpcError).rejects.toEqual({
      code: TransportRpcStatus.INVALID_ARGUMENT,
      message: 'Invalid payload',
      details: ['projectId must be a number'],
    });
  });
});
