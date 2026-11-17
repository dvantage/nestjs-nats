import type { ExceptionFilter } from '@nestjs/common';
import { Catch, HttpException, HttpStatus } from '@nestjs/common';
import type { Observable } from 'rxjs';
import { throwError } from 'rxjs';

import { TransportRpcStatus } from '../constants/transport-rpc-status.enum.js';

@Catch(HttpException)
export class HttpToRpcExceptionFilter implements ExceptionFilter {
  static HttpStatusCode: Record<number, number> = {
    [HttpStatus.BAD_REQUEST]: TransportRpcStatus.INVALID_ARGUMENT,
    [HttpStatus.UNAUTHORIZED]: TransportRpcStatus.UNAUTHENTICATED,
    [HttpStatus.FORBIDDEN]: TransportRpcStatus.PERMISSION_DENIED,
    [HttpStatus.NOT_FOUND]: TransportRpcStatus.NOT_FOUND,
    [HttpStatus.CONFLICT]: TransportRpcStatus.ALREADY_EXISTS,
    [HttpStatus.GONE]: TransportRpcStatus.ABORTED,
    [HttpStatus.TOO_MANY_REQUESTS]: TransportRpcStatus.RESOURCE_EXHAUSTED,
    499: TransportRpcStatus.CANCELLED,
    [HttpStatus.INTERNAL_SERVER_ERROR]: TransportRpcStatus.INTERNAL,
    [HttpStatus.NOT_IMPLEMENTED]: TransportRpcStatus.UNIMPLEMENTED,
    [HttpStatus.BAD_GATEWAY]: TransportRpcStatus.UNKNOWN,
    [HttpStatus.SERVICE_UNAVAILABLE]: TransportRpcStatus.UNAVAILABLE,
    [HttpStatus.GATEWAY_TIMEOUT]: TransportRpcStatus.DEADLINE_EXCEEDED,
    [HttpStatus.HTTP_VERSION_NOT_SUPPORTED]: TransportRpcStatus.UNAVAILABLE,
    [HttpStatus.PAYLOAD_TOO_LARGE]: TransportRpcStatus.OUT_OF_RANGE,
    [HttpStatus.UNSUPPORTED_MEDIA_TYPE]: TransportRpcStatus.CANCELLED,
    [HttpStatus.UNPROCESSABLE_ENTITY]: TransportRpcStatus.CANCELLED,
    [HttpStatus.I_AM_A_TEAPOT]: TransportRpcStatus.UNKNOWN,
    [HttpStatus.METHOD_NOT_ALLOWED]: TransportRpcStatus.CANCELLED,
    [HttpStatus.PRECONDITION_FAILED]: TransportRpcStatus.FAILED_PRECONDITION,
  };

  catch(exception: HttpException): Observable<never> {
    const httpStatus = exception.getStatus();
    const httpExceptionResponse = exception.getResponse() as { details?: unknown };

    return throwError(() => ({
      code: HttpToRpcExceptionFilter.HttpStatusCode[httpStatus] ?? TransportRpcStatus.UNKNOWN,
      message: exception.message,
      details: Array.isArray(httpExceptionResponse.details) ? httpExceptionResponse.details : undefined,
    }));
  }
}
