import type { MsgHdrs } from '@nats-io/nats-core';
import { headers } from '@nats-io/nats-core';
import type { OnApplicationBootstrap } from '@nestjs/common';
import { HttpException, HttpStatus, Inject, Injectable } from '@nestjs/common';
import { ClientProxy, NatsRecordBuilder } from '@nestjs/microservices';
import { lastValueFrom, timeout } from 'rxjs';

import { NATS_MODULE_OPTIONS, NATS_RPC_PROVIDER_NAME } from './constants/nats.constants.js';
import { RpcStatusCode } from './constants/rpc-status-code.constants.js';
import type { NatsModuleOptionsInterface } from './interfaces/nats-module-options.interface.js';
import type { NatsRequestInterface } from './interfaces/nats-request.interface.js';
import { NatsService } from './nats.service.js';

@Injectable()
export class NatsTransportService implements OnApplicationBootstrap {
  constructor(
    @Inject(NATS_RPC_PROVIDER_NAME)
    private readonly natsRpcClient: ClientProxy,
    private readonly natsService: NatsService,
    @Inject(NATS_MODULE_OPTIONS)
    private readonly natsModuleOptions: NatsModuleOptionsInterface,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.natsRpcClient.connect();
  }

  async emitEvent<T>(pattern: string, message?: T): Promise<void> {
    try {
      await this.natsService.publishJetStreamMessage(pattern, message);
    } catch (e) {
      console.log('NatsTransportService - emitEvent error', pattern, message, e);
      throw new HttpException(this.natsModuleOptions.eventErrorMessage, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  sendMessage<R>(pattern: string): Promise<R>;

  sendMessage<R, T>(pattern: string, message?: T, sendOriginalError?: boolean): Promise<R>;

  async sendMessage<R, T>(pattern: string, message?: T | string, sendOriginalError = false): Promise<R> {
    if (message === undefined || message === null) {
      message = '';
    }

    try {
      return await this.sendRpcRequest<R, T | string>(
        pattern,
        message,
        this.natsModuleOptions.sendMessageTimeoutMilliseconds,
      );
    } catch (e) {
      console.log('NatsTransportService - sendMessage error', e, pattern, message);

      if (sendOriginalError) {
        const rpcError = e as { code: number; message: string };
        throw new HttpException(rpcError.message, RpcStatusCode[rpcError.code] ?? HttpStatus.INTERNAL_SERVER_ERROR);
      }

      throw new HttpException(this.natsModuleOptions.sendMessageErrorMessage, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  async sendMessageByBuilder<R, T>(req: NatsRequestInterface, pattern: string, payload?: T): Promise<R> {
    const natsHeaders = this.buildNatsHeadersByReq(req);
    const record = new NatsRecordBuilder<T | undefined>().setData(payload).setHeaders(natsHeaders).build();

    try {
      return await this.sendRpcRequest<R, typeof record>(
        pattern,
        record,
        this.natsModuleOptions.sendMessageByBuilderTimeoutMilliseconds,
      );
    } catch (e) {
      console.log('NatsTransportService - sendMessageByBuilder error', e, pattern, payload);
      throw new HttpException(
        this.natsModuleOptions.sendMessageByBuilderErrorMessage,
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  private buildNatsHeadersByReq(req: NatsRequestInterface): MsgHdrs {
    const natsHeaders = headers();
    natsHeaders.set('user-session', this.serializeSessionUserData(req));
    natsHeaders.set('nginx-request', String(req.headers['x-nginx-proxy'] === 'true'));
    natsHeaders.set('req-path', this.resolveReqPath(req));
    return natsHeaders;
  }

  private resolveReqPath(req: NatsRequestInterface): string {
    const reqPathCandidates = [req.path, req.originalUrl, req.url];

    for (const reqPathCandidate of reqPathCandidates) {
      if (typeof reqPathCandidate === 'string' && reqPathCandidate.trim() !== '') {
        return reqPathCandidate;
      }
    }

    return '/';
  }

  private serializeSessionUserData(req: NatsRequestInterface): string {
    const sessionUserData = req.session?.passport?.user ?? null;

    try {
      return JSON.stringify(sessionUserData);
    } catch {
      return '';
    }
  }

  private sendRpcRequest<R, T>(
    pattern: string,
    rpcRequestBody: T,
    timeoutMilliseconds: number | undefined,
  ): Promise<R> {
    const rpcReply = this.natsRpcClient.send<R, T>(pattern, rpcRequestBody);

    if (timeoutMilliseconds === undefined) {
      return lastValueFrom(rpcReply);
    }

    return lastValueFrom(rpcReply.pipe(timeout(timeoutMilliseconds)));
  }
}
