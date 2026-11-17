import type { DynamicModule } from '@nestjs/common';
import { Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { ClientsModule, Transport } from '@nestjs/microservices';

import { NATS_MODULE_OPTIONS, NATS_RPC_PROVIDER_NAME } from './constants/nats.constants.js';
import type { NatsModuleOptionsInterface } from './interfaces/nats-module-options.interface.js';
import { JetStreamExplorer } from './jetstream.explorer.js';
import { NatsService } from './nats.service.js';
import { NatsTransportService } from './nats-transport.service.js';

@Module({})
export class NatsModule {
  static forRoot(natsModuleOptions: NatsModuleOptionsInterface): DynamicModule {
    return {
      global: true,
      module: NatsModule,
      imports: [
        ClientsModule.register([
          {
            name: NATS_RPC_PROVIDER_NAME,
            transport: Transport.NATS,
            options: {
              servers: natsModuleOptions.servers,
              token: natsModuleOptions.token,
            },
          },
        ]),
        DiscoveryModule,
      ],
      providers: [
        {
          provide: NATS_MODULE_OPTIONS,
          useValue: natsModuleOptions,
        },
        NatsService,
        NatsTransportService,
        JetStreamExplorer,
      ],
      exports: [NatsService, NatsTransportService],
    };
  }
}
