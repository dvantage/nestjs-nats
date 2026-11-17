import type { OnApplicationBootstrap } from '@nestjs/common';
import { Inject, Injectable } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';

import { NATS_MODULE_OPTIONS } from './constants/nats.constants.js';
import { JETSTREAM_EVENT_METADATA } from './decorators/jetstream-event.decorator.js';
import type { NatsModuleOptionsInterface } from './interfaces/nats-module-options.interface.js';
import { NatsService } from './nats.service.js';
import type { JetStreamEventHandler } from './types/jetstream-event-handler.type.js';

@Injectable()
export class JetStreamExplorer implements OnApplicationBootstrap {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly reflector: Reflector,
    private readonly natsService: NatsService,
    @Inject(NATS_MODULE_OPTIONS)
    private readonly natsModuleOptions: NatsModuleOptionsInterface,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    for (const controllerWrapper of this.discoveryService.getControllers()) {
      const controller: unknown = controllerWrapper.instance;

      if (typeof controller !== 'object' || controller === null) {
        continue;
      }

      const prototype = Object.getPrototypeOf(controller) as object;

      for (const methodName of this.metadataScanner.getAllMethodNames(prototype)) {
        const method: unknown = (controller as Record<string, unknown>)[methodName];

        if (typeof method !== 'function') {
          continue;
        }

        const subject = this.reflector.get<string | undefined>(JETSTREAM_EVENT_METADATA, method);

        if (subject === undefined) {
          continue;
        }

        await this.natsService.registerJetStreamSubscription({
          subject,
          durableName: `${this.natsModuleOptions.durablePrefix}-${subject.replaceAll('.', '_')}`,
          handler: (method as JetStreamEventHandler).bind(controller),
        });
      }
    }
  }
}
