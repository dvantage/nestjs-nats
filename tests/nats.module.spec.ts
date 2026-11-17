import { describe, expect, it } from 'vitest';

import { NATS_MODULE_OPTIONS } from '../src/constants/nats.constants.js';
import { NatsModule } from '../src/nats.module.js';
import { NatsService } from '../src/nats.service.js';
import { NatsTransportService } from '../src/nats-transport.service.js';
import { natsModuleOptionsFixture as natsModuleOptions } from './fixtures/nats-module-options.fixture.js';

describe('NatsModule', () => {
  it('registers a global module with the options and exports the NATS services', () => {
    const natsDynamicModule = NatsModule.forRoot(natsModuleOptions);

    expect(natsDynamicModule.global).toBe(true);
    expect(natsDynamicModule.providers).toContainEqual({ provide: NATS_MODULE_OPTIONS, useValue: natsModuleOptions });
    expect(natsDynamicModule.exports).toEqual([NatsService, NatsTransportService]);
  });
});
