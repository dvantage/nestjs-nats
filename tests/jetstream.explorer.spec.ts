import { MetadataScanner, Reflector } from '@nestjs/core';
import { describe, expect, it, vi } from 'vitest';

import { JetStreamEvent } from '../src/decorators/jetstream-event.decorator.js';
import type { JetStreamEventContextInterface } from '../src/interfaces/jetstream-event-context.interface.js';
import type { JetStreamSubscriptionInputInterface } from '../src/interfaces/jetstream-subscription-input.interface.js';
import { JetStreamExplorer } from '../src/jetstream.explorer.js';
import { natsModuleOptionsFixture as natsModuleOptions } from './fixtures/nats-module-options.fixture.js';

class TransportEventsController {
  readonly removedUserIds: number[] = [];
  readonly handledSubjects: string[] = [];

  @JetStreamEvent('event.main.user.removed')
  onMainUserRemovedEvent(payload: { userId: number }, jetStreamEventContext: JetStreamEventContextInterface): void {
    this.removedUserIds.push(payload.userId);
    this.handledSubjects.push(jetStreamEventContext.subject);
  }

  countRemovedUsers(): number {
    return this.removedUserIds.length;
  }
}

describe('JetStreamExplorer', () => {
  it('registers every @JetStreamEvent handler under a durable name built from the prefix and the subject', async () => {
    const transportEventsController = new TransportEventsController();
    const discoveryService = {
      getControllers: vi.fn(() => [{ instance: transportEventsController }, { instance: null }]),
    };
    const natsService = {
      registerJetStreamSubscription: vi.fn<
        (jetStreamRegistration: JetStreamSubscriptionInputInterface) => Promise<void>
      >(() => Promise.resolve()),
    };
    const jetStreamExplorer = new JetStreamExplorer(
      discoveryService as never,
      new MetadataScanner(),
      new Reflector(),
      natsService as never,
      natsModuleOptions,
    );

    await jetStreamExplorer.onApplicationBootstrap();

    expect(natsService.registerJetStreamSubscription).toHaveBeenCalledTimes(1);

    const [jetStreamRegistration] = natsService.registerJetStreamSubscription.mock.calls[0];
    expect(jetStreamRegistration.subject).toBe('event.main.user.removed');
    expect(jetStreamRegistration.durableName).toBe('ecommerce-main-events-event_main_user_removed');

    await jetStreamRegistration.handler({ userId: 5 }, { subject: 'event.main.user.removed' });

    expect(transportEventsController.removedUserIds).toEqual([5]);
    expect(transportEventsController.handledSubjects).toEqual(['event.main.user.removed']);
  });
});
