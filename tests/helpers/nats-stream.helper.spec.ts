import { describe, expect, it, vi } from 'vitest';

import { ensureOwnedNatsStream } from '../../src/helpers/nats-stream.helper.js';

describe('ensureOwnedNatsStream', () => {
  const createJetStreamManager = () => ({
    streams: {
      info: vi.fn(),
      add: vi.fn(),
      update: vi.fn(),
    },
  });

  it('adds the missing subjects to an existing stream and keeps its configuration', async () => {
    const jetStreamManager = createJetStreamManager();
    jetStreamManager.streams.info.mockResolvedValue({
      config: { name: 'main-events-stream', subjects: ['event.main.>'], max_age: 0 },
    });

    await ensureOwnedNatsStream(jetStreamManager as never, 'main-events-stream', ['event.main.>', 'event.products.>']);

    expect(jetStreamManager.streams.add).not.toHaveBeenCalled();
    expect(jetStreamManager.streams.update).toHaveBeenCalledWith('main-events-stream', {
      name: 'main-events-stream',
      subjects: ['event.main.>', 'event.products.>'],
      max_age: 0,
    });
  });

  it('creates the stream when it does not exist', async () => {
    const jetStreamManager = createJetStreamManager();
    jetStreamManager.streams.info.mockRejectedValue(new Error('stream not found'));

    await ensureOwnedNatsStream(jetStreamManager as never, 'ecommerce-events-stream', ['event.ecommerce.>']);

    expect(jetStreamManager.streams.add).toHaveBeenCalledWith({
      name: 'ecommerce-events-stream',
      subjects: ['event.ecommerce.>'],
    });
    expect(jetStreamManager.streams.update).not.toHaveBeenCalled();
  });

  it('extends the stream that another instance created at the same time', async () => {
    const jetStreamManager = createJetStreamManager();
    jetStreamManager.streams.info.mockRejectedValueOnce(new Error('stream not found')).mockResolvedValueOnce({
      config: { name: 'ecommerce-events-stream', subjects: ['event.ecommerce.order.>'] },
    });
    jetStreamManager.streams.add.mockRejectedValue(
      new Error('stream name already in use with a different configuration'),
    );

    await ensureOwnedNatsStream(jetStreamManager as never, 'ecommerce-events-stream', ['event.ecommerce.payout.>']);

    expect(jetStreamManager.streams.update).toHaveBeenCalledWith('ecommerce-events-stream', {
      name: 'ecommerce-events-stream',
      subjects: ['event.ecommerce.order.>', 'event.ecommerce.payout.>'],
    });
  });

  it('rethrows an error that does not mean a missing stream', async () => {
    const jetStreamManager = createJetStreamManager();
    jetStreamManager.streams.info.mockRejectedValue(new Error('authorization violation'));

    await expect(
      ensureOwnedNatsStream(jetStreamManager as never, 'ecommerce-events-stream', ['event.ecommerce.>']),
    ).rejects.toThrow('authorization violation');
    expect(jetStreamManager.streams.add).not.toHaveBeenCalled();
  });
});
