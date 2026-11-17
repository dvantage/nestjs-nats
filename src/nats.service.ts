import type { ConsumerInfo, ConsumerMessages, JetStreamClient } from '@nats-io/jetstream';
import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  JetStreamApiCodes,
  JetStreamApiError,
  jetstreamManager,
} from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { connect } from '@nats-io/transport-node';
import type { OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Inject, Injectable, InternalServerErrorException, ServiceUnavailableException } from '@nestjs/common';

import { NATS_MODULE_OPTIONS } from './constants/nats.constants.js';
import { ensureOwnedNatsStream } from './helpers/nats-stream.helper.js';
import type { JetStreamSubscriptionInputInterface } from './interfaces/jetstream-subscription-input.interface.js';
import type { NatsModuleOptionsInterface } from './interfaces/nats-module-options.interface.js';

@Injectable()
export class NatsService implements OnModuleInit, OnModuleDestroy {
  private readonly reconciliationRetryDelayMilliseconds = 5 * 1000;
  private readonly jetStreamRegistrations = new Map<string, JetStreamSubscriptionInputInterface>();
  private readonly consumerMessagesByDurableName = new Map<string, ConsumerMessages>();
  private connection: NatsConnection | null = null;
  private jetStreamClient: JetStreamClient | null = null;
  private reconciliationRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private reconciliationPromise: Promise<void> | null = null;
  private reconciliationRequested = false;
  private jetStreamConsumersStarted = false;
  private isShuttingDown = false;

  constructor(
    @Inject(NATS_MODULE_OPTIONS)
    private readonly natsModuleOptions: NatsModuleOptionsInterface,
  ) {}

  async onModuleInit(): Promise<void> {
    const streamConnection = await connect({
      servers: this.natsModuleOptions.servers,
      token: this.natsModuleOptions.token,
    });

    try {
      const jetStreamManager = await jetstreamManager(streamConnection);

      for (const natsStream of this.natsModuleOptions.streams) {
        await ensureOwnedNatsStream(jetStreamManager, natsStream.name, natsStream.subjects);
      }
    } finally {
      await streamConnection.drain();
    }

    await this.reconcileState();
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;
    this.clearReconciliationTimer();
    this.clearActiveSubscriptions();

    const connection = this.connection;
    this.connection = null;
    this.jetStreamClient = null;

    if (connection && !connection.isClosed()) {
      await connection.close();
    }
  }

  async publishJetStreamMessage(subject: string, payload: unknown): Promise<void> {
    if (!this.jetStreamClient) {
      throw new ServiceUnavailableException(`JetStream publication to ${subject} failed: no NATS connection`);
    }

    const messageBody = JSON.stringify(payload);
    await this.jetStreamClient.publish(subject, messageBody);
  }

  async registerJetStreamSubscription(jetStreamRegistration: JetStreamSubscriptionInputInterface): Promise<void> {
    if (this.jetStreamRegistrations.has(jetStreamRegistration.durableName)) {
      throw new InternalServerErrorException(
        `JetStream consumer ${jetStreamRegistration.durableName} is already registered`,
      );
    }

    this.jetStreamRegistrations.set(jetStreamRegistration.durableName, jetStreamRegistration);

    if (!this.jetStreamConsumersStarted) {
      return;
    }

    if (!this.connection) {
      this.scheduleReconciliation();
      return;
    }

    await this.reconcileState();
  }

  async startJetStreamConsumers(): Promise<void> {
    this.jetStreamConsumersStarted = true;
    await this.reconcileState();
  }

  private reconcileState(): Promise<void> {
    this.reconciliationRequested = true;

    if (this.reconciliationPromise !== null) {
      return this.reconciliationPromise;
    }

    const reconciliationPromise = this.runReconciliation().catch((e) => {
      console.log('NATS reconciliation failed, retry scheduled', e);
      this.scheduleReconciliation();
    });
    this.reconciliationPromise = reconciliationPromise;

    void reconciliationPromise
      .finally(() => {
        if (this.reconciliationPromise === reconciliationPromise) {
          this.reconciliationPromise = null;
        }

        if (this.reconciliationRequested && !this.isShuttingDown) {
          this.reconcileStateInBackground();
        }
      })
      .catch((e) => console.log('NATS reconciliation finalization failed', e));

    return reconciliationPromise;
  }

  private async runReconciliation(): Promise<void> {
    while (this.reconciliationRequested && !this.isShuttingDown) {
      this.reconciliationRequested = false;
      await this.reconcileOnce();
    }
  }

  private async reconcileOnce(): Promise<void> {
    if (!(await this.ensureConnection())) {
      return;
    }

    if (!this.jetStreamConsumersStarted) {
      this.clearReconciliationTimer();
      return;
    }

    for (const jetStreamRegistration of this.jetStreamRegistrations.values()) {
      await this.openJetStreamSubscription(jetStreamRegistration);
    }

    if (this.consumerMessagesByDurableName.size === this.jetStreamRegistrations.size) {
      this.clearReconciliationTimer();
      return;
    }

    this.scheduleReconciliation();
  }

  private async ensureConnection(): Promise<boolean> {
    if (this.connection && !this.connection.isClosed() && this.jetStreamClient) {
      return true;
    }

    this.clearActiveSubscriptions();
    this.connection = null;
    this.jetStreamClient = null;

    try {
      const connection = await connect({
        servers: this.natsModuleOptions.servers,
        token: this.natsModuleOptions.token,
        name: this.natsModuleOptions.connectionName,
        maxReconnectAttempts: this.natsModuleOptions.maxReconnectAttempts,
      });

      if (this.isShuttingDown) {
        await connection.close();
        return false;
      }

      this.connection = connection;
      this.jetStreamClient = jetstream(connection);
      this.watchConnection(connection);
      return true;
    } catch (e) {
      console.log('NATS connection failed, retry scheduled', e);
      this.scheduleReconciliation();
      return false;
    }
  }

  private watchConnection(connection: NatsConnection): void {
    void connection
      .closed()
      .then((closeError) => {
        if (this.connection !== connection) {
          return;
        }

        this.connection = null;
        this.jetStreamClient = null;
        this.clearActiveSubscriptions();

        if (this.isShuttingDown) {
          return;
        }

        console.log('NATS connection closed, reconnect scheduled', closeError);
        this.scheduleReconciliation();
      })
      .catch((e) => console.log('NATS connection close watcher failed', e));
  }

  private async openJetStreamSubscription(jetStreamRegistration: JetStreamSubscriptionInputInterface): Promise<void> {
    if (this.consumerMessagesByDurableName.has(jetStreamRegistration.durableName)) {
      return;
    }

    const connection = this.connection;
    const jetStreamClient = this.jetStreamClient;

    if (!connection || !jetStreamClient) {
      this.scheduleReconciliation();
      return;
    }

    let consumerMessages: ConsumerMessages;

    try {
      const jetStreamManager = await jetstreamManager(connection);
      const streamName = await jetStreamManager.streams.find(jetStreamRegistration.subject);
      let consumerInfo: ConsumerInfo | null = null;

      try {
        consumerInfo = await jetStreamManager.consumers.info(streamName, jetStreamRegistration.durableName);
      } catch (e) {
        if (!(e instanceof JetStreamApiError) || e.code !== JetStreamApiCodes.ConsumerNotFound) {
          throw e;
        }
      }

      if (consumerInfo) {
        if (
          consumerInfo.config.deliver_subject !== undefined ||
          consumerInfo.config.filter_subject !== jetStreamRegistration.subject ||
          consumerInfo.config.deliver_policy !== DeliverPolicy.All ||
          consumerInfo.config.ack_policy !== AckPolicy.Explicit
        ) {
          throw new InternalServerErrorException(
            `JetStream consumer ${jetStreamRegistration.durableName} has incompatible configuration`,
          );
        }
      } else {
        await jetStreamManager.consumers.add(streamName, {
          durable_name: jetStreamRegistration.durableName,
          ack_policy: AckPolicy.Explicit,
          deliver_policy: DeliverPolicy.All,
          filter_subject: jetStreamRegistration.subject,
        });
      }

      const consumer = await jetStreamClient.consumers.get(streamName, jetStreamRegistration.durableName);
      consumerMessages = await consumer.consume();
    } catch (e) {
      console.log('JetStream subscription failed, retry scheduled', jetStreamRegistration.subject, e);
      this.scheduleReconciliation();
      return;
    }

    if (this.isShuttingDown || this.connection !== connection) {
      consumerMessages.stop();
      return;
    }

    this.consumerMessagesByDurableName.set(jetStreamRegistration.durableName, consumerMessages);
    this.watchJetStreamSubscription(jetStreamRegistration, consumerMessages);
    void this.consumeJetStreamMessages(jetStreamRegistration, consumerMessages).catch((e) =>
      console.log('JetStream consumer loop failed', jetStreamRegistration.subject, e),
    );
  }

  private watchJetStreamSubscription(
    jetStreamRegistration: JetStreamSubscriptionInputInterface,
    consumerMessages: ConsumerMessages,
  ): void {
    void consumerMessages
      .closed()
      .then((closeError) => {
        if (this.consumerMessagesByDurableName.get(jetStreamRegistration.durableName) !== consumerMessages) {
          return;
        }

        this.consumerMessagesByDurableName.delete(jetStreamRegistration.durableName);

        if (this.isShuttingDown) {
          return;
        }

        console.log('JetStream subscription closed, retry scheduled', jetStreamRegistration.subject, closeError);
        this.scheduleReconciliation();
      })
      .catch((e) => console.log('JetStream subscription close watcher failed', jetStreamRegistration.subject, e));
  }

  private async consumeJetStreamMessages(
    jetStreamRegistration: JetStreamSubscriptionInputInterface,
    consumerMessages: ConsumerMessages,
  ): Promise<void> {
    try {
      for await (const message of consumerMessages) {
        try {
          const payload = message.json<unknown>();
          await jetStreamRegistration.handler(payload, { subject: message.subject });
          message.ack();
        } catch (e) {
          console.log('JetStream event handling failed, message terminated', jetStreamRegistration.subject, e);
          message.term();
        }
      }
    } catch (e) {
      console.log('JetStream message loop failed, retry scheduled', jetStreamRegistration.subject, e);
    }

    if (this.consumerMessagesByDurableName.get(jetStreamRegistration.durableName) !== consumerMessages) {
      return;
    }

    this.consumerMessagesByDurableName.delete(jetStreamRegistration.durableName);
    consumerMessages.stop();

    if (!this.isShuttingDown) {
      this.scheduleReconciliation();
    }
  }

  private clearActiveSubscriptions(): void {
    for (const consumerMessages of this.consumerMessagesByDurableName.values()) {
      consumerMessages.stop();
    }

    this.consumerMessagesByDurableName.clear();
  }

  private scheduleReconciliation(): void {
    if (this.isShuttingDown || this.reconciliationRetryTimer) {
      return;
    }

    this.reconciliationRetryTimer = setTimeout(() => {
      this.reconciliationRetryTimer = null;
      this.reconcileStateInBackground();
    }, this.reconciliationRetryDelayMilliseconds);
  }

  private reconcileStateInBackground(): void {
    void this.reconcileState().catch((e) => console.log('NATS reconciliation failed', e));
  }

  private clearReconciliationTimer(): void {
    if (!this.reconciliationRetryTimer) {
      return;
    }

    clearTimeout(this.reconciliationRetryTimer);
    this.reconciliationRetryTimer = null;
  }
}
