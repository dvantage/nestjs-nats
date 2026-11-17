# @dvantage/nestjs-nats

NATS for NestJS 12 services: JetStream events with durable pull consumers and request/reply RPC over the NestJS NATS transport.

## Installation

```bash
pnpm add @dvantage/nestjs-nats @nats-io/jetstream @nats-io/nats-core @nats-io/transport-node
```

Peer dependencies: `@nestjs/common`, `@nestjs/core` and `@nestjs/microservices` 12, `@nats-io/jetstream`, `@nats-io/nats-core` and `@nats-io/transport-node` 3.5, `rxjs` 7.8. The package is ESM only.

## Setup

```ts
NatsModule.forRoot({
  servers: [configService.natsUrl],
  token: configService.natsSecretKey,
  connectionName: configService.natsConnectionName,
  maxReconnectAttempts: configService.natsMaxReconnectAttempts,
  streams: [{ name: configService.natsEventsStream, subjects: configService.natsEventsSubjects }],
  durablePrefix: configService.natsConsumerDurable,
  eventErrorMessage: 'TransportService - event transmission error',
  sendMessageErrorMessage: 'Websocket to Main - message transmission error',
  sendMessageByBuilderErrorMessage: 'Websocket to Main - message transmission error',
});
```

The module is global: `NatsService` and `NatsTransportService` can be injected anywhere after one `forRoot`.

| Option | Meaning |
|---|---|
| `servers`, `token` | NATS servers and token for JetStream and the RPC client |
| `connectionName` | name of the JetStream connection |
| `maxReconnectAttempts` | reconnect attempts of the JetStream connection |
| `streams` | streams created at startup; an existing stream gets the missing subjects |
| `durablePrefix` | prefix of the durable consumer names |
| `eventErrorMessage` | text of the `HttpException` thrown by `emitEvent` |
| `sendMessageErrorMessage` | text of the `HttpException` thrown by `sendMessage` |
| `sendMessageByBuilderErrorMessage` | text of the `HttpException` thrown by `sendMessageByBuilder` |
| `sendMessageTimeoutMilliseconds` | reply timeout of `sendMessage`, none when not set |
| `sendMessageByBuilderTimeoutMilliseconds` | reply timeout of `sendMessageByBuilder`, none when not set |

## Events

```ts
@Controller('transport/events')
export class TransportEventsController {
  constructor(private readonly payoneerPayment: PayoneerPayment) {}

  @JetStreamEvent('event.main.user.removed')
  async onMainUserRemovedEvent(payload: { userId: number }): Promise<void> {
    await this.payoneerPayment.unlinkPayeeOfRemovedUser(payload.userId);
  }
}
```

- Every `@JetStreamEvent` method of a controller gets a durable pull consumer `<durablePrefix>-<subject with dots replaced by underscores>` with `DeliverPolicy.All` and `AckPolicy.Explicit`.
- The handler receives the payload and a `JetStreamEventContextInterface` context with the subject. The message is acknowledged after the handler resolves; an error thrown by the handler terminates the message without redelivery.
- An existing consumer with the same name and another configuration, a push consumer included, is not used.
- Guards, filters, pipes and interceptors of NestJS do not run for these handlers: the library calls the controller method directly.
- Reading starts only when the application calls `startJetStreamConsumers()`, for example after `listen()`:

```ts
await app.listen(configService.serverPort, configService.serverHost);
await app.get(NatsService).startJetStreamConsumers();
```

- `natsTransportService.emitEvent(subject, payload)` publishes an event through JetStream.

## RPC

- `natsTransportService.sendMessage(pattern, message?, sendOriginalError?)` sends a request; a missing message is sent as an empty string. With `sendOriginalError` the RPC error becomes an `HttpException` with the HTTP status of its code.
- `natsTransportService.sendMessageByBuilder(req, pattern, payload?)` adds the headers `user-session` (the passport user of the session), `nginx-request` (the `x-nginx-proxy` header) and `req-path` (`req.path`, `req.originalUrl` or `req.url`).
- `@UseFilters(new HttpToRpcExceptionFilter())` on an RPC controller turns an `HttpException` into an RPC error with the code of `TransportRpcStatus`.
- `readNatsRequestHeaders(natsContext)` returns the three headers of a request, or `null` when the request has none.

## Development

```bash
pnpm lint
pnpm typecheck
pnpm test
NATS_URL=nats://127.0.0.1:4222 NATS_TOKEN=<token> pnpm test:e2e
pnpm build
```

`pnpm test:e2e` creates its own stream on the NATS server and deletes it afterwards.
