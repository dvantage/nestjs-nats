import { SetMetadata } from '@nestjs/common';

export const JETSTREAM_EVENT_METADATA = 'jetstream:event';

export const JetStreamEvent = (subject: string): MethodDecorator => SetMetadata(JETSTREAM_EVENT_METADATA, subject);
