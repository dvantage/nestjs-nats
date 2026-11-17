import type { JetStreamManager, StreamInfo } from '@nats-io/jetstream';

const getNormalizedErrorMessage = (error: unknown): string => {
  let errorMessage = '';

  if (error instanceof Error) {
    errorMessage = error.message.toLowerCase();
  } else if (typeof error === 'string') {
    errorMessage = error.toLowerCase();
  }

  const apiDescription = (error as { api_error?: { description?: string | null } } | undefined)?.api_error?.description;

  if (apiDescription === undefined || apiDescription === null || apiDescription === '') {
    return errorMessage;
  }

  return `${errorMessage} ${apiDescription}`.toLowerCase();
};

const isStreamNotFoundError = (error: unknown): boolean => {
  const message = getNormalizedErrorMessage(error);
  return message.includes('stream not found') || message.includes('no stream matches');
};

const isStreamAlreadyExistsError = (error: unknown): boolean => {
  const message = getNormalizedErrorMessage(error);
  return message.includes('already in use') || message.includes('name already in use');
};

export const ensureOwnedNatsStream = async (
  jetStreamManager: JetStreamManager,
  streamName: string,
  streamSubjects: string[],
): Promise<void> => {
  let streamInfo: StreamInfo | undefined;

  try {
    streamInfo = await jetStreamManager.streams.info(streamName);
  } catch (error) {
    if (!isStreamNotFoundError(error)) {
      throw error;
    }

    try {
      await jetStreamManager.streams.add({
        name: streamName,
        subjects: streamSubjects,
      });
      return;
    } catch (addError) {
      if (!isStreamAlreadyExistsError(addError)) {
        throw addError;
      }

      streamInfo = await jetStreamManager.streams.info(streamName);
    }
  }

  if (streamInfo === undefined) {
    return;
  }

  const mergedSubjects = [...new Set([...(streamInfo.config.subjects ?? []), ...streamSubjects])];

  await jetStreamManager.streams.update(streamName, {
    ...streamInfo.config,
    subjects: mergedSubjects,
  });
};
