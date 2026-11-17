import type { IncomingHttpHeaders } from 'node:http';

export interface NatsRequestInterface {
  headers: IncomingHttpHeaders;
  path?: string;
  originalUrl?: string;
  url?: string;
  session?: {
    passport?: {
      user?: unknown;
    };
  };
}
