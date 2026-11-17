export interface NatsRequestHeadersInterface<T> {
  sessionUserData: T | null;
  isReqFromNginx: boolean;
  reqPath: string;
}
