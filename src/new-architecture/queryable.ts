import { Source } from "@watchmanlab/stream";

export interface Queryable<INPUT extends any[], OUTPUT> {
  has(...keys: Partial<INPUT>): boolean;
  get(...keys: Partial<INPUT>): OUTPUT | undefined;
  eq(...keys: Partial<INPUT>): Source<OUTPUT>;
  lt(...keys: Partial<INPUT>): Source<OUTPUT>;
  lte(...keys: Partial<INPUT>): Source<OUTPUT>;
  gt(...keys: Partial<INPUT>): Source<OUTPUT>;
  gte(...keys: Partial<INPUT>): Source<OUTPUT>;
  between(start: Partial<INPUT>, end: Partial<INPUT>): Source<OUTPUT>;
}
