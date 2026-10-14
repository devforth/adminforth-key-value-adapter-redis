import type { KeyValueAdapter } from "adminforth";
import { createClient } from 'redis';
import { afLogger } from "adminforth";
import { AdapterOptions } from "./types.js";

// Reserved namespace for the adapter's own bookkeeping keys (lex indexes and
// backfill markers). These must never collide with user data and are excluded
// from any backfill scan.
const RESERVED_PREFIX = '__afkv:';
const LIST_PAGE_SIZE = 500;

export default class RedisKeyValueAdapter implements KeyValueAdapter {
  private redis: ReturnType<typeof createClient>;
  options: AdapterOptions;

  constructor(options: AdapterOptions) {
    this.redis = createClient({ url: options.redisUrl });

    this.redis.on('error', (err) => afLogger.error(`Redis error: ${err}`));

    this.redis.connect();
    this.options = options;
  }

  validate() {

  }

  protected getActualKey(key: string, collection?: string): string {
    if (collection) {
      return `${collection}:${key}`;
    }
    return key;
  }

  private indexKey(collection?: string): string {
    return collection ? `${RESERVED_PREFIX}idx:${collection}` : `${RESERVED_PREFIX}idx`;
  }

  private lexPrefixRange(prefix: string): { min: string; max: string } {
    if (prefix === '') {
      return { min: '-', max: '+' };
    }
    const lastCode = prefix.charCodeAt(prefix.length - 1);
    const upper = `${prefix.slice(0, -1)}${String.fromCharCode(lastCode + 1)}`;
    // `[` = inclusive, `(` = exclusive.
    return { min: `[${prefix}`, max: `(${upper}` };
  }

  async set(key, value, expiresInSeconds?, collection?: string) {
    const actualKey = this.getActualKey(key, collection);
    const indexKey = this.indexKey(collection);

    const multi = this.redis.multi();
    if (expiresInSeconds) {
      multi.set(actualKey, value, { expiration: { type: 'EX', value: expiresInSeconds } });
    } else {
      multi.set(actualKey, value);
    }
    // Keep the lex index in sync with the write.
    multi.zAdd(indexKey, { score: 0, value: key });
    await multi.exec();
  }

  async get(key, collection?: string) {
    const actualKey = this.getActualKey(key, collection);
    const redisValue = await this.redis.get(actualKey);
    if (redisValue) {
      const valueStr = typeof redisValue === 'string' ? redisValue : redisValue.toString();
      return valueStr;
    }

    return null;
  }

  async delete(key, collection?: string) {
    const actualKey = this.getActualKey(key, collection);
    const indexKey = this.indexKey(collection);

    await this.redis.multi()
      .del(actualKey)
      .zRem(indexKey, key)
      .exec();
  }

  async listByPrefix(prefix: string, limit?: number, collection?: string): Promise<Record<string, string>[]> {
    // rows are whole: COUNT is an error unless it is an integer, and COUNT 0 returns nothing silently
    const rowLimit = limit === undefined ? Infinity : Math.floor(limit);
    if (rowLimit < 1) {
      return [];
    }
    const indexKey = this.indexKey(collection);
    const { min: firstMin, max } = this.lexPrefixRange(prefix);
    let min = firstMin;
    let cursor: string | undefined;
    let pageSize = Math.min(rowLimit, LIST_PAGE_SIZE);
    const result: Record<string, string>[] = [];

    // Expired data keys leave index members behind. Continue until enough live
    // rows are found or the lex range is exhausted: a short answer would be
    // indistinguishable from an exhausted prefix for the caller.
    while (result.length < rowLimit) {
      const rawMembers = await this.redis.zRangeByLex(
        indexKey,
        min,
        max,
        { LIMIT: { offset: 0, count: pageSize } },
      );
      if (!rawMembers.length) break;

      const members = rawMembers.map((member) => typeof member === 'string' ? member : member.toString());
      const last = members[members.length - 1];
      // ZRANGEBYLEX only orders members of equal score, and a member whose bytes we cannot reproduce
      // is never excluded by the cursor built from it: both repeat the page, so stop instead of spinning
      if (cursor !== undefined && Buffer.compare(Buffer.from(last), Buffer.from(cursor)) <= 0) {
        break;
      }
      cursor = last;
      const values = await this.redis.mGet(members.map((member) => this.getActualKey(member, collection)));
      const staleMembers: string[] = [];
      for (let index = 0; index < members.length; index++) {
        const value = values[index];
        if (value === null) {
          staleMembers.push(members[index]);
        } else if (result.length < rowLimit) {
          result.push({ [members[index]]: typeof value === 'string' ? value : value.toString() });
        }
      }

      // An exclusive lex cursor remains valid after stale members are removed.
      min = `(${last}`;
      if (staleMembers.length) {
        // awaited so the prune is done when the call resolves and the next one is cheap; a failure is logged, not raised
        await this.redis.zRem(indexKey, staleMembers).catch((err) => afLogger.error(`Failed to prune stale index members: ${err}`));
      }
      if (members.length < pageSize) break;
      pageSize = LIST_PAGE_SIZE;
    }

    return result;
  }

}
