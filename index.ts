import type { KeyValueAdapter } from "adminforth";
import { createClient } from 'redis';
import { afLogger } from "adminforth";
import { AdapterOptions } from "./types.js";

// Reserved namespace for the adapter's own bookkeeping keys (lex indexes and
// backfill markers). These must never collide with user data and are excluded
// from any backfill scan.
const RESERVED_PREFIX = '__afkv:';

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
    if (typeof limit === 'number' && limit <= 0) {
      return [];
    }
    const indexKey = this.indexKey(collection);
    const { min, max } = this.lexPrefixRange(prefix);

    // ZRANGEBYLEX returns members already sorted ASC, so ordering is
    // deterministic (e.g. ISO date keys come out in date order) and the limit
    // is applied server-side.
    const rawMembers = await this.redis.zRangeByLex(
      indexKey,
      min,
      max,
      typeof limit === 'number' ? { LIMIT: { offset: 0, count: limit } } : undefined,
    );

    if (!rawMembers.length) {
      return [];
    }

    const members = rawMembers.map((member) => typeof member === 'string' ? member : member.toString());

    const actualKeys = members.map((member) => this.getActualKey(member, collection));
    const values = await this.redis.mGet(actualKeys);

    const staleMembers: string[] = [];
    const result = members.reduce<Record<string, string>[]>((acc, member, index) => {
      const value = values[index];

      if (value === null) {
        // The data key expired (TTL) but its index entry lingered; clean it up.
        staleMembers.push(member);
        return acc;
      }

      acc.push({ [member]: typeof value === 'string' ? value : value.toString() });
      return acc;
    }, []);

    if (staleMembers.length) {
      // Best-effort lazy cleanup; failures here must not affect the response.
      this.redis.zRem(indexKey, staleMembers).catch((err) => afLogger.error(`Failed to prune stale index members: ${err}`));
    }

    return result;
  }

}
