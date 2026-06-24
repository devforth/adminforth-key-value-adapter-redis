import type { KeyValueAdapter } from "adminforth";
import { createClient } from 'redis';
import { afLogger } from "adminforth";
import { AdapterOptions } from "./types.js";

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

 async set(key, value, expiresInSeconds?, collection?: string) {
    const actualKey = this.getActualKey(key, collection);
    if (expiresInSeconds) {
      await this.redis.set(actualKey, value, { expiration: {type: 'EX', value: expiresInSeconds} });
    } else {
      await this.redis.set(actualKey, value);
    }
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
    await this.redis.del(actualKey);
  }

  async listByPrefix(prefix: string, limit?: number, collection?: string): Promise<Record<string, string>[]> {
    afLogger.warn('listByPrefix is not optimized for large datasets for Redis. Will be optimized in future versions. Consider using a different adapter for large datasets.');
    if (typeof limit === 'number' && limit <= 0) {
      return [];
    }

    const actualPrefix = this.getActualKey(prefix, collection);
    const keys: string[] = [];

    for await (const scanChunk of this.redis.scanIterator({ MATCH: `${actualPrefix}*`, COUNT: 100 })) {
      const chunk = Array.isArray(scanChunk) ? scanChunk : [scanChunk];

      for (const key of chunk) {
        keys.push(typeof key === 'string' ? key : key.toString());
      }
    }

    if (!keys.length) {
      return [];
    }

    // Redis SCAN returns keys in arbitrary order, so sort ASC to guarantee
    // deterministic ordering (e.g. ISO date keys come out in date order)
    // before applying the limit.
    keys.sort();

    const limitedKeys = typeof limit === 'number' ? keys.slice(0, limit) : keys;

    const values = await this.redis.mGet(limitedKeys);

    return limitedKeys.reduce<Record<string, string>[]>((result, key, index) => {
      const value = values[index];

      if (value === null) {
        return result;
      }

      let resultKey = key;
      if (collection) {
        // only keep keys that belong to the requested collection
        if (!resultKey.startsWith(`${collection}:`)) {
          return result;
        }
        // return keys without the collection prefix
        resultKey = resultKey.replace(`${collection}:`, '');
      }

      // return the key only if it starts with the requested prefix
      if (!resultKey.startsWith(prefix)) {
        return result;
      }

      result.push({ [resultKey]: typeof value === 'string' ? value : value.toString() });

      return result;
    }, []);
  }

}
