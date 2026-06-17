import type { KeyValueAdapter } from "adminforth";
import { createClient } from 'redis';
import { AdapterOptions } from "./types.js";

export default class RedisKeyValueAdapter implements KeyValueAdapter {
  private redis: ReturnType<typeof createClient>;
  options: AdapterOptions;

  constructor(options: AdapterOptions) {
    this.redis = createClient({ url: options.redisUrl });

    this.redis.on('error', (err) => console.error('Redis error', err));

    this.redis.connect();
    this.options = options;
  }

  validate() {

  }

 async set(key, value, expiresInSeconds?) {
    if (expiresInSeconds) {
      await this.redis.set(key, value, { expiration: {type: 'EX', value: expiresInSeconds} });
    } else {
      await this.redis.set(key, value);
    }
  }

  async get(key) {

    const redisValue = await this.redis.get(key);
    if (redisValue) {
      const valueStr = typeof redisValue === 'string' ? redisValue : redisValue.toString();
      return valueStr;
    }

    return null;
  }

  async delete(key) {
    await this.redis.del(key);
  }

  async listByPrefix(prefix: string, limit: number): Promise<Record<string, string>> {
    if (limit <= 0) {
      return {};
    }

    const keys: string[] = [];

    for await (const key of this.redis.scanIterator({ MATCH: `${prefix}*`, COUNT: limit })) {
      keys.push(typeof key === 'string' ? key : key.toString());

      if (keys.length >= limit) {
        break;
      }
    }

    if (!keys.length) {
      return {};
    }

    const values = await this.redis.mGet(keys);

    return keys.reduce<Record<string, string>>((result, key, index) => {
      const value = values[index];

      if (value !== null) {
        result[key] = typeof value === 'string' ? value : value.toString();
      }

      return result;
    }, {});
  }

  }
