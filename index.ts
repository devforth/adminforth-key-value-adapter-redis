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

  }
