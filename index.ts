import type { KeyValueAdapter } from "adminforth";
import { createClient } from 'redis';
import { AdapterOptions } from "./types.js";

export default class RAMKeyValueAdapter implements KeyValueAdapter {
  private data: Map<string, any>;

  private memory: Map<string, any>;
  private redis: ReturnType<typeof createClient>;
  options: AdapterOptions;

  constructor(options: AdapterOptions) {
    this.memory = new Map();
    this.redis = createClient({ url: options.redisUrl });

    this.redis.on('error', (err) => console.error('Redis error', err));

    this.redis.connect();
    this.options = options;
  }

  validate() {

  }

 async set(key, value, expiresInSeconds?) {
    if (expiresInSeconds) {
      await this.redis.set(key, value, { EX: expiresInSeconds });
    } else {
      await this.redis.set(key, value);
    }
  }

  async get(key) {
    if (this.memory.has(key)) {
      return this.memory.get(key);
    }

    const redisValue = await this.redis.get(key);
    if (redisValue) {
      const parsed = JSON.parse(redisValue);
      this.memory.set(key, parsed);
      return parsed;
    }

    return null;
  }

  async delete(key) {
    this.memory.delete(key);
    await this.redis.del(key);
  }

  }
