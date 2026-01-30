import type { KeyValueAdapter } from "adminforth";

export default class RAMKeyValueAdapter implements KeyValueAdapter {
  private data: Map<string, any>;

  constructor() {
    this.data = new Map();
  }

  validate() {

  }

  async get(key: string): Promise<string> {
  }

  async set(key: string, value: any, expiresInSeconds?: number): Promise<void> {

  }

  async delete(key: string): Promise<void> {
    
  }
}