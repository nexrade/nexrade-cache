import type { IDBPDatabase } from "./internal-types.js";

export type CacheValue = string | Uint8Array;
export type PersistenceMode = "memory" | "indexeddb";

export interface SetOptions {
  /** Expire after this many seconds. */
  ttl?: number;
  /** Expire after this many milliseconds. */
  ttlMs?: number;
  /** Only set when the key does not already exist. */
  nx?: boolean;
  /** Only set when the key already exists. */
  xx?: boolean;
}

export interface LocalCacheOptions {
  /** Prefix all high-level keys to isolate this cache instance. */
  namespace?: string;
  /** Memory is volatile; IndexedDB restores entries across page reloads. */
  persistence?: PersistenceMode;
  /** IndexedDB database name. Defaults to nexrade-local-cache. */
  databaseName?: string;
  /** Persist writes automatically when IndexedDB is enabled. */
  autoPersist?: boolean;
  /** Debounce delay for automatic persistence. Defaults to 250 ms. */
  persistDebounceMs?: number;
}

export interface CacheStats {
  keys: number;
}

interface WasmStore {
  command(args: unknown[]): Promise<unknown>;
  execute(command: string): Promise<string>;
  dbsize(): number;
  flushall(): void;
}

interface WasmModule {
  default(): Promise<unknown>;
  NexradeWasm: new () => WasmStore;
}

type SnapshotEntry = {
  key: number[];
  ttl: number;
  dump: number[];
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Create a local Redis-compatible cache backed by nexrade-wasm.
 *
 * The asynchronous factory loads the WebAssembly module and, when enabled,
 * restores the IndexedDB snapshot before returning a ready cache.
 */
export async function createCache(options: LocalCacheOptions = {}): Promise<LocalCache> {
  const cache = new LocalCache(options);
  await cache.ready();
  return cache;
}

export class LocalCache {
  private readonly namespace: string;
  private readonly persistence: PersistenceMode;
  private readonly databaseName: string;
  private readonly autoPersist: boolean;
  private readonly persistDebounceMs: number;
  private readonly readyPromise: Promise<void>;
  private store: WasmStore | undefined;
  private persistTimer: ReturnType<typeof setTimeout> | undefined;
  private persistenceDatabase: IDBPDatabase | undefined;
  private restoring = false;
  private persisting = false;

  public constructor(options: LocalCacheOptions = {}) {
    this.namespace = options.namespace ?? "";
    this.persistence = options.persistence ?? "memory";
    this.databaseName = options.databaseName ?? "nexrade-local-cache";
    this.autoPersist = options.autoPersist ?? true;
    this.persistDebounceMs = options.persistDebounceMs ?? 250;
    this.readyPromise = this.initialize();
  }

  /** Resolves once WASM has loaded and the persisted snapshot is restored. */
  public async ready(): Promise<void> {
    await this.readyPromise;
  }

  /** Run a raw command with binary-safe arguments. Raw commands are unscoped. */
  public async command(...args: Array<string | number | Uint8Array>): Promise<unknown> {
    await this.ready();
    const result = await this.executeRaw(args);
    if (isMutationCommand(args[0])) this.schedulePersistence();
    return result;
  }

  public async getBytes(key: string): Promise<Uint8Array | null> {
    const result = await this.highLevelCommand(["GET", this.wireKey(key)]);
    return result == null ? null : asBytes(result);
  }

  public async get(key: string): Promise<string | null> {
    const value = await this.getBytes(key);
    return value == null ? null : textDecoder.decode(value);
  }

  public async getJson<T = unknown>(key: string): Promise<T | null> {
    const value = await this.get(key);
    return value == null ? null : (JSON.parse(value) as T);
  }

  public async set(key: string, value: CacheValue, options: SetOptions = {}): Promise<boolean> {
    const args: Array<string | number | Uint8Array> = ["SET", this.wireKey(key), value];
    this.appendSetOptions(args, options);
    const result = await this.highLevelCommand(args);
    return result !== null && result !== false;
  }

  public async setJson(key: string, value: unknown, options: SetOptions = {}): Promise<boolean> {
    return this.set(key, JSON.stringify(value), options);
  }

  public async delete(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    const result = await this.highLevelCommand(["DEL", ...keys.map((key) => this.wireKey(key))]);
    return asNumber(result);
  }

  public async exists(key: string): Promise<boolean> {
    const result = await this.highLevelCommand(["EXISTS", this.wireKey(key)]);
    return asNumber(result) > 0;
  }

  public async has(key: string): Promise<boolean> {
    return this.exists(key);
  }

  public async expire(key: string, seconds: number): Promise<boolean> {
    const result = await this.highLevelCommand(["EXPIRE", this.wireKey(key), seconds]);
    return asNumber(result) > 0;
  }

  public async ttl(key: string): Promise<number> {
    const result = await this.highLevelCommand(["TTL", this.wireKey(key)]);
    return asNumber(result);
  }

  public async incr(key: string, amount = 1): Promise<number> {
    const command = amount === 1 ? "INCR" : "INCRBY";
    return asNumber(await this.highLevelCommand([command, this.wireKey(key), amount]));
  }

  public async decr(key: string, amount = 1): Promise<number> {
    const command = amount === 1 ? "DECR" : "DECRBY";
    return asNumber(await this.highLevelCommand([command, this.wireKey(key), amount]));
  }

  public async keys(pattern = "*"): Promise<string[]> {
    const output: string[] = [];
    const match = `${escapeGlob(this.namespace)}${pattern}`;
    let cursor = "0";

    do {
      const result = await this.highLevelCommand(["SCAN", cursor, "MATCH", match, "COUNT", 1000]);
      if (!Array.isArray(result) || result.length < 2 || !Array.isArray(result[1])) return output;
      output.push(
        ...result[1].map((key) => this.unwireKey(textDecoder.decode(asBytes(key)))),
      );
      cursor = textDecoder.decode(asBytes(result[0]));
    } while (cursor !== "0");

    return output;
  }

  public async size(): Promise<number> {
    if (this.namespace === "") {
      await this.ready();
      return this.store?.dbsize() ?? 0;
    }
    return (await this.keys()).length;
  }

  public async stats(): Promise<CacheStats> {
    return { keys: await this.size() };
  }

  /** Delete all keys in this cache namespace without affecting other users. */
  public async clear(): Promise<void> {
    const keys = await this.keys();
    if (keys.length > 0) await this.delete(...keys);
    if (this.persistence === "indexeddb") await this.clearPersisted();
  }

  /** Persist the current namespace to IndexedDB. No-op in memory mode. */
  public async persist(): Promise<void> {
    await this.ready();
    if (this.persistence !== "indexeddb") return;
    if (this.persisting) return;
    this.persisting = true;
    try {
      const database = await this.openPersistenceDatabase();
      const snapshot: SnapshotEntry[] = [];
      const keys = await this.keys();

      for (const key of keys) {
        const wireKey = this.wireKey(key);
        const dump = await this.executeRaw(["DUMP", wireKey]);
        if (dump == null) continue;
        snapshot.push({
          key: Array.from(textEncoder.encode(key)),
          ttl: asNumber(await this.executeRaw(["PTTL", wireKey])),
          dump: Array.from(asBytes(dump)),
        });
      }

      await idbPut(database, this.namespace, snapshot);
    } finally {
      this.persisting = false;
    }
  }

  /** Remove this namespace's persisted snapshot. */
  public async clearPersisted(): Promise<void> {
    if (this.persistence !== "indexeddb") return;
    const database = await this.openPersistenceDatabase();
    await idbDelete(database, this.namespace);
  }

  private async initialize(): Promise<void> {
    // The wasm-pack module is generated by `npm run build:wasm` and is not
    // present in a source checkout until that step has run.
    const wasm = (await import("../wasm/nexrade_wasm.js")) as unknown as WasmModule;
    await wasm.default();
    this.store = new wasm.NexradeWasm();
    if (this.persistence === "indexeddb") await this.restore();
  }

  private async restore(): Promise<void> {
    this.restoring = true;
    try {
      const database = await this.openPersistenceDatabase();
      const snapshot = (await idbGet(database, this.namespace)) ?? [];
      for (const entry of snapshot) {
        const key = textDecoder.decode(new Uint8Array(entry.key));
        await this.executeRaw(["RESTORE", this.wireKey(key), Math.max(0, entry.ttl), new Uint8Array(entry.dump), "REPLACE"]);
      }
    } finally {
      this.restoring = false;
    }
  }

  private async openPersistenceDatabase(): Promise<IDBPDatabase> {
    if (this.persistenceDatabase) return this.persistenceDatabase;
    if (typeof indexedDB === "undefined") {
      throw new Error("IndexedDB is not available; use persistence: 'memory' instead");
    }
    this.persistenceDatabase = await openDatabase(this.databaseName);
    return this.persistenceDatabase;
  }

  private async highLevelCommand(args: Array<string | number | Uint8Array>): Promise<unknown> {
    await this.ready();
    const result = await this.executeRaw(args);
    if (isMutationCommand(args[0])) this.schedulePersistence();
    return result;
  }

  private async executeRaw(args: Array<string | number | Uint8Array>): Promise<unknown> {
    if (!this.store) throw new Error("cache is not initialized");
    return this.store.command(args);
  }

  private wireKey(key: string): string {
    return `${this.namespace}${key}`;
  }

  private unwireKey(key: string): string {
    return key.startsWith(this.namespace) ? key.slice(this.namespace.length) : key;
  }

  private appendSetOptions(args: Array<string | number | Uint8Array>, options: SetOptions): void {
    if (options.ttlMs != null) {
      if (!Number.isInteger(options.ttlMs) || options.ttlMs <= 0) throw new Error("ttlMs must be a positive integer");
      args.push("PX", options.ttlMs);
    } else if (options.ttl != null) {
      if (!Number.isInteger(options.ttl) || options.ttl <= 0) throw new Error("ttl must be a positive integer");
      args.push("EX", options.ttl);
    }
    if (options.nx && options.xx) throw new Error("nx and xx cannot be used together");
    if (options.nx) args.push("NX");
    if (options.xx) args.push("XX");
  }

  private schedulePersistence(): void {
    if (!this.autoPersist || this.persistence !== "indexeddb" || this.restoring) return;
    if (this.persistTimer) clearTimeout(this.persistTimer);
    this.persistTimer = setTimeout(() => {
      this.persistTimer = undefined;
      void this.persist();
    }, this.persistDebounceMs);
  }
}

function asNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number(value);
  throw new Error("expected a numeric response");
}

function isMutationCommand(command: string | number | Uint8Array | undefined): boolean {
  if (typeof command !== "string") return false;
  return new Set([
    "APPEND", "DECR", "DECRBY", "DEL", "EXPIRE", "EXPIREAT", "HDEL", "HINCRBY",
    "HINCRBYFLOAT", "HSET", "INCR", "INCRBY", "LPUSH", "LPOP", "LTRIM", "PERSIST",
    "PSETEX", "RENAME", "RESTORE", "RPUSH", "SADD", "SET", "SETEX", "SETNX", "SREM",
    "ZADD", "ZINCRBY", "ZREM", "FLUSHALL", "FLUSHDB",
  ]).has(command.toUpperCase());
}

function asBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string") return textEncoder.encode(value);
  throw new Error("expected a bulk-string response");
}

function escapeGlob(value: string): string {
  return value.replace(/[\\*?\[\]]/g, "\\$&");
}

async function openDatabase(name: string): Promise<IDBPDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("snapshots");
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("failed to open IndexedDB"));
  });
}

function idbGet(database: IDBPDatabase, key: string): Promise<SnapshotEntry[] | undefined> {
  return new Promise((resolve, reject) => {
    const request = database.transaction("snapshots", "readonly").objectStore("snapshots").get(key);
    request.onsuccess = () => resolve(request.result as SnapshotEntry[] | undefined);
    request.onerror = () => reject(request.error ?? new Error("failed to read cache snapshot"));
  });
}

function idbPut(database: IDBPDatabase, key: string, value: SnapshotEntry[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = database.transaction("snapshots", "readwrite").objectStore("snapshots").put(value, key);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error ?? new Error("failed to write cache snapshot"));
  });
}

function idbDelete(database: IDBPDatabase, key: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = database.transaction("snapshots", "readwrite").objectStore("snapshots").delete(key);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error ?? new Error("failed to delete cache snapshot"));
  });
}
