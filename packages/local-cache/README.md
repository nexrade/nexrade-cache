# @nexrade/local-cache

A fast, Redis-compatible cache that runs locally in the browser, PWA, or edge runtime. It uses the Nexrade Rust storage engine compiled to WebAssembly, so reads and writes do not require a server round trip.

## Install

```sh
npm install @nexrade/local-cache
```

## Usage

```ts
import { createCache } from "@nexrade/local-cache";

const cache = await createCache({
  namespace: "my-app:",
  persistence: "indexeddb",
});

await cache.setJson("user:123", { name: "Ada" }, { ttl: 3600 });
const user = await cache.getJson<{ name: string }>("user:123");

console.log(user); // { name: "Ada" }
console.log(await cache.ttl("user:123"));
```

## API

- `get(key)` and `getBytes(key)`
- `set(key, value, { ttl, ttlMs, nx, xx })`
- `getJson(key)` and `setJson(key, value)`
- `delete(key)`, `exists(key)`, `expire(key, seconds)`, and `ttl(key)`
- `incr(key, amount)` and `decr(key, amount)`
- `keys(pattern)`, `size()`, `stats()`, and `clear()`
- `command(...args)` for binary-safe Redis-compatible commands
- `persist()` and `clearPersisted()` when IndexedDB persistence is enabled

Command arguments are passed as separate values, not interpolated into a command string:

```ts
await cache.command("HSET", "profile", "name", "Ada");
const profile = await cache.command("HGETALL", "profile");
```

`Uint8Array` values are preserved as binary data. Redis errors reject the returned promise.

## Persistence

Memory mode is the default and is the fastest option. IndexedDB mode stores a namespace snapshot in the browser:

```ts
const cache = await createCache({
  persistence: "indexeddb",
  databaseName: "my-product-cache",
  autoPersist: true,
});

await cache.persist(); // explicit checkpoint; autoPersist also checkpoints writes
```

Persistence uses `DUMP`, `RESTORE`, and remaining TTL values. It is intended for local cache and offline-first data, not as a substitute for a durable server database. Browser storage can be evicted by the user or browser, and each browser context has its own local store.

## Build from the repository

The package build requires Rust, the `wasm32-unknown-unknown` target, `wasm-pack`, and Node.js:

```sh
npm run build
```

The generated `wasm/` and `dist/` directories are build artifacts and are not committed to the repository.
