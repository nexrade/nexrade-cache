# Nexrade LocalCache browser example

This example demonstrates the high-level `@nexrade/local-cache` API. Build the package first, then serve this directory from a static HTTP server.

```sh
cd packages/local-cache
npm install
npm run build
```

The package is intentionally kept separate from the existing raw WASM demo so both the low-level Redis-compatible API and the ergonomic local-cache API remain visible.
