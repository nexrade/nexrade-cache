# syntax=docker/dockerfile:1

# ─── Base: cargo-chef on Rust + musl alpine ───────────────────────────────────
#
# cargo-chef pre-cooks all dependencies in a separate, cacheable layer.
# Subsequent builds only recompile crates that actually changed.
FROM lukemathwalker/cargo-chef:latest-rust-1-alpine AS chef

RUN apk add --no-cache \
    musl-dev \
    gcc \
    make \
    cmake \
    pkgconfig \
    openssl-dev \
    openssl-libs-static

WORKDIR /src

# ─── Stage 1: Compute the dependency recipe ───────────────────────────────────
FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ─── Stage 2: Build dependencies only (cached layer) ──────────────────────────
FROM chef AS builder

COPY --from=planner /src/recipe.json recipe.json

# This layer is only invalidated when Cargo.toml / Cargo.lock change,
# not when application source files change.
RUN cargo chef cook --release --recipe-path recipe.json

# ─── Stage 3: Build application ───────────────────────────────────────────────
COPY . .
RUN cargo build --release --package nexrade-cache

# Strip debug symbols (belt-and-suspenders: profile already sets strip=true)
RUN strip target/release/nexrade-cache \
          target/release/nexrade-cli

# ─── Stage 4: Minimal scratch image ───────────────────────────────────────────
FROM scratch

COPY --from=builder /src/target/release/nexrade-cache /usr/local/bin/nexrade-cache
COPY --from=builder /src/target/release/nexrade-cli   /usr/local/bin/nexrade-cli

# Default config
COPY nexrade.example.toml /etc/nexrade/nexrade.toml

# Redis-compatible default port + Prometheus metrics
EXPOSE 6379 9091

# Volume for persistence (RDB / AOF)
VOLUME ["/data"]

WORKDIR /data

ENTRYPOINT ["/usr/local/bin/nexrade-cache"]
CMD ["--config", "/etc/nexrade/nexrade.toml"]
