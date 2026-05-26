# ── Builder ──────────────────────────────────────────────────────────
FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* build.rs ./
COPY proto/ proto/
COPY src/ src/

# Warm build cache with dependencies (dummy main, then real build)
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

COPY src/ src/
RUN cargo build --release

# ── Runner ───────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rave-bebop-stream-relay /usr/local/bin/relay

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/relay"]
