# ── Build stage
# Built from the *workspace root* now (context: .) since the agent depends
# on the `common` crate via a workspace path dependency.
FROM rust:latest AS builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p agent

# ── Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/agent ./agent
COPY config.toml ./config/config.toml
COPY rules.toml ./config/rules.toml

# Internal only — not published to the host anymore, the gateway is the
# external entry point. Kept EXPOSE for documentation / inter-container use.
EXPOSE 3000
CMD ["./agent"]
