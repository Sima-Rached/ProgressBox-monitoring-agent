# ── Build stage
# Also built from the workspace root (context: .) — same reasoning as
# agent/Dockerfile: cargo needs every workspace member's manifest present
# even when only building one of them with `-p`.
FROM rust:latest AS builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p gateway

# ── Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/gateway ./gateway

# External-facing — this is the port that gets published to the host.
EXPOSE 3000
CMD ["./gateway"]
