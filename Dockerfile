# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM rust:1.85-bookworm AS builder

# Install protobuf compiler required by tonic-build / prost-build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests and build inputs
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src

# Build release binary and strip debug symbols
RUN cargo build --release --bin mso && \
    strip target/release/mso

# -----------------------------------------------------------------------------
# Stage 2: Minimal Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runner

# Install CA certificates for TLS connections to Binance/Hyperliquid/Jupiter & curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create dedicated non-root application user and persistent data directory
RUN groupadd -g 1000 mso && \
    useradd -u 1000 -g mso -m -s /bin/bash mso && \
    mkdir -p /data && \
    chown -R mso:mso /data

USER mso
WORKDIR /data

# Copy binary from builder
COPY --from=builder --chown=mso:mso /app/target/release/mso /usr/local/bin/mso

# Default environment configuration
ENV MSO_HTTP_ADDR=0.0.0.0:4000 \
    MSO_GRPC_ADDR=0.0.0.0:50051 \
    MSO_DB_PATH=/data/oracle.db \
    RUST_LOG=info

# Expose REST Gateway (4000) and gRPC Server (50051)
EXPOSE 4000 50051

VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4000/health || exit 1

ENTRYPOINT ["mso"]
CMD ["serve"]
