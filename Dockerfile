# =============================================================================
# Stage 1: Build Environment
# =============================================================================
FROM rust:1.85-bookworm AS builder

# Install protobuf compiler required by tonic-build / prost-build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests and sources
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src

# Build stripped release binary with optimizations
RUN cargo build --release --bin mso && \
    strip /app/target/release/mso

# Create data directory owned by distroless nonroot user (UID 65532)
RUN mkdir -p /data && chown 65532:65532 /data

# =============================================================================
# Stage 2: Ultra-Minimal Hardened Distroless Runtime
# Contains ONLY libc, libgcc, ca-certificates, and the stripped mso binary.
# No shell, no package manager, zero bloat, minimal attack surface.
# =============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot AS runner

# Copy stripped binary and data volume directory
COPY --from=builder /app/target/release/mso /usr/local/bin/mso
COPY --from=builder --chown=65532:65532 /data /data

# Run as nonroot user (UID 65532)
USER 65532:65532
WORKDIR /data

# Default production configuration
ENV MSO_HTTP_ADDR=0.0.0.0:4000 \
    MSO_GRPC_ADDR=0.0.0.0:50051 \
    MSO_DB_PATH=/data/oracle.db \
    RUST_LOG=info

# Expose REST Gateway / Web Dashboard (4000) and gRPC Server (50051)
EXPOSE 4000 50051

VOLUME ["/data"]

# Native in-binary health check (no curl or shell required)
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/mso", "health"]

ENTRYPOINT ["/usr/local/bin/mso"]
CMD ["serve"]
