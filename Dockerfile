# syntax=docker/dockerfile:1.7

# Build stage - use Alpine for minimal image size
FROM rust:1.96-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    pkgconfig \
    musl-dev \
    cmake \
    make \
    gcc \
    g++ \
    openssl-dev \
    openssl-libs-static \
    zlib-static \
    curl-dev \
    cyrus-sasl-dev \
    perl \
    linux-headers

# Create app directory
WORKDIR /usr/src/pgmq-relay

# Target musl and link OpenSSL statically in both cached build passes.
ENV OPENSSL_STATIC=1

# Build dependencies first so source-only changes reuse the expensive dependency layer.
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/pgmq-relay/target \
    mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && printf '' > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src

# Build the application with release optimizations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/pgmq-relay/target \
    cargo clean --release -p pgmq-relay \
    && cargo build --release --locked \
    && cp target/release/pgmq-relay /tmp/pgmq-relay

# Create a new stage with a minimal image
FROM alpine:3.24

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libsasl \
    libgcc

# Copy the build artifact from the builder stage
COPY --from=builder /tmp/pgmq-relay /usr/local/bin/pgmq-relay

# Verify the binary was copied and make it executable
RUN chmod +x /usr/local/bin/pgmq-relay

# Create a non-root user to run the application
RUN adduser -D -u 1000 pgmq-relay

# Create config directory and copy config file
RUN mkdir -p /etc/pgmq-relay
COPY docker/config-docker.toml /etc/pgmq-relay/config.toml
RUN chown -R pgmq-relay:pgmq-relay /etc/pgmq-relay

USER pgmq-relay

# Set the working directory
WORKDIR /home/pgmq-relay

# Expose the port the app runs on
EXPOSE 9090

# Set environment variables
ENV RUST_LOG=info

# Health check using wget (smaller than curl)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD wget -q --spider http://127.0.0.1:9090/health || exit 1

# Run the application
CMD ["pgmq-relay", "--config", "/etc/pgmq-relay/config.toml"]
