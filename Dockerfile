# Build stage - use Alpine for minimal image size
FROM rust:1.87-alpine AS builder

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
    cyrus-sasl-dev \
    perl \
    linux-headers

# Create app directory
WORKDIR /usr/src/pgmq-relay
COPY . .

# Build the application with release optimizations
# Target musl for static linking
ENV OPENSSL_STATIC=1
RUN cargo build --release

# Create a new stage with a minimal image
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libsasl \
    libgcc

# Copy the build artifact from the builder stage
COPY --from=builder /usr/src/pgmq-relay/target/release/pgmq-relay /usr/local/bin/pgmq-relay

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
    CMD wget -q --spider http://localhost:9090/health || exit 1

# Run the application
CMD ["pgmq-relay", "--config", "/etc/pgmq-relay/config.toml"]
