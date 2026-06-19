# Install

PGMQ Relay can be built as a container or Rust binary. The release workflow is prepared to publish images and archives when a version tag is created, but the repository currently has no published version tag.

## Build the container

Build the current checkout:

```bash
git clone https://github.com/lightsaway/pgmq-relay.git
cd pgmq-relay
docker build -t pgmq-relay:local .
```

Run the image with a configuration file mounted read-only:

```bash
docker run --rm \
  --name pgmq-relay \
  -p 9090:9090 \
  -v "$PWD/config.toml:/etc/pgmq-relay/config.toml:ro" \
  pgmq-relay:local
```

## Published releases

When a version tag is published, the release workflow creates:

- a multi-architecture GHCR image for Linux AMD64 and ARM64
- Linux and macOS release archives
- per-archive SHA-256 checksums
- build attestations

Images are published to:

```text
ghcr.io/lightsaway/pgmq-relay
```

After the first stable release:

```bash
# Convenience: newest stable release
docker pull ghcr.io/lightsaway/pgmq-relay:latest

# Production: exact immutable release
docker pull ghcr.io/lightsaway/pgmq-relay:0.1.0
```

Stable releases also publish moving major/minor tags such as `0` and `0.1`. Prerelease tags such as `0.2.0-rc.1` do not update `latest`.

Use an exact version in production. `latest`, major, and minor tags can move after a later release.

## Build from source

Building requires the Rust toolchain and native dependencies used by `librdkafka`.

```bash
git clone https://github.com/lightsaway/pgmq-relay.git
cd pgmq-relay
cargo build --release --locked
./target/release/pgmq-relay --config config.toml
```

## Runtime requirements

- PostgreSQL with PGMQ `1.11.1` compatible functions
- At least one configured destination broker
- Existing source queues and any configured dead-letter queues
- Network access from the relay to PostgreSQL and the broker
