# Build and Contribute

## Code quality

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

Native Kafka build dependencies are required because `rdkafka` builds `librdkafka`.

## Documentation

The mdBook source is `site/src`. The generated `book/` directory is build output.

Install the pinned tools:

```bash
cargo install mdbook --version 0.5.3 --locked
cargo install lychee --version 0.24.2 --locked
```

Build and check:

```bash
mdbook build
lychee --offline --no-progress --exclude-path 'book/404.html' book
```

Do not add Mermaid blocks. The published site does not depend on a Mermaid JavaScript runtime; use semantic HTML/CSS diagrams, tables, or committed image assets.

## Documentation ownership

The mdBook is the detailed source of truth. Keep the repository README short and link to the relevant book pages instead of duplicating full references.

## Releases

A version tag matching `Cargo.toml` triggers release binaries and a multi-architecture GHCR image:

```bash
git tag v0.1.0
git push origin v0.1.0
```
