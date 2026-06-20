# Publish a Release

The release workflow runs when a tag matching `v*` is pushed.

## Required secrets and environment variables

No custom repository secrets or environment variables are required.

The workflow uses the automatic `GITHUB_TOKEN` for:

- creating the GitHub Release
- pushing the container image to GHCR
- publishing build attestations

The necessary job permissions are declared in `.github/workflows/release.yml`.

## Repository settings

In **Settings > Actions > General > Workflow permissions**, allow GitHub Actions to create and approve pull requests only if your broader workflow needs it; this release does not.

The important requirement is that Actions can use the workflow-declared write permissions. Organization policy must not restrict:

- `contents: write`
- `packages: write`
- `attestations: write`
- `id-token: write`

After the first image is published, set the GHCR package visibility appropriate for your users. Public repositories do not automatically guarantee that every package is public.

## Release checklist

1. Update `version` in `Cargo.toml`.
2. Run the local checks:

   ```bash
   make ci-check
   make ci-audit
   make docker-build
   ```

3. Validate the intended tag:

   ```bash
   make release-validate TAG=v0.1.1
   ```

4. Commit and merge the version change.
5. Create a tag derived from the Cargo version and push it:

   ```bash
   make release-tag
   git push origin v0.1.1
   ```

Pushing a tag alone is sufficient only when the tagged commit already contains
the matching version in `Cargo.toml`. The workflow rejects mismatches because
otherwise the release tag, archive names, container tags, and compiled binary
would disagree.

If a mismatched tag was already pushed and the workflow failed, first commit
the corrected Cargo version. Then replace the failed tag:

```bash
git tag -d v0.1.1
git push origin :refs/tags/v0.1.1
make release-tag
git push origin v0.1.1
```

Do not move a tag after a successful release has published artifacts.

## Published artifacts

The workflow creates:

- Linux AMD64 and ARM64 archives
- macOS x86_64 and ARM64 archives
- SHA-256 files and a combined `SHA256SUMS`
- a multi-architecture GHCR image
- GitHub attestations
- generated GitHub release notes

The image repository is:

```text
ghcr.io/lightsaway/pgmq-relay
```

For a stable `v0.1.0` tag, image tags include:

```text
ghcr.io/lightsaway/pgmq-relay:0.1.0
ghcr.io/lightsaway/pgmq-relay:0.1
ghcr.io/lightsaway/pgmq-relay:0
ghcr.io/lightsaway/pgmq-relay:latest
```

`latest` is updated only by stable SemVer releases. A prerelease such as `v0.2.0-rc.1` publishes `0.2.0-rc.1` without replacing `latest`.

If the Docker job fails, the GitHub Release is not created because the publish job depends on every binary and image job.
