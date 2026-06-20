#!/bin/sh

set -eu

cd "$(git rev-parse --show-toplevel)"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Working tree has tracked changes; commit or stash them before releasing" >&2
    exit 1
fi

current_version=$(
    awk '
        /^\[package\]$/ { in_package = 1; next }
        in_package && /^\[/ { exit }
        in_package && /^version = "/ {
            value = $0
            sub(/^version = "/, "", value)
            sub(/"$/, "", value)
            print value
            exit
        }
    ' Cargo.toml
)

if [ -z "$current_version" ]; then
    echo "Could not read the package version from Cargo.toml" >&2
    exit 1
fi

if [ -n "${VERSION:-}" ]; then
    next_version=$VERSION
else
    next_version=$(
        printf '%s\n' "$current_version" | awk -F. '
            NF == 3 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {
                printf "%d.%d.%d\n", $1, $2, $3 + 1
            }
        '
    )
    if [ -z "$next_version" ]; then
        echo "Automatic patch bumps require a stable x.y.z version; use VERSION=x.y.z" >&2
        exit 1
    fi
fi

if ! printf '%s\n' "$next_version" |
    grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z][0-9A-Za-z.-]*)?$'; then
    echo "Invalid release version: $next_version" >&2
    exit 1
fi

if [ "$next_version" = "$current_version" ]; then
    echo "Next version must differ from the current version: $current_version" >&2
    exit 1
fi

tag="v$next_version"
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "Tag already exists: $tag" >&2
    exit 1
fi

echo "Preparing $tag from v$current_version"

if [ "${DRY_RUN:-0}" = "1" ]; then
    exit 0
fi

prepared=0
cleanup() {
    if [ "$prepared" -eq 0 ]; then
        git restore -- Cargo.toml Cargo.lock
    fi
}
trap cleanup EXIT HUP INT TERM

NEXT_VERSION=$next_version perl -0pi -e \
    's/(\[package\].*?^version = ")[^"]+(")/$1$ENV{NEXT_VERSION}$2/ms' \
    Cargo.toml

cargo check
cargo check --locked
make --no-print-directory release-validate TAG="$tag"

unexpected_changes=$(
    git diff --name-only | grep -Ev '^(Cargo\.toml|Cargo\.lock)$' || true
)
if [ -n "$unexpected_changes" ]; then
    echo "Release preparation changed unexpected tracked files:" >&2
    printf '%s\n' "$unexpected_changes" >&2
    exit 1
fi

git add Cargo.toml Cargo.lock
git commit -m "prepare $tag release"
prepared=1
git tag "$tag"

echo "Created commit and tag $tag"
echo "Push both with: git push origin HEAD $tag"
