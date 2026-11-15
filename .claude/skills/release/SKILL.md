---
name: release
description: Create a new countdb release with git tag and push to GitHub. Fetches latest version from git tags and creates next patch version by default.
disable-model-invocation: true
allowed-tools: Bash(git fetch *) Bash(git tag *) Bash(git push *) Bash(git status *) Bash(git rev-parse *)
---

# Release Version Management

Automates the release workflow for countdb.

Pushing a `v*.*.*` tag triggers the `Package and Release Sources` GitHub workflow
(`.github/workflows/release.yaml`), which packages the sources and creates a GitHub release.
This is outward-facing and hard to undo, so only run it when the user asks for a release.

## Usage

```
/release
```

## What it does

1. **Checks the repo is releasable**: clean working tree, and `HEAD` equal to `origin/main` (works from `dev` right after it was fast-forwarded to `main`)
2. **Fetches tags** from `origin`, so the local tag list is current
3. **Calculates next version** by incrementing the patch of the latest `vX.Y.Z` tag (e.g. v0.0.8 → v0.0.9)
4. **Shows the version and asks for confirmation** before tagging
5. **Creates an annotated git tag** and **pushes it** to GitHub

## Implementation

```bash
set -e

# Release only what is on origin/main
git fetch origin main --tags --quiet
if [ -n "$(git status --porcelain)" ]; then
    echo "Working tree is not clean - aborting"; exit 1
fi
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
    echo "HEAD differs from origin/main - push or pull first"; exit 1
fi

# Latest version tag (only vX.Y.Z tags)
LATEST_TAG=$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' | sort -V | tail -1)

if [ -z "$LATEST_TAG" ]; then
    echo "No tags found. Creating first release v0.0.1"
    NEW_VERSION="0.0.1"
else
    # Remove 'v' prefix and extract version parts
    VERSION=${LATEST_TAG#v}
    IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"

    # Increment patch version
    PATCH=$((PATCH + 1))
    NEW_VERSION="$MAJOR.$MINOR.$PATCH"
fi

echo "Latest: ${LATEST_TAG:-none}. New release: v$NEW_VERSION ($(git rev-parse --short HEAD))"
```

Confirm the version with the user, and only then run:

```bash
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"
git push origin "v$NEW_VERSION"
```
