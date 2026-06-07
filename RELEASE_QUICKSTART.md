# Monthly PyPI Release Plan - Quick Reference

## Overview

Mooncake now has automated monthly releases to PyPI with support for critical hotfixes.

## Release Types

### 1. Monthly Releases (Automated)
- **When**: 1st of every month at 00:00 UTC
- **Version**: Minor bump (e.g., 0.3.9 → 0.4.0)
- **Trigger**: Automatic via cron or manual via GitHub Actions
- **Workflow**: `.github/workflows/monthly-release.yaml`

### 2. Hotfix Releases (Manual)
- **When**: Critical bugs need immediate fix
- **Version**: Post release (e.g., 0.4.0.post1)
- **Trigger**: Manual via GitHub Actions
- **Workflow**: `.github/workflows/hotfix-release.yaml`

### 3. Manual Releases (Existing)
- **When**: Ad-hoc releases needed
- **Version**: Any valid version
- **Trigger**: Push git tag (e.g., `v0.4.0`)
- **Workflows**: `release.yaml`, `release-non-cuda.yaml`, `release-cuda13.yaml`

## Quick Start

### Trigger Monthly Release Manually
1. Go to GitHub Actions → "Monthly Release"
2. Click "Run workflow"
3. Select version type (major/minor/patch)
4. Click "Run workflow"

### Create Hotfix Release
1. Go to GitHub Actions → "Hotfix Release (Post Version)"
2. Click "Run workflow"
3. Enter:
   - Base version: `0.4.0`
   - Post number: `1`
   - Description: "Fix critical memory leak"
4. Click "Run workflow"

### Use Release Manager Script
```bash
# Show current version
python scripts/release_manager.py current

# Preview version bump
python scripts/release_manager.py bump minor --dry-run

# Generate changelog
python scripts/release_manager.py changelog --since v0.3.9

# Verify PyPI package
python scripts/release_manager.py verify mooncake-transfer-engine 0.3.9
```

## Version Format (PEP 440)

- **Standard**: `X.Y.Z` (e.g., `0.4.0`)
- **Post/Hotfix**: `X.Y.Z.postN` (e.g., `0.4.0.post1`)

## PyPI Packages

- `mooncake-transfer-engine` - CUDA version (default)
- `mooncake-transfer-engine-non-cuda` - CPU-only version
- `mooncake-transfer-engine-cuda13` - CUDA 13 version

## Installation

```bash
# Install latest
pip install mooncake-transfer-engine

# Upgrade (includes post releases)
pip install --upgrade mooncake-transfer-engine

# Specific version
pip install mooncake-transfer-engine==0.4.0
```

## Required Secrets

- `PYPI_API_TOKEN` - Must be configured in GitHub repository secrets

## Documentation

See `docs/RELEASE_PLAN.md` for complete documentation.
