# Mooncake PyPI Release Plan

This document outlines the automated release strategy for Mooncake wheel packages on PyPI.

## Release Strategy

### 1. Monthly Releases (Automated)

**Schedule**: 1st of every month at 00:00 UTC

**Version Bump**: Minor version increment (e.g., 0.3.9 → 0.4.0)

**Workflow**: `.github/workflows/monthly-release.yaml`

**Trigger**:
- Automatic via cron schedule
- Manual via GitHub Actions UI with version type selection (major/minor/patch)

**Process**:
1. Automatically bumps version in `pyproject.toml`
2. Creates commit with version bump
3. Creates and pushes git tag (e.g., `v0.4.0`)
4. Builds wheels for:
   - CUDA builds (Python 3.10, 3.11, 3.12, 3.13)
   - Non-CUDA builds (Python 3.10, 3.11, 3.12, 3.13)
5. Creates GitHub Release with auto-generated notes
6. Publishes to PyPI

**PyPI Packages**:
- `mooncake-transfer-engine` (CUDA version)
- `mooncake-transfer-engine-non-cuda` (CPU-only version)

### 2. Hotfix Releases (Post Versions)

**Purpose**: Critical bug fixes between monthly releases

**Version Format**: `X.Y.Z.postN` (e.g., `0.4.0.post1`, `0.4.0.post2`)

**Workflow**: `.github/workflows/hotfix-release.yaml`

**Trigger**: Manual via GitHub Actions UI

**Required Inputs**:
- `base_version`: Base version to hotfix (e.g., `0.4.0`)
- `post_number`: Post release number (e.g., `1` for `.post1`)
- `description`: Brief description of the fix

**Process**:
1. Validates version format and post number
2. Creates hotfix branch (`hotfix/X.Y.Z.postN`)
3. Updates version in `pyproject.toml`
4. Creates and pushes git tag (e.g., `v0.4.0.post1`)
5. Builds wheels for all Python versions (CUDA + Non-CUDA)
6. Creates GitHub Release marked as hotfix
7. Publishes to PyPI

**When to Use**:
- Security vulnerabilities
- Critical bugs affecting production users
- Data corruption issues
- Severe performance regressions

### 3. Manual Tag-Based Releases (Existing)

**Workflows**:
- `.github/workflows/release.yaml` (CUDA builds)
- `.github/workflows/release-non-cuda.yaml` (Non-CUDA builds)
- `.github/workflows/release-cuda13.yaml` (CUDA 13 builds)

**Trigger**: Push tags matching `v*` pattern

**Use Case**: Ad-hoc releases when needed

## Version Numbering (PEP 440 Compliant)

Mooncake follows [PEP 440](https://peps.python.org/pep-0440/) versioning:

### Standard Releases
- **Major**: `X.0.0` - Breaking changes, major features
- **Minor**: `X.Y.0` - New features, backward compatible
- **Patch**: `X.Y.Z` - Bug fixes, backward compatible

### Post Releases (Hotfixes)
- **Format**: `X.Y.Z.postN`
- **Example**: `0.4.0.post1`, `0.4.0.post2`
- **Purpose**: Critical fixes without changing the base release
- **PyPI Behavior**: Users on `0.4.0` will automatically get `0.4.0.post1` when upgrading

### Examples
```
0.3.9           # Current stable
0.4.0           # Next monthly release (minor bump)
0.4.0.post1     # Hotfix for 0.4.0
0.4.0.post2     # Second hotfix for 0.4.0
0.4.1           # Next patch release
0.5.0           # Following monthly release
```

## PyPI Package Structure

### Main Package: `mooncake-transfer-engine`
- Full-featured CUDA build
- Includes Transfer Engine, Store, and Expert Parallelism
- CUDA 12.8 support
- Supports Python 3.10, 3.11, 3.12, 3.13
- Platform: `manylinux_2_*_x86_64`, `manylinux_2_*_aarch64`

### Alternative Package: `mooncake-transfer-engine-non-cuda`
- CPU-only build
- No CUDA dependencies
- Lighter weight for non-GPU environments
- Same Python version support

### CUDA 13 Package: `mooncake-transfer-engine-cuda13`
- CUDA 13.x support
- Triggered by manual tag releases only

## Installation Examples

### Standard Installation
```bash
# Latest stable (CUDA)
pip install mooncake-transfer-engine

# Latest stable (Non-CUDA)
pip install mooncake-transfer-engine-non-cuda

# Specific version
pip install mooncake-transfer-engine==0.4.0

# Upgrade to latest (includes post releases)
pip install --upgrade mooncake-transfer-engine
```

### Post Release Behavior
```bash
# User has 0.4.0 installed
pip install --upgrade mooncake-transfer-engine
# Will upgrade to 0.4.0.post1 if available

# Explicit post version
pip install mooncake-transfer-engine==0.4.0.post1
```

## Release Checklist

### Monthly Release (Automated)
- ✅ Runs automatically on 1st of month
- ✅ Version bump handled automatically
- ✅ Git tag created automatically
- ✅ Wheels built for all Python versions
- ✅ Published to PyPI automatically
- ⚠️ Monitor GitHub Actions for failures
- ⚠️ Verify PyPI upload success

### Hotfix Release (Manual)
1. Identify critical bug requiring immediate fix
2. Merge fix to main branch
3. Go to GitHub Actions → "Hotfix Release (Post Version)"
4. Click "Run workflow"
5. Enter:
   - Base version (e.g., `0.4.0`)
   - Post number (e.g., `1`)
   - Description of fix
6. Monitor workflow execution
7. Verify PyPI upload
8. Announce hotfix to users

### Manual Release (Tag-Based)
1. Ensure all changes are merged to main
2. Create and push tag: `git tag v0.4.0 && git push origin v0.4.0`
3. Workflows trigger automatically
4. Monitor GitHub Actions
5. Verify PyPI upload

## GitHub Actions Secrets Required

- `PYPI_API_TOKEN`: PyPI API token for publishing packages
- `GITHUB_TOKEN`: Automatically provided by GitHub Actions

## Monitoring and Notifications

### Success Indicators
- ✅ GitHub Release created with wheels attached
- ✅ PyPI shows new version available
- ✅ All workflow jobs completed successfully

### Failure Handling
- Check GitHub Actions logs for errors
- Common issues:
  - Build failures (dependency issues, CUDA problems)
  - PyPI upload failures (token expiration, duplicate version)
  - Version conflicts (tag already exists)

## Best Practices

1. **Monthly Releases**: Let automation handle routine releases
2. **Hotfixes**: Use sparingly, only for critical issues
3. **Testing**: Ensure CI passes before releases
4. **Communication**: Announce major releases and hotfixes
5. **Documentation**: Update changelog and release notes
6. **Version Discipline**: Follow semantic versioning principles

## Migration from Current Setup

The new workflows complement existing tag-based releases:
- Existing `release.yaml` workflows remain functional
- Monthly automation adds predictable release cadence
- Hotfix workflow provides emergency fix capability
- No breaking changes to current process

## Future Enhancements

- [ ] Automated changelog generation from commit messages
- [ ] Pre-release versions (alpha, beta, rc)
- [ ] Automated rollback on critical failures
- [ ] Integration with issue tracker for release notes
- [ ] Slack/Discord notifications for releases
- [ ] Automated testing of published wheels
