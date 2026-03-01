#!/usr/bin/env python3
"""
Mooncake Release Management Script

This script helps manage Mooncake releases by providing utilities for:
- Version validation and bumping
- Release preparation
- Changelog generation
- PyPI package verification

Usage:
    python scripts/release_manager.py --help
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Tuple, Optional
from datetime import datetime


class ReleaseManager:
    """Manages Mooncake release operations."""

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.pyproject_path = repo_root / "mooncake-wheel" / "pyproject.toml"

    def get_current_version(self) -> str:
        """Read current version from pyproject.toml."""
        content = self.pyproject_path.read_text()
        match = re.search(r'version = "([^"]+)"', content)
        if not match:
            raise ValueError("Could not find version in pyproject.toml")
        return match.group(1)

    def validate_version(self, version: str) -> bool:
        """Validate version string according to PEP 440."""
        # Standard version: X.Y.Z
        standard_pattern = r'^\d+\.\d+\.\d+$'
        # Post version: X.Y.Z.postN
        post_pattern = r'^\d+\.\d+\.\d+\.post\d+$'

        return bool(re.match(standard_pattern, version) or re.match(post_pattern, version))

    def parse_version(self, version: str) -> Tuple[int, int, int, Optional[int]]:
        """Parse version string into components."""
        if '.post' in version:
            base, post = version.split('.post')
            major, minor, patch = map(int, base.split('.'))
            return major, minor, patch, int(post)
        else:
            major, minor, patch = map(int, version.split('.'))
            return major, minor, patch, None

    def bump_version(self, version_type: str) -> str:
        """Bump version based on type (major, minor, patch)."""
        current = self.get_current_version()
        major, minor, patch, post = self.parse_version(current)

        # Remove post version for standard bumps
        if version_type == 'major':
            return f"{major + 1}.0.0"
        elif version_type == 'minor':
            return f"{major}.{minor + 1}.0"
        elif version_type == 'patch':
            return f"{major}.{minor}.{patch + 1}"
        else:
            raise ValueError(f"Invalid version type: {version_type}")

    def create_post_version(self, base_version: str, post_number: int) -> str:
        """Create a post version string."""
        if not self.validate_version(base_version):
            raise ValueError(f"Invalid base version: {base_version}")
        if '.post' in base_version:
            raise ValueError("Base version should not contain .post")
        return f"{base_version}.post{post_number}"

    def update_version_file(self, new_version: str) -> None:
        """Update version in pyproject.toml."""
        content = self.pyproject_path.read_text()
        updated = re.sub(
            r'version = "[^"]+"',
            f'version = "{new_version}"',
            content
        )
        self.pyproject_path.write_text(updated)
        print(f"✓ Updated version to {new_version} in {self.pyproject_path}")

    def get_git_tags(self) -> list:
        """Get all git tags."""
        result = subprocess.run(
            ['git', 'tag', '-l'],
            cwd=self.repo_root,
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split('\n') if result.stdout else []

    def tag_exists(self, tag: str) -> bool:
        """Check if a git tag exists."""
        return tag in self.get_git_tags()

    def get_commits_since_tag(self, tag: str) -> list:
        """Get commit messages since a specific tag."""
        result = subprocess.run(
            ['git', 'log', f'{tag}..HEAD', '--pretty=format:%s'],
            cwd=self.repo_root,
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split('\n') if result.stdout else []

    def generate_changelog(self, since_tag: Optional[str] = None) -> str:
        """Generate changelog from git commits."""
        if since_tag:
            commits = self.get_commits_since_tag(since_tag)
        else:
            # Get last 20 commits
            result = subprocess.run(
                ['git', 'log', '-20', '--pretty=format:%s'],
                cwd=self.repo_root,
                capture_output=True,
                text=True
            )
            commits = result.stdout.strip().split('\n') if result.stdout else []

        # Categorize commits
        features = []
        fixes = []
        docs = []
        other = []

        for commit in commits:
            commit = commit.strip()
            if not commit:
                continue

            lower = commit.lower()
            if any(word in lower for word in ['feat', 'feature', 'add']):
                features.append(commit)
            elif any(word in lower for word in ['fix', 'bug', 'patch']):
                fixes.append(commit)
            elif any(word in lower for word in ['doc', 'readme']):
                docs.append(commit)
            else:
                other.append(commit)

        # Build changelog
        changelog = []
        if features:
            changelog.append("### Features")
            for feat in features:
                changelog.append(f"- {feat}")
            changelog.append("")

        if fixes:
            changelog.append("### Bug Fixes")
            for fix in fixes:
                changelog.append(f"- {fix}")
            changelog.append("")

        if docs:
            changelog.append("### Documentation")
            for doc in docs:
                changelog.append(f"- {doc}")
            changelog.append("")

        return '\n'.join(changelog)

    def verify_pypi_package(self, package_name: str, version: str) -> bool:
        """Verify if a package version exists on PyPI."""
        try:
            import requests
            url = f"https://pypi.org/pypi/{package_name}/{version}/json"
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"Warning: Could not verify PyPI package: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Mooncake Release Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show current version
  python scripts/release_manager.py current

  # Bump version
  python scripts/release_manager.py bump minor

  # Create post version
  python scripts/release_manager.py post 0.4.0 1

  # Generate changelog
  python scripts/release_manager.py changelog --since v0.3.9

  # Verify PyPI package
  python scripts/release_manager.py verify mooncake-transfer-engine 0.3.9
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Current version command
    subparsers.add_parser('current', help='Show current version')

    # Bump version command
    bump_parser = subparsers.add_parser('bump', help='Bump version')
    bump_parser.add_argument(
        'type',
        choices=['major', 'minor', 'patch'],
        help='Version bump type'
    )
    bump_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show new version without updating files'
    )

    # Post version command
    post_parser = subparsers.add_parser('post', help='Create post version')
    post_parser.add_argument('base_version', help='Base version (e.g., 0.4.0)')
    post_parser.add_argument('post_number', type=int, help='Post number (e.g., 1)')
    post_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show new version without updating files'
    )

    # Changelog command
    changelog_parser = subparsers.add_parser('changelog', help='Generate changelog')
    changelog_parser.add_argument(
        '--since',
        help='Generate changelog since this tag'
    )

    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify PyPI package')
    verify_parser.add_argument('package', help='Package name')
    verify_parser.add_argument('version', help='Version to verify')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate version string')
    validate_parser.add_argument('version', help='Version to validate')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Find repo root
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent

    manager = ReleaseManager(repo_root)

    try:
        if args.command == 'current':
            version = manager.get_current_version()
            print(f"Current version: {version}")

        elif args.command == 'bump':
            new_version = manager.bump_version(args.type)
            if args.dry_run:
                print(f"Would bump to: {new_version}")
            else:
                manager.update_version_file(new_version)
                print(f"Version bumped to: {new_version}")

        elif args.command == 'post':
            new_version = manager.create_post_version(
                args.base_version,
                args.post_number
            )
            if args.dry_run:
                print(f"Would create post version: {new_version}")
            else:
                manager.update_version_file(new_version)
                print(f"Created post version: {new_version}")

        elif args.command == 'changelog':
            changelog = manager.generate_changelog(args.since)
            print(changelog)

        elif args.command == 'verify':
            exists = manager.verify_pypi_package(args.package, args.version)
            if exists:
                print(f"✓ Package {args.package} {args.version} exists on PyPI")
            else:
                print(f"✗ Package {args.package} {args.version} not found on PyPI")
                return 1

        elif args.command == 'validate':
            if manager.validate_version(args.version):
                print(f"✓ Version {args.version} is valid")
            else:
                print(f"✗ Version {args.version} is invalid")
                return 1

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
