#!/usr/bin/env python3
"""Calculate and verify release versions from Conventional Commits.

Project policy:
- feat: minor
- fix, perf, docs, chore, ci, test, build, style, refactor, revert: patch
- ! or BREAKING CHANGE: major
- feat(demo) limited to demo/docs/tests/metadata: patch exception
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

VERSION_RE = re.compile(r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
CONVENTIONAL_RE = re.compile(
    r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?(?P<breaking>!)?: (?P<subject>.+)$"
)
DEMO_PREFIXES = (
    ".github/",
    "benchmarks/",
    "demo/",
    "docs/",
    "examples/",
    "scripts/",
    "tests/",
)
DEMO_FILES = {
    "CHANGELOG.md",
    "Dockerfile",
    "README.md",
    "docker-compose.yml",
    "mkdocs.yml",
    "pyproject.toml",
    "uv.lock",
}
VERSION_FILE = "src/fastapi_stream_lease/__init__.py"
ROOT = Path.cwd()
PYPROJECT = ROOT / "pyproject.toml"
INIT_FILE = ROOT / VERSION_FILE


class ReleaseError(RuntimeError):
    """Raised when the release metadata or commit history is invalid."""


@dataclass(frozen=True)
class ParsedSubject:
    type: str
    scope: str | None
    breaking: bool
    subject: str


@dataclass(frozen=True)
class Commit:
    sha: str
    subject: str
    body: str
    paths: tuple[str, ...]
    init_version_only: bool

    @property
    def parsed(self) -> ParsedSubject:
        return parse_subject(self.subject)

    @property
    def breaking(self) -> bool:
        return (
            self.parsed.breaking
            or "BREAKING CHANGE:" in self.body
            or "BREAKING-CHANGE:" in self.body
        )

    def is_demo_only(self) -> bool:
        for path in self.paths:
            if path == VERSION_FILE:
                if not self.init_version_only:
                    return False
                continue
            if path in DEMO_FILES or path.startswith(DEMO_PREFIXES):
                continue
            return False
        return True


def parse_subject(subject: str) -> ParsedSubject:
    match = CONVENTIONAL_RE.match(subject)
    if match is None:
        raise ReleaseError(f"commit is not a Conventional Commit: {subject!r}")
    return ParsedSubject(
        type=match.group("type"),
        scope=match.group("scope"),
        breaking=match.group("breaking") == "!",
        subject=match.group("subject"),
    )


def parse_version(value: str) -> tuple[int, int, int]:
    match = VERSION_RE.match(value)
    if match is None:
        raise ReleaseError(f"invalid semantic version: {value!r}")
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )


def bump_version(value: str, bump: str) -> str:
    major, minor, patch = parse_version(value)
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if bump == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise ReleaseError(f"unknown bump: {bump!r}")


def required_bump(commits: Sequence[Commit]) -> str:
    if not commits:
        raise ReleaseError("no commits found since the previous tag")
    if any(commit.breaking for commit in commits):
        return "major"
    for commit in commits:
        if commit.parsed.type != "feat":
            continue
        if commit.parsed.scope == "demo" and commit.is_demo_only():
            continue
        return "minor"
    return "patch"


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or f"git {' '.join(args)} failed"
        raise ReleaseError(message)
    return result.stdout


def latest_tag(ref: str = "HEAD") -> str | None:
    try:
        return _git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*", ref).strip()
    except ReleaseError:
        return None


def previous_tag(tag: str) -> str:
    previous = latest_tag(f"{tag}^")
    if previous is None:
        raise ReleaseError(f"no previous tag found for {tag}")
    return previous


def _commit_paths(sha: str) -> tuple[str, ...]:
    output = _git("show", "--pretty=format:", "--name-only", sha)
    return tuple(line for line in output.splitlines() if line.strip())


def _init_version_only(sha: str) -> bool:
    diff = _git("show", "--pretty=format:", "--unified=0", sha, "--", VERSION_FILE)
    changed = [
        line
        for line in diff.splitlines()
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    ]
    return bool(changed) and all("__version__" in line for line in changed)


def load_commits(since: str) -> list[Commit]:
    output = _git(
        "log",
        "--first-parent",
        "--no-merges",
        "--format=%H%x00%s%x00%b%x1e",
        f"{since}..HEAD",
    )
    commits: list[Commit] = []
    for record in output.split("\x1e"):
        record = record.strip("\n")
        if not record:
            continue
        sha, subject, body = record.split("\x00", 2)
        paths = _commit_paths(sha)
        commits.append(
            Commit(
                sha=sha,
                subject=subject,
                body=body,
                paths=paths,
                init_version_only=VERSION_FILE in paths and _init_version_only(sha),
            )
        )
    return commits


def next_version() -> tuple[str, str, str]:
    previous = latest_tag()
    if previous is None:
        raise ReleaseError("no previous vX.Y.Z tag found")
    commits = load_commits(previous)
    bump = required_bump(commits)
    return bump_version(previous, bump), bump, previous


def check_tag(tag: str) -> None:
    previous = previous_tag(tag)
    commits = load_commits(previous)
    bump = required_bump(commits)
    expected = f"v{bump_version(previous, bump)}"
    if tag != expected:
        raise ReleaseError(f"release tag {tag} does not match expected {expected} ({bump})")
    print(f"{tag}: {previous} -> {expected} ({bump})")


def _replace_version(path: Path, pattern: str, version: str) -> None:
    text = path.read_text(encoding="utf-8")
    replacement = f'version = "{version}"' if path == PYPROJECT else f'__version__ = "{version}"'
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ReleaseError(f"could not find version field in {path}")
    path.write_text(updated, encoding="utf-8")


def write_version(version: str) -> None:
    _replace_version(PYPROJECT, r'^version = "[^"]+"', version)
    _replace_version(INIT_FILE, r'^__version__ = "[^"]+"', version)
    result = subprocess.run(
        ["uv", "lock"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "uv lock failed"
        raise ReleaseError(message)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", metavar="TAG", help="verify a release tag against its commits")
    group.add_argument("--next", action="store_true", help="print the next release version")
    group.add_argument("--write", action="store_true", help="update version files and uv.lock")
    args = parser.parse_args(argv)

    try:
        if args.check:
            check_tag(args.check)
        elif args.write:
            version, bump, previous = next_version()
            write_version(version)
            print(f"Updated to {version} ({bump} from {previous}); run tests and tag v{version}.")
        else:
            version, bump, previous = next_version()
            print(f"v{version} ({bump} from {previous})")
    except ReleaseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
