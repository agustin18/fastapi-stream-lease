from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.release_version import (
    Commit,
    ReleaseError,
    bump_version,
    parse_subject,
    parse_version,
    required_bump,
)


def make_commit(
    subject: str,
    *,
    paths: tuple[str, ...] = ("demo/app.py",),
    body: str = "",
    init_version_only: bool = False,
) -> Commit:
    return Commit(
        sha="abc123",
        subject=subject,
        body=body,
        paths=paths,
        init_version_only=init_version_only,
    )


def test_parse_subject_with_scope_and_breaking_marker():
    parsed = parse_subject("feat(api)!: add a new endpoint")

    assert parsed.type == "feat"
    assert parsed.scope == "api"
    assert parsed.breaking is True
    assert parsed.subject == "add a new endpoint"


def test_parse_subject_rejects_non_conventional_title():
    with pytest.raises(ReleaseError, match="not a Conventional Commit"):
        parse_subject("update stuff")


@pytest.mark.parametrize(
    ("value", "bump", "expected"),
    [
        ("0.6.3", "patch", "0.6.4"),
        ("0.6.3", "minor", "0.7.0"),
        ("0.6.3", "major", "1.0.0"),
        ("v1.2.3", "patch", "1.2.4"),
    ],
)
def test_bump_version(value: str, bump: str, expected: str):
    assert bump_version(value, bump) == expected


def test_parse_version_rejects_invalid_values():
    with pytest.raises(ReleaseError, match="invalid semantic version"):
        parse_version("1.2")


def test_demo_only_feat_is_a_patch():
    commits = [
        make_commit(
            "feat(demo): clarify limits",
            paths=(
                "demo/app.py",
                "README.md",
                "CHANGELOG.md",
                "pyproject.toml",
                "uv.lock",
                "src/fastapi_stream_lease/__init__.py",
            ),
            init_version_only=True,
        )
    ]

    assert required_bump(commits) == "patch"


def test_demo_feat_touching_runtime_is_a_minor():
    commits = [
        make_commit(
            "feat(demo): add an example",
            paths=("demo/app.py", "src/fastapi_stream_lease/manager.py"),
        )
    ]

    assert required_bump(commits) == "minor"


def test_demo_feat_touching_init_code_is_a_minor():
    commits = [
        make_commit(
            "feat(demo): add an example",
            paths=("demo/app.py", "src/fastapi_stream_lease/__init__.py"),
            init_version_only=False,
        )
    ]

    assert required_bump(commits) == "minor"


def test_feat_without_demo_scope_is_a_minor():
    assert required_bump([make_commit("feat(manager): add capacity")]) == "minor"


def test_fix_is_a_patch():
    assert required_bump([make_commit("fix(manager): handle timeout")]) == "patch"


def test_breaking_bang_is_a_major():
    assert required_bump([make_commit("feat(manager)!: change the API")]) == "major"


def test_breaking_body_is_a_major():
    commits = [make_commit("feat(demo): update", body="BREAKING CHANGE: new behavior")]

    assert required_bump(commits) == "major"


def test_empty_history_is_rejected():
    with pytest.raises(ReleaseError, match="no commits"):
        required_bump([])


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _release_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test User")

    (repo / "demo").mkdir()
    (repo / "demo" / "app.py").write_text("version = 1\n", encoding="utf-8")
    (repo / "src" / "fastapi_stream_lease").mkdir(parents=True)
    (repo / "src" / "fastapi_stream_lease" / "__init__.py").write_text(
        '__version__ = "0.6.3"\n', encoding="utf-8"
    )
    (repo / "pyproject.toml").write_text('version = "0.6.3"\n', encoding="utf-8")
    (repo / "CHANGELOG.md").write_text("# Changelog\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "chore(release): v0.6.3")
    _git(repo, "tag", "v0.6.3")

    (repo / "demo" / "app.py").write_text("version = 2\n", encoding="utf-8")
    (repo / "src" / "fastapi_stream_lease" / "__init__.py").write_text(
        '__version__ = "0.6.4"\n', encoding="utf-8"
    )
    (repo / "pyproject.toml").write_text('version = "0.6.4"\n', encoding="utf-8")
    (repo / "CHANGELOG.md").write_text("# Changelog\n\n## 0.6.4\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "feat(demo): clarify limits")
    _git(repo, "tag", "v0.6.4")
    return repo


def _run_check(repo: Path, tag: str) -> subprocess.CompletedProcess[str]:
    script = Path(__file__).parents[1] / "scripts" / "release_version.py"
    return subprocess.run(
        [sys.executable, str(script), "--check", tag],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )


def test_check_accepts_demo_only_patch_release(tmp_path: Path):
    repo = _release_repo(tmp_path)

    result = _run_check(repo, "v0.6.4")

    assert result.returncode == 0, result.stderr
    assert "v0.6.4: v0.6.3 -> v0.6.4 (patch)" in result.stdout


def test_check_rejects_patch_tag_for_runtime_feat(tmp_path: Path):
    repo = _release_repo(tmp_path)
    (repo / "src" / "fastapi_stream_lease" / "manager.py").write_text(
        "capacity = 1\n", encoding="utf-8"
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "feat(manager): add capacity")
    _git(repo, "tag", "v0.6.5")

    result = _run_check(repo, "v0.6.5")

    assert result.returncode == 1
    assert "does not match expected v0.7.0" in result.stderr
