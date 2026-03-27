"""Root conftest — runs one-time repository setup on first test session."""

import subprocess, shutil
from pathlib import Path

_REPO = Path(__file__).resolve().parent


def pytest_configure(config):
    """Auto-install dev tooling (nbstripout filter, pre-commit hook)."""
    _ensure_nbstripout()
    _ensure_pre_commit_hook()


def _ensure_nbstripout():
    if shutil.which("nbstripout") is None:
        return
    result = subprocess.run(
        ["git", "config", "--local", "filter.nbstripout.clean"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        subprocess.run(["nbstripout", "--install"], check=True)


def _ensure_pre_commit_hook():
    hook_src = _REPO / "hooks" / "pre-commit"
    hook_dst = _REPO / ".git" / "hooks" / "pre-commit"
    if not hook_src.exists():
        return
    if hook_dst.is_symlink() and hook_dst.resolve() == hook_src.resolve():
        return
    hook_dst.unlink(missing_ok=True)
    hook_dst.symlink_to(hook_src)
