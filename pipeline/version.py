"""
Pipeline version — resolves the current git commit hash for audit stamps.

Cached at module level; git is called at most once per process.
On Streamlit Cloud the app runs inside a git clone so git is always available.
Falls back gracefully if git is absent or the call fails.

Returned format:  'GA Automation v2  •  abc1234  •  2026-06-15'
                   (version tag)         (short hash)  (commit date)
"""
from __future__ import annotations

import os
import subprocess

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_cached: str | None = None


def get_version() -> str:
    """Return the pipeline version string, e.g. 'GA Automation v2  •  abc1234  •  2026-06-15'."""
    global _cached
    if _cached is not None:
        return _cached
    try:
        _hash = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, cwd=_REPO_ROOT, timeout=3,
        ).stdout.strip()
        _date = subprocess.run(
            ['git', 'log', '-1', '--format=%as'],
            capture_output=True, text=True, cwd=_REPO_ROOT, timeout=3,
        ).stdout.strip()
        if _hash:
            _cached = f'GA Automation v2  •  {_hash}  •  {_date}' if _date else f'GA Automation v2  •  {_hash}'
        else:
            _cached = 'GA Automation v2'
    except Exception:
        _cached = 'GA Automation v2'
    return _cached
