"""
checklist_persistence.py — Save / load monthly close checklist state
=====================================================================
Saves to data/{property_code}/checklist_{YYYY_MM}.json via GitHub API
(same credentials as property_writer.py), with local-disk fallback.

On Streamlit Cloud the app has an ephemeral filesystem, so GitHub is the
only durable store.  Without GitHub credentials the checklist is session-
only (resets on refresh) but still fully usable within a single session.

State format
------------
{
  "period":        "2026-05",
  "property_code": "revlabspm",
  "steps": {
    "0": {"completed_by": "Jane Smith", "timestamp": "05/08/2026 14:14",
          "auto": false},
    ...
  },
  "custom_items": [
    {
      "id":           "custom_0",
      "label":        "Confirm Berkadia loan pay-off",
      "created_by":   "Jane Smith",
      "created_at":   "05/08/2026 14:00",
      "completed":    false,
      "completed_by": null,
      "completed_at": null
    }
  ]
}
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ── Helpers ───────────────────────────────────────────────────────────────────

def period_to_key(period: str) -> str:
    """
    Convert any period string to a filename-safe YYYY_MM key.
    Accepts:  'May 2026', 'May-2026', '2026-05', '05/2026', 'May2026'
    Returns:  '2026_05'
    """
    period = (period or '').strip()
    # Already YYYY-MM or YYYY_MM
    import re
    m = re.match(r'^(\d{4})[-_](\d{2})$', period)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    # MM/YYYY
    m = re.match(r'^(\d{1,2})/(\d{4})$', period)
    if m:
        return f"{m.group(2)}_{int(m.group(1)):02d}"
    # Month YYYY or Month-YYYY
    _months = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    m = re.match(r'^([A-Za-z]{3,9})[-\s]?(\d{4})$', period)
    if m:
        mon = _months.get(m.group(1)[:3].lower(), 0)
        if mon:
            return f"{m.group(2)}_{mon:02d}"
    # Fallback: use period as-is but replace unsafe chars
    return re.sub(r'[^A-Za-z0-9]', '_', period)


def _github_credentials() -> Tuple[str, str]:
    """Return (token, repo) from st.secrets or env vars."""
    token, repo = '', ''
    try:
        import streamlit as st
        token = st.secrets.get('github', {}).get('token', '')
        repo  = st.secrets.get('github', {}).get('repo',  '')
    except Exception:
        pass
    token = token or os.environ.get('GITHUB_TOKEN', '')
    repo  = repo  or os.environ.get('GITHUB_REPO',  '')
    return token, repo


def _local_path(property_code: str, period_key: str, data_dir: str) -> str:
    return os.path.join(data_dir, property_code, f'checklist_{period_key}.json')


# ── Load ──────────────────────────────────────────────────────────────────────

def load_checklist(
    property_code: str,
    period_key: str,
    data_dir: str,
) -> Dict[str, Any]:
    """
    Load checklist state.  Priority: GitHub API → local file → empty default.
    Returns the state dict (never raises).
    """
    # Try GitHub first
    token, repo = _github_credentials()
    if token and repo:
        state = _load_from_github(property_code, period_key, token, repo)
        if state is not None:
            return state

    # Try local disk
    path = _local_path(property_code, period_key, data_dir)
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass

    # Empty default
    return _empty_state(property_code, period_key)


def _load_from_github(
    property_code: str, period_key: str, token: str, repo: str
) -> Optional[Dict[str, Any]]:
    try:
        import base64
        import requests
        path = f'data/{property_code}/checklist_{period_key}.json'
        url  = f'https://api.github.com/repos/{repo}/contents/{path}'
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            content = base64.b64decode(r.json()['content']).decode('utf-8')
            return json.loads(content)
    except Exception:
        pass
    return None


def _empty_state(property_code: str, period_key: str) -> Dict[str, Any]:
    return {
        'period':        period_key,
        'property_code': property_code,
        'steps':         {},
        'custom_items':  [],
        'locked':        False,
        'locked_by':     None,
        'locked_at':     None,
    }


# ── Save ──────────────────────────────────────────────────────────────────────

def save_checklist(
    property_code: str,
    period_key: str,
    state: Dict[str, Any],
    data_dir: str,
) -> Tuple[bool, str]:
    """
    Save checklist state to GitHub (preferred) and local disk.
    Returns (success, message).
    """
    payload = json.dumps(state, indent=2, ensure_ascii=False)

    # Local save (best-effort)
    try:
        local_path = _local_path(property_code, period_key, data_dir)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(payload)
    except Exception:
        pass

    # GitHub save
    token, repo = _github_credentials()
    if token and repo:
        ok, msg = _save_to_github(property_code, period_key, payload, token, repo)
        return ok, msg

    return True, 'saved locally (no GitHub credentials configured)'


def _save_to_github(
    property_code: str, period_key: str,
    payload: str, token: str, repo: str,
) -> Tuple[bool, str]:
    try:
        import base64
        import requests
        path = f'data/{property_code}/checklist_{period_key}.json'
        url  = f'https://api.github.com/repos/{repo}/contents/{path}'
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        sha = None
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            sha = r.json().get('sha')
        body: dict = {
            'message': f'Update checklist: {property_code} {period_key}',
            'content': base64.b64encode(payload.encode('utf-8')).decode('ascii'),
        }
        if sha:
            body['sha'] = sha
        r = requests.put(url, json=body, headers=headers, timeout=15)
        if r.status_code in (200, 201):
            return True, 'checklist saved to GitHub'
        return False, f'GitHub returned {r.status_code}'
    except Exception as e:
        return False, str(e)


# ── Session-state ↔ persistence format conversion ───────────────────────────

def session_to_state(
    close_tracker: Dict[int, Dict],
    custom_items: List[Dict],
    property_code: str,
    period_key: str,
    locked: bool = False,
    locked_by: Optional[str] = None,
    locked_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert app session_state format → persistence format."""
    steps = {str(k): v for k, v in close_tracker.items()}
    return {
        'period':        period_key,
        'property_code': property_code,
        'steps':         steps,
        'custom_items':  custom_items,
        'locked':        locked,
        'locked_by':     locked_by,
        'locked_at':     locked_at,
    }


def state_to_session(
    state: Dict[str, Any],
) -> Tuple[Dict[int, Dict], List[Dict], bool, Optional[str], Optional[str]]:
    """
    Convert persistence format → (close_tracker, custom_items, locked, locked_by, locked_at).
    """
    close_tracker = {int(k): v for k, v in (state.get('steps') or {}).items()}
    custom_items  = list(state.get('custom_items') or [])
    locked        = bool(state.get('locked', False))
    locked_by     = state.get('locked_by')
    locked_at     = state.get('locked_at')
    return close_tracker, custom_items, locked, locked_by, locked_at


# ── Convenience: current-month period key ────────────────────────────────────

def current_period_key() -> str:
    """Return the current month as 'YYYY_MM', e.g. '2026_05'."""
    now = datetime.now()
    return f'{now.year}_{now.month:02d}'


def period_key_to_label(key: str) -> str:
    """Convert '2026_05' → 'May 2026'."""
    _month_names = [
        '', 'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December',
    ]
    import re
    m = re.match(r'^(\d{4})_(\d{2})$', key)
    if m:
        year = int(m.group(1))
        mon  = int(m.group(2))
        if 1 <= mon <= 12:
            return f'{_month_names[mon]} {year}'
    return key
