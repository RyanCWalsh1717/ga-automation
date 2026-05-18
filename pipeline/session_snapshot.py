"""
Session Snapshot — save and restore key pipeline inputs between sessions.

Persists the user-entered table data (One-Off Accruals, Pass 2 manual JEs) and
settings so that closing the browser doesn't wipe hours of data entry.

File uploads are NOT persisted — browser security prevents it, and re-uploading
Yardi exports is fast.  The snapshot covers the data that's painful to re-enter.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

_SCHEMA_VERSION = 2

# ── Public API ────────────────────────────────────────────────

def save_snapshot(session_state: Any) -> bytes:
    """
    Serialize key session_state values to compact JSON bytes for download.

    Persisted keys:
      - active_property_code
      - prepared_by
      - checklist_period_key
      - manual_accruals_df   (One-Off Accruals table)
      - post_close_je_df     (Pass 2 manual JEs table)
      - je_desc_overrides    (Pass 1 JE description overrides)
      - bonus_overrides      (Pass 1 bonus amounts by account)

    Returns:
        UTF-8 JSON bytes suitable for st.download_button(data=...).
    """
    data: dict = {
        'schema_version': _SCHEMA_VERSION,
        'saved_at': datetime.now().isoformat(),
        'active_property_code': session_state.get('active_property_code', '') or '',
        'prepared_by': session_state.get('prepared_by', '') or '',
        'checklist_period_key': session_state.get('checklist_period_key', '') or '',
        'je_desc_overrides': dict(session_state.get('je_desc_overrides', {}) or {}),
    }

    # One-Off Accruals table (manual_accruals_df is a pandas DataFrame)
    df_accruals = session_state.get('manual_accruals_df')
    if df_accruals is not None:
        try:
            data['manual_accruals_df'] = df_accruals.to_dict(orient='records')
        except Exception:
            data['manual_accruals_df'] = []
    else:
        data['manual_accruals_df'] = []

    # Pass 2 manual JEs table
    df_jes = session_state.get('post_close_je_df')
    if df_jes is not None:
        try:
            data['post_close_je_df'] = df_jes.to_dict(orient='records')
        except Exception:
            data['post_close_je_df'] = []
    else:
        data['post_close_je_df'] = []

    return json.dumps(data, indent=2, default=str).encode('utf-8')


def load_snapshot(json_bytes: bytes) -> dict:
    """
    Parse snapshot bytes.  Returns empty dict on any parse failure — the caller
    should check the returned dict before applying it.
    """
    try:
        raw = json.loads(json_bytes.decode('utf-8'))
        if not isinstance(raw, dict):
            return {}
        return raw
    except Exception:
        return {}


def restore_snapshot(data: dict, session_state: Any) -> list[str]:
    """
    Apply a parsed snapshot dict to Streamlit session_state in-place.

    Args:
        data:          Dict returned by load_snapshot()
        session_state: Streamlit st.session_state object (or any Mapping)

    Returns:
        List of human-readable strings describing what was restored.
        Empty list means nothing was applied (schema mismatch, empty data, etc.).
    """
    try:
        import pandas as pd
    except ImportError:
        return []

    if not data:
        return []

    restored: list[str] = []

    # ── Scalar settings ────────────────────────────────────────
    for key, label in [
        ('active_property_code', 'Property'),
        ('prepared_by',          'Prepared by'),
        ('checklist_period_key', 'Close period'),
    ]:
        val = data.get(key)
        if val:
            session_state[key] = val
            restored.append(label)

    # ── JE description overrides ───────────────────────────────
    overrides = data.get('je_desc_overrides')
    if isinstance(overrides, dict) and overrides:
        session_state['je_desc_overrides'] = overrides
        restored.append('JE description overrides')

    # ── One-Off Accruals table ─────────────────────────────────
    accruals_rows = data.get('manual_accruals_df', [])
    if accruals_rows:
        try:
            df = pd.DataFrame(accruals_rows)
            # Ensure Amount column is numeric
            if 'Amount ($)' in df.columns:
                df['Amount ($)'] = pd.to_numeric(df['Amount ($)'], errors='coerce').fillna(0.0)
            session_state['manual_accruals_df'] = df
            restored.append(f'One-Off Accruals ({len(df)} rows)')
        except Exception:
            pass

    # ── Pass 2 manual JEs table ────────────────────────────────
    je_rows = data.get('post_close_je_df', [])
    if je_rows:
        try:
            df = pd.DataFrame(je_rows)
            for _col, _dtype in [('Debit ($)', float), ('Credit ($)', float)]:
                if _col in df.columns:
                    df[_col] = pd.to_numeric(df[_col], errors='coerce').fillna(0.0)
            session_state['post_close_je_df'] = df
            restored.append(f'Pass 2 manual JEs ({len(df)} rows)')
        except Exception:
            pass

    return restored


def snapshot_filename(session_state: Any) -> str:
    """Return a descriptive filename for the snapshot download."""
    prop = session_state.get('active_property_code', 'property') or 'property'
    period = (session_state.get('checklist_period_key', '') or '').replace('_', '-')
    ts = datetime.now().strftime('%Y%m%d')
    parts = [p for p in [prop, period, ts] if p]
    return f"GA_Session_{'_'.join(parts)}.json"
