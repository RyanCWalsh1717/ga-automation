"""
Run Log
=======
Appends a row to GA_Run_Log.csv each time Pass 2 reports are generated.
Provides a persistent, carry-forward audit trail of every close run.

Columns: timestamp | prepared_by | property | period |
         files_generated | qc_checks_passed | qc_checks_failed
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import List, Optional

_COLUMNS = [
    'timestamp',
    'prepared_by',
    'property',
    'period',
    'files_generated',
    'qc_checks_passed',
    'qc_checks_failed',
]


def append_run_log(
    output_path: str,
    prior_log_path: Optional[str],
    timestamp: str,
    prepared_by: str,
    property_name: str,
    period: str,
    files_generated: List[str],
    qc_checks_passed: int,
    qc_checks_failed: int,
) -> str:
    """
    Build GA_Run_Log.csv at output_path.

    If prior_log_path is a valid CSV, its rows are carried forward and the new
    row is appended at the bottom.  Otherwise a fresh file is created.

    Args:
        output_path:       Destination path for the updated CSV.
        prior_log_path:    Optional path to prior-period GA_Run_Log.csv.
        timestamp:         ISO-style string: 'YYYY-MM-DD HH:MM:SS'.
        prepared_by:       Name of the preparer (e.g. 'Ryan Walsh').
        property_name:     Property display name (e.g. 'Revolution Labs').
        period:            Close period string (e.g. 'Jan-2026').
        files_generated:   List of output file names included in the ZIP.
        qc_checks_passed:  Number of QC checks that passed.
        qc_checks_failed:  Number of QC checks that flagged or failed.

    Returns:
        output_path (for convenience in caller).
    """
    existing_rows: list[dict] = []

    # ── Carry forward prior log rows ──────────────────────────────────────────
    if prior_log_path and os.path.exists(prior_log_path):
        try:
            with open(prior_log_path, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    # Normalise: only keep known columns, fill missing with ''
                    existing_rows.append({col: row.get(col, '') for col in _COLUMNS})
        except Exception:
            existing_rows = []   # corrupt prior log — start fresh

    # ── New row ───────────────────────────────────────────────────────────────
    new_row = {
        'timestamp':         timestamp,
        'prepared_by':       prepared_by or 'Ryan Walsh',
        'property':          property_name or 'Revolution Labs',
        'period':            period,
        'files_generated':   '; '.join(files_generated),
        'qc_checks_passed':  str(qc_checks_passed),
        'qc_checks_failed':  str(qc_checks_failed),
    }
    existing_rows.append(new_row)

    # ── Write ─────────────────────────────────────────────────────────────────
    with open(output_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=_COLUMNS)
        writer.writeheader()
        writer.writerows(existing_rows)

    return output_path
