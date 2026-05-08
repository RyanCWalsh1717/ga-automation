"""
Run Log
=======
Appends a row to GA_Run_Log.csv each time Pass 1 or Pass 2 reports are generated.
Provides a persistent, carry-forward audit trail of every close run.

Columns: timestamp | pass_number | prepared_by | property | period |
         files_generated | qc_checks_passed | qc_checks_failed |
         je_count | je_total_dollars | close_tracker_complete
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import List, Optional

_COLUMNS = [
    'timestamp',
    'pass_number',
    'prepared_by',
    'property',
    'period',
    'files_generated',
    'qc_checks_passed',
    'qc_checks_failed',
    'je_count',
    'je_total_dollars',
    'close_tracker_complete',
]


def _read_prior(prior_log_path: Optional[str]) -> list[dict]:
    """Load rows from an existing run log CSV, normalised to _COLUMNS."""
    if not prior_log_path or not os.path.exists(prior_log_path):
        return []
    try:
        with open(prior_log_path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            return [{col: row.get(col, '') for col in _COLUMNS} for row in reader]
    except Exception:
        return []   # corrupt prior log — start fresh


def append_run_log_pass1(
    output_path: str,
    prior_log_path: Optional[str],
    timestamp: str,
    prepared_by: str,
    property_name: str,
    period: str,
    je_count: int,
    je_total_dollars: float,
    close_tracker_complete: bool = False,
) -> str:
    """
    Append a Pass 1 row to GA_Run_Log.csv.

    Pass 1 captures JE processing metrics but no workpaper QC counts.

    Args:
        output_path:            Destination path for the updated CSV.
        prior_log_path:         Optional path to prior-period GA_Run_Log.csv.
        timestamp:              ISO-style string: 'YYYY-MM-DD HH:MM:SS'.
        prepared_by:            Name of the preparer (e.g. 'Ryan Walsh').
        property_name:          Property display name (e.g. 'Revolution Labs').
        period:                 Close period string (e.g. 'Jan-2026').
        je_count:               Number of journal entries processed.
        je_total_dollars:       Sum of absolute JE amounts (gross activity).
        close_tracker_complete: Whether all 9 close tracker steps are done.

    Returns:
        output_path
    """
    existing_rows = _read_prior(prior_log_path)

    new_row = {
        'timestamp':              timestamp,
        'pass_number':            '1',
        'prepared_by':            prepared_by or 'Ryan Walsh',
        'property':               property_name or 'Revolution Labs',
        'period':                 period,
        'files_generated':        '',
        'qc_checks_passed':       '',
        'qc_checks_failed':       '',
        'je_count':               str(je_count),
        'je_total_dollars':       f'{je_total_dollars:,.2f}',
        'close_tracker_complete': 'Yes' if close_tracker_complete else 'No',
    }
    existing_rows.append(new_row)

    with open(output_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=_COLUMNS)
        writer.writeheader()
        writer.writerows(existing_rows)

    return output_path


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
    close_tracker_complete: bool = False,
) -> str:
    """
    Append a Pass 2 row to GA_Run_Log.csv.

    If prior_log_path is a valid CSV, its rows are carried forward and the new
    row is appended at the bottom.  Otherwise a fresh file is created.

    Args:
        output_path:            Destination path for the updated CSV.
        prior_log_path:         Optional path to prior-period GA_Run_Log.csv.
        timestamp:              ISO-style string: 'YYYY-MM-DD HH:MM:SS'.
        prepared_by:            Name of the preparer (e.g. 'Ryan Walsh').
        property_name:          Property display name (e.g. 'Revolution Labs').
        period:                 Close period string (e.g. 'Jan-2026').
        files_generated:        List of output file names included in the ZIP.
        qc_checks_passed:       Number of QC checks that passed.
        qc_checks_failed:       Number of QC checks that flagged or failed.
        close_tracker_complete: Whether all 9 close tracker steps are done.

    Returns:
        output_path
    """
    existing_rows = _read_prior(prior_log_path)

    new_row = {
        'timestamp':              timestamp,
        'pass_number':            '2',
        'prepared_by':            prepared_by or 'Ryan Walsh',
        'property':               property_name or 'Revolution Labs',
        'period':                 period,
        'files_generated':        '; '.join(files_generated),
        'qc_checks_passed':       str(qc_checks_passed),
        'qc_checks_failed':       str(qc_checks_failed),
        'je_count':               '',
        'je_total_dollars':       '',
        'close_tracker_complete': 'Yes' if close_tracker_complete else 'No',
    }
    existing_rows.append(new_row)

    with open(output_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=_COLUMNS)
        writer.writeheader()
        writer.writerows(existing_rows)

    return output_path
