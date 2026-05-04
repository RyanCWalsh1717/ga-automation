"""
Parser for Yardi 12-Month Income Statement export (revlabpm).

Expected layout:
  Row 1: "Revolution Labs Owner, LLC (revlabpm)"
  Row 2: "Statement (12 months)"
  Row 3: "Period = Feb 2025-Jan 2026"
  Row 4: "Book = Accrual"
  Row 5: [None, None, 'Feb 2025', 'Mar 2025', ..., 'Jan 2026', 'Total']
  Row 6+: [account_code, account_name (indented), val_month1, ..., val_month12, total]

Returns T12Result with:
  - months       : ['Feb 2025', ..., 'Jan 2026']
  - month_nums   : [2, 3, ..., 12, 1]
  - accounts     : {account_code: {month_num: float}}
  - account_names: {account_code: str}
  - prior_month() / get_month() helpers
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List

import openpyxl

# ── Month label → calendar number ────────────────────────────────
_MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3,  'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7,  'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
}


def _parse_month_num(label: str) -> int:
    """'Feb 2025' → 2, 'Jan 2026' → 1, anything else → 0."""
    m = re.match(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', str(label or ''))
    return _MONTH_MAP[m.group(1)] if m else 0


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _is_subtotal_or_header(code: str) -> bool:
    """
    Skip section headers (xxxxx000) and subtotal rows (xxxxx999).
    Real leaf accounts have codes that don't end in 3+ identical digits.
    """
    c = str(code).strip()
    return (
        c.endswith('999')
        or c.endswith('9999')
        or c.endswith('000')
        or c.endswith('0000')
    )


# ── Result dataclass ──────────────────────────────────────────────

@dataclass
class T12Result:
    property_name: str
    period_start: str                   # e.g. "Feb 2025"
    period_end:   str                   # e.g. "Jan 2026"
    months:       List[str]             # ["Feb 2025", ..., "Jan 2026"]
    month_nums:   List[int]             # [2, 3, ..., 12, 1]
    accounts:     Dict[str, Dict[int, float]] = field(default_factory=dict)
    account_names: Dict[str, str]            = field(default_factory=dict)

    # ── Lookup helpers ──────────────────────────────────────────

    def get_month(self, account_code: str, month_num: int) -> float:
        """Return PTD actual for account_code in the given calendar month, or 0."""
        return self.accounts.get(account_code, {}).get(month_num, 0.0)

    def prior_month(self, account_code: str, current_month: int) -> float:
        """
        Return the actual for the calendar month immediately before current_month.
        Wraps: January's prior month is December.
        """
        prior = 12 if current_month == 1 else current_month - 1
        return self.get_month(account_code, prior)

    def has_prior_month_data(self, current_month: int) -> bool:
        """True if the T12 contains data for the month before current_month."""
        prior = 12 if current_month == 1 else current_month - 1
        return prior in self.month_nums

    def trailing_avg(self, account_code: str, n_months: int = 11,
                     exclude_month: int = None) -> float:
        """
        Average of the most-recent n_months of actuals (default 11, i.e. excluding
        the current period).  Months with $0 activity are excluded from the average.
        Returns 0 if no non-zero months found.
        """
        eligible = [m for m in self.month_nums if m != exclude_month]
        recent   = eligible[-n_months:]
        vals     = [self.get_month(account_code, m)
                    for m in recent
                    if self.get_month(account_code, m) != 0.0]
        return sum(vals) / len(vals) if vals else 0.0


# ── Parser ────────────────────────────────────────────────────────

def parse(filepath: str) -> T12Result:
    """
    Parse a Yardi 12-Month Statement .xlsx file.
    Returns T12Result.  Raises ValueError on unrecognised format.
    """
    wb = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        raise ValueError('T12 file is empty.')

    # ── Property name ──────────────────────────────────────
    property_name = str(rows[0][0] or '').strip()

    # ── Period string ("Period = Feb 2025-Jan 2026") ───────
    period_start = period_end = ''
    for row in rows[:8]:
        cell = str(row[0] or '')
        m = re.search(r'Period\s*=\s*(\w+\s+\d{4})\s*-\s*(\w+\s+\d{4})', cell, re.IGNORECASE)
        if m:
            period_start = m.group(1).strip()
            period_end   = m.group(2).strip()
            break

    # ── Find month-header row ──────────────────────────────
    # Row where ≥10 cols contain month-label strings (e.g. 'Feb 2025').
    header_row_idx      = None
    month_col_indices: List[tuple] = []   # [(col_idx, month_num, label), ...]

    for i, row in enumerate(rows):
        hits = []
        for j, val in enumerate(row):
            if j < 2:
                continue            # skip code/name cols
            mnum = _parse_month_num(val)
            if mnum:
                hits.append((j, mnum, str(val).strip()))
        if len(hits) >= 10:
            header_row_idx    = i
            month_col_indices = hits
            break

    if not month_col_indices:
        raise ValueError(
            'Could not locate month-header row in T12 statement. '
            'Expected ≥10 columns with labels like "Feb 2025".'
        )

    months     = [lbl for _, _, lbl in month_col_indices]
    month_nums = [mn  for _, mn,  _ in month_col_indices]

    # ── Data rows ──────────────────────────────────────────
    accounts:      Dict[str, Dict[int, float]] = {}
    account_names: Dict[str, str]              = {}

    for row in rows[header_row_idx + 1:]:
        code = str(row[0] or '').strip()

        # Must be a 6-digit numeric code
        if not re.match(r'^\d{6}$', code):
            continue

        # Skip subtotals and section headers
        if _is_subtotal_or_header(code):
            continue

        # Skip rows where all month columns are None (pure section headers
        # that happen to have a 6-digit code)
        has_any = any(
            row[col_idx] is not None
            for col_idx, _, _ in month_col_indices
            if col_idx < len(row)
        )
        if not has_any:
            continue

        name = str(row[1] or '').strip()  # strip leading indent spaces

        monthly: Dict[int, float] = {}
        for col_idx, mnum, _ in month_col_indices:
            val = row[col_idx] if col_idx < len(row) else None
            monthly[mnum] = _safe_float(val)

        accounts[code]      = monthly
        account_names[code] = name

    return T12Result(
        property_name=property_name,
        period_start=period_start,
        period_end=period_end,
        months=months,
        month_nums=month_nums,
        accounts=accounts,
        account_names=account_names,
    )
