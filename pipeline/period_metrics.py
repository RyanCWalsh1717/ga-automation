"""
Period Metrics — save and load key financial metrics per close period.

After each successful Pass 2 run, `save_metrics()` appends one JSON record
to `data/{property_code}/metrics.jsonl`.  The file grows one line per close
and becomes the source of truth for the cross-period trending dashboard.

Metrics captured per period:
  - Revenue (total 4xxxxx credit activity)
  - Expenses (total 5xxxxx–8xxxxx debit activity)
  - NOI  (revenue − expenses)
  - Management fee (637130 debit activity)
  - QC pass / warn / fail counts
  - Operating cash balance (GL 111100 ending)
  - DACA balance (GL 115100 ending)
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from property_config import is_expense_account


# ── Period-label helpers ──────────────────────────────────────

_MONTH_ORDER = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,  'May': 5,  'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
}

def _period_sort_key(period: str) -> tuple:
    """Return (year, month_num) for sorting period labels like 'Jan-2026'."""
    import re
    m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ](\d{4})', period)
    if m:
        return (int(m.group(2)), _MONTH_ORDER.get(m.group(1), 0))
    return (0, 0)


# ── GL extraction helpers ─────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _extract_gl_metrics(gl_data) -> dict:
    """
    Walk the GL accounts list and return aggregated financial metrics.
    Works with the GLParseResult dataclass produced by parsers/yardi_gl.py.
    """
    revenue   = 0.0
    expenses  = 0.0
    mgmt_fee  = 0.0
    op_cash   = 0.0
    daca      = 0.0

    if gl_data is None:
        return dict(revenue=0, expenses=0, noi=0, management_fee=0,
                    operating_cash=0, daca_balance=0)

    accounts = getattr(gl_data, 'accounts', []) or []
    for acct in accounts:
        code = str(getattr(acct, 'account_code', '') or '').strip()
        if not code:
            continue

        # GLAccount fields: net_change = total_debits - total_credits (MTD activity)
        #                   ending_balance = balance sheet ending balance
        # Fall back to total_debits - total_credits if net_change is not present.
        net_ch = _safe_float(
            getattr(acct, 'net_change', None)
            if getattr(acct, 'net_change', None) is not None
            else (getattr(acct, 'total_debits', 0) - getattr(acct, 'total_credits', 0))
        )
        end = _safe_float(getattr(acct, 'ending_balance', None)
                          or getattr(acct, 'balance', None))

        first = code[0] if code else ''

        if first == '4':
            # Revenue accounts: credits exceed debits → net_change is negative
            revenue += abs(net_ch)
        elif is_expense_account(code):
            # Property-level expense accounts only (6xxxxx/8xxxxx by default,
            # per-property override via coa_expense_prefixes) — a raw
            # first-digit check of ('5','6','7','8') let 5xxxxx (entity-level
            # company revenue) and 7xxxxx (corporate expense) leak into NOI
            # trending, contrary to the documented COA convention used
            # correctly elsewhere in the pipeline.
            expenses += abs(net_ch)
            if code == '637130':
                mgmt_fee += abs(net_ch)
        elif code == '111100':
            op_cash = end
        elif code == '115100':
            daca = end

    noi = revenue - expenses
    return dict(
        revenue=round(revenue, 2),
        expenses=round(expenses, 2),
        noi=round(noi, 2),
        management_fee=round(mgmt_fee, 2),
        operating_cash=round(op_cash, 2),
        daca_balance=round(daca, 2),
    )


def _extract_qc_metrics(qc_report) -> dict:
    """Extract pass/warn/fail counts from a QC report object or list."""
    if qc_report is None:
        return dict(qc_pass=0, qc_warn=0, qc_fail=0, qc_overall='unknown')

    checks = []
    if isinstance(qc_report, list):
        checks = qc_report
    elif hasattr(qc_report, 'checks'):
        checks = qc_report.checks or []
    elif hasattr(qc_report, '__iter__'):
        checks = list(qc_report)

    n_pass = sum(1 for c in checks if getattr(c, 'status', '') == 'pass')
    n_warn = sum(1 for c in checks if getattr(c, 'status', '') in ('warn', 'warning'))
    n_fail = sum(1 for c in checks if getattr(c, 'status', '') in ('fail', 'error'))

    overall = 'fail' if n_fail else ('warn' if n_warn else 'pass')
    return dict(qc_pass=n_pass, qc_warn=n_warn, qc_fail=n_fail, qc_overall=overall)


def _extract_fee_metrics(fee_result) -> dict:
    """Extract management fee amount from the fee result object."""
    if fee_result is None:
        return dict(fee_amount=0.0, fee_basis=0.0)
    try:
        amount = _safe_float(getattr(fee_result, 'total_fee', None)
                             or getattr(fee_result, 'fee_amount', None))
        basis  = _safe_float(getattr(fee_result, 'cash_received', None)
                             or getattr(fee_result, 'basis', None))
        return dict(fee_amount=round(amount, 2), fee_basis=round(basis, 2))
    except Exception:
        return dict(fee_amount=0.0, fee_basis=0.0)


# ── Public API ────────────────────────────────────────────────

def save_metrics(
    data_dir: str,
    property_code: str,
    period: str,
    property_name: str = '',
    gl_data=None,
    qc_report=None,
    fee_result=None,
    extra: Optional[Dict] = None,
) -> str:
    """
    Append one period's metrics to data/{property_code}/metrics.jsonl.

    Args:
        data_dir:       Path to the repo's data/ directory
        property_code:  Yardi property code (e.g. 'revlabspm')
        period:         Close period label (e.g. 'Apr-2026')
        property_name:  Human-readable property name
        gl_data:        GLParseResult from engine_result.parsed['gl']
        qc_report:      QC report object / list from pass2_output_files
        fee_result:     Fee result from pass2_output_files['fee_result']
        extra:          Optional dict of additional key/value pairs to store

    Returns:
        Path to the metrics file.
    """
    prop_dir = Path(data_dir) / property_code
    prop_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = prop_dir / 'metrics.jsonl'

    record: Dict[str, Any] = {
        'saved_at':      datetime.now().isoformat(),
        'period':        period,
        'property_code': property_code,
        'property_name': property_name,
    }
    record.update(_extract_gl_metrics(gl_data))
    record.update(_extract_qc_metrics(qc_report))
    record.update(_extract_fee_metrics(fee_result))

    if extra and isinstance(extra, dict):
        record.update(extra)

    # Only write if we don't already have a record for this period
    # (re-runs overwrite the existing record rather than duplicating)
    existing: List[Dict] = load_metrics_raw(data_dir, property_code)
    existing = [r for r in existing if r.get('period') != period]
    existing.append(record)

    with open(metrics_path, 'w', encoding='utf-8') as fh:
        for row in existing:
            fh.write(json.dumps(row) + '\n')

    return str(metrics_path)


def load_metrics_raw(data_dir: str, property_code: str) -> List[Dict]:
    """
    Load all metric records for a property, unsorted.
    Returns empty list if no metrics file exists yet.
    """
    path = Path(data_dir) / property_code / 'metrics.jsonl'
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def load_metrics(data_dir: str, property_code: str) -> List[Dict]:
    """
    Load metric records sorted chronologically by period.
    """
    records = load_metrics_raw(data_dir, property_code)
    return sorted(records, key=lambda r: _period_sort_key(r.get('period', '')))


def load_all_properties_metrics(data_dir: str) -> Dict[str, List[Dict]]:
    """
    Load metrics for every property that has a metrics.jsonl file.
    Returns {property_code: [sorted records]}.
    """
    result: Dict[str, List[Dict]] = {}
    data_path = Path(data_dir)
    if not data_path.exists():
        return result
    for prop_dir in data_path.iterdir():
        if not prop_dir.is_dir():
            continue
        metrics_file = prop_dir / 'metrics.jsonl'
        if metrics_file.exists():
            result[prop_dir.name] = load_metrics(data_dir, prop_dir.name)
    return result
