"""
GL History Analyzer — Onboarding Recurring-Vendor Summary
==============================================================
Analyzes a multi-month (target: 12-month) Yardi GL export uploaded at
property onboarding time and produces an INFORMATIONAL summary of which
expense accounts/vendors bill on a recurring-but-not-monthly cadence
(quarterly, semi-annual, annual) — the kind of thing a PM or accounting
team would want to keep an eye out for as a One-Off Accruals candidate,
without the pipeline auto-filling anything (default_accruals pre-population
was removed 2026-08-23 for exactly that reason — this is read-only review
material, not a seed list).

Deliberately does NOT modify yardi_gl.py's header/period parsing — a ranged
("Jan-2026 to Dec-2026"-style) header isn't something we've seen a real
sample of yet, and every transaction already carries its own date
independent of the header, which is all this analysis actually needs. Feed
it the GLParseResult from the EXISTING parse_gl() unchanged.

⚠️ UNVERIFIED AGAINST A REAL MULTI-MONTH EXPORT as of 2026-08-23 — Ryan
didn't have a 12-month sample on hand yet. Cadence classification in
particular (quarterly/semi-annual/annual thresholds) is a best-guess and
should be checked against a real file before trusting its output blindly.
Transaction-level parsing (dates/descriptions/amounts) reuses the same
parse_gl() already running in production every month, so that part is on
solid ground regardless of how many months the file spans.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass
class VendorPattern:
    """One recurring (or one-time) vendor/account pattern found in the GL history."""
    account_code:    str
    account_name:    str
    vendor:          str
    occurrences:     int
    months_seen:     List[str]        # e.g. ['01/2026', '04/2026', '07/2026', '10/2026']
    avg_amount:      float
    total_amount:    float
    cadence:         str              # 'Monthly' | 'Quarterly' | 'Semi-Annual' | 'Annual/One-time' | 'Irregular'


_VENDOR_CODE_RE = re.compile(r'\s*\(v\d+\)\s*$', re.IGNORECASE)

# Confirmed on a real GL export 2026-08-23: a transaction that later got
# reversed can carry "Reversed by J-XXXXX" as a suffix on its OWN line (not
# just "Reversal of J-XXXXX" on the reversal's own line) — e.g.
# "Eversource :Reversed by J-22800". Strip it off whatever's left after the
# vendor-code cleanup so it doesn't fragment one real vendor into two
# unrelated-looking groups ("Eversource" vs "Eversource :Reversed by...").
_REVERSAL_SUFFIX_RE = re.compile(
    r'\s*[:\-–]?\s*reversed\s+by\s+[Jj]-\d+\s*$', re.IGNORECASE
)


def _extract_vendor(txn) -> str:
    """
    Pull a clean vendor name off a GL transaction. Yardi AP transactions
    typically carry 'Vendor Name (vXXXXXXX)' in the description field —
    strip the trailing vendor-code parenthetical and any "Reversed by
    J-XXXXX" annotation. Falls back to remarks, then a blank string, if
    description doesn't look like a vendor line.
    """
    desc = str(getattr(txn, 'description', '') or '').strip()
    cleaned = _REVERSAL_SUFFIX_RE.sub('', _VENDOR_CODE_RE.sub('', desc)).strip()
    if cleaned:
        return cleaned
    return str(getattr(txn, 'remarks', '') or '').strip()


def _classify_cadence(months_seen: List[date]) -> str:
    """
    Classify a vendor/account's billing cadence from the DISTINCT months it
    appears in, by the average gap between consecutive occurrences.

    Thresholds are a best guess pending a real multi-month sample:
      ~1 month apart   -> Monthly
      ~3 months apart  -> Quarterly
      ~6 months apart  -> Semi-Annual
      1 occurrence, or >=10 months apart -> Annual/One-time
      anything else    -> Irregular (doesn't fit a clean recurring pattern)
    """
    if len(months_seen) <= 1:
        return 'Annual/One-time'
    _sorted = sorted(months_seen)
    _gaps_months = [
        (b.year - a.year) * 12 + (b.month - a.month)
        for a, b in zip(_sorted[:-1], _sorted[1:])
    ]
    _avg_gap = sum(_gaps_months) / len(_gaps_months)
    _spread = max(_gaps_months) - min(_gaps_months) if len(_gaps_months) > 1 else 0
    if _spread > 2:
        return 'Irregular'
    if _avg_gap <= 1.5:
        return 'Monthly'
    if _avg_gap <= 3.5:
        return 'Quarterly'
    if _avg_gap <= 7.0:
        return 'Semi-Annual'
    return 'Annual/One-time'


def analyze_recurring_vendors(
    gl_result,
    is_expense_fn=None,
    min_occurrences: int = 1,
) -> List[VendorPattern]:
    """
    Group a parsed GL's transactions by (account_code, vendor) and classify
    each group's billing cadence. Expense accounts only (6/7/8xxxxx by
    default) — this is meant to surface One-Off Accruals review candidates,
    not balance sheet or revenue activity.

    Args:
        gl_result:       GLParseResult from parsers.yardi_gl.parse_gl() —
                          works on any period span; each transaction's own
                          date drives this analysis, not the file's header.
        is_expense_fn:   Optional callable(account_code) -> bool, e.g.
                          property_config.is_expense_account with a specific
                          cfg bound in. Defaults to the standard 6/7/8xxxxx
                          Yardi convention when not provided.
        min_occurrences: Skip patterns seen fewer than this many times —
                          default 1 keeps everything, including true one-offs,
                          since those are exactly what a reviewer wants to see.

    Returns a list of VendorPattern, sorted by account_code then vendor.
    Excludes patterns where a usable date couldn't be read off any transaction.
    """
    if is_expense_fn is None:
        is_expense_fn = lambda code: str(code or '').strip()[:1] in ('6', '7', '8')

    # Reuse the same auto-reversal detector already trusted in production
    # (bs_workpaper_generator.py) rather than re-implementing it — a
    # transaction that got estimated-then-reversed the following month
    # (the standard accrual cycle per CLAUDE.md) isn't a second real
    # occurrence of a vendor charge; counting it would fabricate a
    # recurring pattern that's really just one accrual being unwound.
    from bs_workpaper_generator import _is_reversal_txn

    groups: Dict[tuple, list] = {}
    for acct in getattr(gl_result, 'accounts', None) or []:
        code = str(getattr(acct, 'account_code', '') or '').strip()
        if not code or not is_expense_fn(code):
            continue
        name = str(getattr(acct, 'account_name', '') or '').strip()
        for txn in getattr(acct, 'transactions', None) or []:
            if _is_reversal_txn(txn):
                continue
            txn_date = getattr(txn, 'date', None)
            if txn_date is None:
                continue
            vendor = _extract_vendor(txn) or '(no vendor on file)'
            key = (code, vendor)
            groups.setdefault(key, {'name': name, 'txns': []})
            groups[key]['txns'].append(txn)

    patterns: List[VendorPattern] = []
    for (code, vendor), info in groups.items():
        txns = info['txns']
        if len(txns) < min_occurrences:
            continue
        # Dedupe by MONTH, not exact date — _classify_cadence measures the
        # gap in months between DISTINCT MONTHS of activity (per its own
        # docstring). Deduping by exact date instead let two same-month
        # transactions (e.g. an invoice plus a same-month credit/adjustment,
        # common for utility true-ups) inject a spurious 0-month gap,
        # dragging the average down and able to flip a genuinely quarterly
        # vendor to 'Monthly' or 'Irregular'. Confirmed as a real bug in
        # review before push 2026-08-23.
        _dates = sorted({date(t.date.year, t.date.month, 1)
                         for t in txns if getattr(t, 'date', None)})
        _amounts = [abs(float(getattr(t, 'net_amount', 0) or 0)) for t in txns]
        patterns.append(VendorPattern(
            account_code=code,
            account_name=info['name'],
            vendor=vendor,
            occurrences=len(txns),
            months_seen=[d.strftime('%m/%Y') for d in _dates],
            avg_amount=round(sum(_amounts) / len(_amounts), 2) if _amounts else 0.0,
            total_amount=round(sum(_amounts), 2),
            cadence=_classify_cadence(_dates),
        ))

    patterns.sort(key=lambda p: (p.account_code, p.vendor))
    return patterns
