"""
GL History Analyzer — Recurring-Vendor & Budget-Gap Review
==============================================================
Analyzes a multi-month (target: 12-month) Yardi GL export, uploadable
anytime from Property Setup (not gated to a one-time onboarding step — new
lagged-billing patterns can surface well after a property is onboarded, as
637150 Reimbursable Payroll did). Produces two INFORMATIONAL summaries:

  analyze_recurring_vendors() — which expense accounts/vendors bill on a
    recurring-but-not-monthly cadence (quarterly, semi-annual, annual) — the
    kind of thing a reviewer would want to keep an eye out for as a One-Off
    Accruals or named sub-line accrual candidate.

  compare_budget_to_history() — for accounts with no automated Layer 3
    fallback (layer3_exclude_accounts), whether 12 months of real GL
    activity falls materially short of the Kardin budget — added 2026-09-08
    after 637150's Reimbursable Payroll ran undetected for months (Ryan's
    question: "is there a way for this to read the 12 month prior GL to see
    if the app should accrue based on budget for certain line items?").

  find_unreversed_accruals() — always-reversing pipeline accruals (see
    _ALWAYS_REVERSING_REFERENCES) with no matching reversal anywhere else in
    the file, excluding the file's own most recent period (normal timing,
    not a problem yet) — added 2026-09-15 at Ryan's request, a candidate for
    a stuck/orphaned balance that should have cleared but didn't.

Neither function auto-fills anything (default_accruals pre-population was
removed 2026-08-23 for exactly that reason — this is read-only review
material, not a seed list, and never a trigger to re-enable a whole-account
Layer 3 average for an excluded account).

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
from datetime import date, datetime
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


@dataclass
class BudgetGapCandidate:
    """One account where real GL net activity falls short of its (prorated)
    Kardin budget by a material amount — a candidate for a targeted named
    sub-line accrual (see accrual_entry_generator.NAMED_SUBLINE_ACCRUALS),
    not for re-enabling a whole-account Layer 3 average."""
    account_code:     str
    account_name:     str
    months_covered:   int         # distinct calendar months found in the file
    budget_prorated:  float       # Kardin annual total (minus covered items),
                                   # scaled to months_covered/12 for a fair
                                   # comparison against a partial-year file
    actual_net:       float       # acct.net_change over the file's span —
                                   # debits minus credits, so a temporary
                                   # accrual and its own reversal correctly
                                   # cancel to $0 instead of both counting
                                   # as separate real spend
    gap:              float       # budget_prorated - actual_net
    kardin_lines:     List[Dict]        # the (uncovered) Kardin rows behind budget_prorated
    vendor_patterns:  List[VendorPattern]  # this account's own vendor/cadence detail, for review context only


def compare_budget_to_history(
    gl_result,
    kardin_records: List[Dict],
    tracked_account_codes: List[str],
    already_covered_keywords: Optional[Dict[str, List[str]]] = None,
    min_gap_dollars: float = 2500.0,
    min_gap_pct: float = 0.15,
) -> List[BudgetGapCandidate]:
    """
    Compare 12 months of real GL activity against the Kardin budget, for
    accounts that have no automated Layer 3 fallback today (pass
    property_config.layer3_exclude_accounts as tracked_account_codes) —
    the exact situation that let 637150's Reimbursable Payroll run
    undetected for months before a manual GL review (2026-09-03) caught it.

    This does NOT suggest specific dollar amounts to accrue or auto-fill
    anything — same read-only philosophy as analyze_recurring_vendors above.
    It flags "the account looks materially under-covered vs. its budget" and
    hands the reviewer the real vendor/cadence detail (what's actually in the
    GL) alongside the Kardin line items (what's budgeted) so a HUMAN makes
    the connection — the reviewer, not this function, decides whether that
    means a new named sub-line accrual is worth building.

    Why account-level, not vendor-level: matching a Kardin line's wording to
    the right GL transactions requires a human — Kardin's "JLL XM" and the
    GL's "Reimbursable Payroll"/"Experience Mgt" share no common text at all,
    so no reliable keyword-matching exists between the two sides in general.

    Args:
        gl_result:               GLParseResult from a 12-month GL export.
        kardin_records:          Parsed Kardin budget rows (parsers.kardin_budget.parse()).
        tracked_account_codes:   Account codes to check — normally
                                 property_config.layer3_exclude_accounts,
                                 since those are exactly the accounts Layer 3
                                 already can't help with. Checking every
                                 account here would just re-flag accounts
                                 Layer 1-4 already handle correctly every
                                 month — the "no duplicates" requirement
                                 confirmed with Ryan 2026-09-08.
        already_covered_keywords: {account_code: [kardin_keyword, ...]} for
                                 items already handled by an existing
                                 NAMED_SUBLINE_ACCRUALS entry (e.g.
                                 {'637150': ['jll xm']}) — their budget $ are
                                 excluded from the gap so an already-fixed
                                 item doesn't keep getting flagged.
        min_gap_dollars/pct:     Only flag when the gap clears BOTH a dollar
                                 floor and a percentage floor — a small
                                 percentage of a huge budget, or a large
                                 percentage of a trivial budget, isn't worth
                                 a reviewer's time either way.

    Returns candidates sorted by gap size, largest first.
    """
    if not gl_result or not kardin_records or not tracked_account_codes:
        return []

    already_covered_keywords = already_covered_keywords or {}
    _tracked = {str(c).strip() for c in tracked_account_codes}

    _gl_accts = {
        str(getattr(a, 'account_code', '') or '').strip(): a
        for a in (getattr(gl_result, 'accounts', None) or [])
    }

    # Reuse the same vendor analysis for review-context display only (the
    # cadence/occurrence detail a reviewer wants to see) — NOT for the gap
    # math itself. Its total_amount sums abs(net_amount) per transaction,
    # which is right for showing "how big was each occurrence" but wrong for
    # "how much real economic activity happened": a temporary JLL accrual
    # and its own reversal both count as separate real spend that way,
    # rather than cancelling to $0 as they should. Confirmed on real data
    # 2026-09-08 — this masked 637150's own Reimbursable Payroll gap in
    # testing (abs-summed "actual" came out to 2x+ the entire annual
    # budget, when the account's real net_change was a small net credit).
    _all_patterns = analyze_recurring_vendors(
        gl_result, is_expense_fn=lambda code: str(code or '').strip() in _tracked
    )
    _patterns_by_code: Dict[str, list] = {}
    for p in _all_patterns:
        _patterns_by_code.setdefault(p.account_code, []).append(p)

    candidates: List[BudgetGapCandidate] = []
    for code in _tracked:
        _covered_kw = [kw.lower() for kw in already_covered_keywords.get(code, [])]
        _lines = [
            r for r in kardin_records
            if str(r.get('account_code', '') or '').strip() == code
            and not any(kw in (r.get('description', '') or '').lower() for kw in _covered_kw)
        ]
        if not _lines:
            continue
        budget_annual = sum(float(r.get('m_total', 0) or 0) for r in _lines)
        if budget_annual < 1:
            continue

        _acct = _gl_accts.get(code)
        if _acct is None:
            continue

        # Prorate the annual budget to however many distinct ACCOUNTING
        # periods (not raw transaction dates) this account has activity in —
        # comparing a partial-year upload against the full annual figure
        # would wildly overstate the gap. Uses each transaction's own
        # .period (Yardi's "Feb-2026"-style accounting period), not .date —
        # a late-arriving invoice can carry an OLDER invoice date while still
        # posting to the CURRENT accounting period (confirmed on real data:
        # a 12/18/2025-dated invoice recorded in a Jan-Jun 2026 export), so
        # counting raw dates overstated months_covered to 12 even for a file
        # that only actually spans 6 real accounting periods. Falls back to
        # 12 (no scaling) if no period text parses, so a $0-actual account
        # still compares against its full annual budget rather than nothing.
        _months = set()
        for t in (getattr(_acct, 'transactions', None) or []):
            _per = str(getattr(t, 'period', '') or '').strip()
            try:
                _pm = datetime.strptime(_per, '%b-%Y')
                _months.add((_pm.year, _pm.month))
            except ValueError:
                continue
        months_covered = min(12, len(_months)) or 12
        budget_prorated = round(budget_annual * (months_covered / 12.0), 2)

        actual_net = round(float(getattr(_acct, 'net_change', 0) or 0), 2)

        gap = round(budget_prorated - actual_net, 2)
        if gap <= 0:
            continue
        if gap < min_gap_dollars or gap < (budget_prorated * min_gap_pct):
            continue

        _patterns = _patterns_by_code.get(code, [])
        _name = getattr(_acct, 'account_name', '') or str(_lines[0].get('account_name', '') or '')
        candidates.append(BudgetGapCandidate(
            account_code=code,
            account_name=_name,
            months_covered=months_covered,
            budget_prorated=budget_prorated,
            actual_net=actual_net,
            gap=gap,
            kardin_lines=_lines,
            vendor_patterns=_patterns,
        ))

    candidates.sort(key=lambda c: c.gap, reverse=True)
    return candidates


# ── Unreversed accrual check ────────────────────────────────────────────────

# Reference tags the pipeline generates as ALWAYS-reversing monthly accruals
# (reverse_next_month = -1, either explicitly or via generate_etl_csv's
# batch-level heuristic — every one of these credits 213100 Accrued Expenses
# by design). Confirmed by reading accrual_entry_generator.py and
# management_fee.py directly rather than assuming (2026-09-15) — deliberately
# excludes reference tags that are intentionally PERMANENT (prepaid_
# amortization, PPD-RECLASS, INS-ESCROW — credits 135110, not 213100, so the
# batch heuristic already makes it permanent, INTERCO-RECODE, Post-Close JEs)
# or user-controlled (MANUAL — the One-Off Accruals table's own Auto-Reverse
# checkbox decides that one, not this check). Getting this list wrong in
# either direction defeats the point: too broad flags correctly-permanent
# entries and trains a reviewer to ignore it; too narrow misses real stuck
# balances.
_ALWAYS_REVERSING_REFERENCES = frozenset({
    'RECURRING', 'INV-PRORATION', 'BONUS', 'NAMED-ACCR',
    'ELEC-REIMB', 'ELEC-ACCRUAL', 'METER-READ',
    'MGMT-FEE-JLL', 'MGMT-FEE-GRP', 'MGMT-CATCHUP',
})

_REVERSAL_TEXT_RE = re.compile(r'reversal\s+of\s+([Jj]-\d+)', re.IGNORECASE)


@dataclass
class UnreversedAccrual:
    """One pipeline-generated accrual that should have auto-reversed by now
    (per its reference tag) but no matching reversal was found anywhere else
    in the uploaded file — a candidate for a stuck/orphaned balance, not
    just normal accrual timing."""
    account_code:  str
    account_name:  str
    je_number:     str      # Yardi control number, e.g. 'J-22545'
    reference:     str      # the pipeline reference tag, e.g. 'RECURRING'
    period:        str      # the accrual's own accounting period
    amount:        float
    description:   str


def find_unreversed_accruals(gl_result) -> List[UnreversedAccrual]:
    """
    Flags always-reversing pipeline accruals (see _ALWAYS_REVERSING_REFERENCES)
    with no matching reversal anywhere else in the uploaded GL history —
    added 2026-09-15 at Ryan's request. Excludes accruals from the file's
    OWN most recent accounting period, since those haven't had a chance to
    reverse yet (Yardi reverses on the 1st of the FOLLOWING period, which may
    be outside the file's window) — that's normal timing, not a problem.
    Anything older that never found its reversal has run out of legitimate
    timing excuses.

    Matching works the same way _reversal_j_debits() and
    _subline_item_activity() already do elsewhere in this codebase: Yardi's
    own auto-reversal doesn't carry the pipeline's reference tag (it's
    Yardi-generated, not pipeline-generated) — it carries ":Reversal of
    J-####" in its own description/remarks instead. So this searches for
    that text referencing the specific accrual's control number, not for a
    second occurrence of the same reference tag.

    Informational only — same read-only philosophy as the other two
    functions in this module. Does not attempt to auto-correct anything.
    """
    results: List[UnreversedAccrual] = []
    if not gl_result or not hasattr(gl_result, 'accounts'):
        return results

    # Determine the file's most recent accounting period so its own accruals
    # (not yet due to reverse) are excluded — same period-based approach as
    # compare_budget_to_history's months_covered, not raw transaction dates.
    _latest_period: Optional[datetime] = None
    for _acct in gl_result.accounts:
        for _t in (getattr(_acct, 'transactions', None) or []):
            _per = str(getattr(_t, 'period', '') or '').strip()
            try:
                _pm = datetime.strptime(_per, '%b-%Y')
            except ValueError:
                continue
            if _latest_period is None or _pm > _latest_period:
                _latest_period = _pm

    for acct in gl_result.accounts:
        code = str(getattr(acct, 'account_code', '') or '').strip()
        name = str(getattr(acct, 'account_name', '') or '').strip()
        txns = getattr(acct, 'transactions', None) or []

        # Build the full account-level text blob once so each candidate's
        # reversal search doesn't re-scan the transaction list from scratch.
        _all_text = ' '.join(
            f"{getattr(t, 'description', '') or ''} {getattr(t, 'remarks', '') or ''}"
            for t in txns
        ).lower()

        for t in txns:
            ref = str(getattr(t, 'reference', '') or '').strip().upper()
            if ref not in _ALWAYS_REVERSING_REFERENCES:
                continue
            debit = float(getattr(t, 'debit', 0) or 0)
            if debit <= 0.01:
                continue
            control = str(getattr(t, 'control', '') or '').strip()
            if not control:
                continue
            # Skip Yardi's own reversal lines — they wouldn't carry our
            # reference tag in the first place, but guard against a
            # coincidental match on the description text anyway.
            combined = f"{getattr(t, 'description', '') or ''} {getattr(t, 'remarks', '') or ''}".lower()
            if _REVERSAL_TEXT_RE.search(combined):
                continue

            _per = str(getattr(t, 'period', '') or '').strip()
            try:
                _pm = datetime.strptime(_per, '%b-%Y')
            except ValueError:
                _pm = None
            if _latest_period is not None and _pm == _latest_period:
                continue   # this period's own accrual — hasn't had a chance to reverse yet

            if f'reversal of {control.lower()}' in _all_text:
                continue   # matching reversal found — properly cleared

            results.append(UnreversedAccrual(
                account_code=code,
                account_name=name,
                je_number=control,
                reference=ref,
                period=_per,
                amount=round(debit, 2),
                description=str(getattr(t, 'remarks', '') or getattr(t, 'description', '') or '').strip(),
            ))

    results.sort(key=lambda u: (u.account_code, u.period))
    return results
