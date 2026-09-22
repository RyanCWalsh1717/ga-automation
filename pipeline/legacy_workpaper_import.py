"""
Legacy Workpaper Import — Onboarding Extract from a Prior Manual Workpaper
=============================================================================
For an EXISTING GRP asset being onboarded into this app (e.g. 25 & 40
Hartwell), the property's accounting history already lives in a large,
manually-built Excel workpaper (one tab per Balance Sheet account, built up
over years by JLL/GRP staff) rather than anything this pipeline generated.
This module reads that file and extracts what onboarding actually needs,
rather than trying to parse all ~80 tabs uniformly — confirmed against two
real 25 & 40 Hartwell workpapers (2026-09-22) that the tabs are NOT one
consistent format; only a subset are.

extract_trial_balance() -- every real workpaper sampled has one tab that is
    literally a pasted Yardi Trial Balance export (property header, "Period =
    ...", "Book = ... ; Tree = ysi_tb", Forward/Ending column groups) --
    exactly the format parsers/yardi_trial_balance.py already parses for the
    standalone monthly TB upload. This reuses that parser UNCHANGED, just
    pointed at the embedded tab instead of a separate file -- every account
    code, name, and balance on the property's real books, in one shot.

extract_prepaid_items() -- a SUBSET of the per-account tabs (confirmed on
    135150 Prepaid Other and 1310-140 PPD Operating Exp) use a clean,
    consistent itemized schedule: one row per prepaid item with its own G/L
    account, payment date/amount, service period, and months elapsed. This
    maps directly onto prepaid_ledger.generate_seed()'s own item shape, so
    onboarding can produce a real seed file from real history instead of
    Ryan retyping every open prepaid item by hand.

    Deliberately NOT attempted for every prepaid/escrow-flavored tab: RE Tax
    Analysis, the Insurance Escrow tabs, and Insurance Analysis all use
    different layouts (running ledgers or per-policy premium tracking, not
    an itemized amortization list) -- confirmed on the same real file. This
    isn't a gap: those accounts (135110, 135120, 639110, 639120, 641110,
    115xxx escrows) are already excluded from the generic prepaid ledger by
    _LEDGER_EXCLUDED_GL_ACCOUNTS in prepaid_ledger.py, since they're handled
    by dedicated RE-tax/insurance amortization logic instead. Extracting
    them here would produce items generate_seed() silently drops anyway.

Scope: GRP-managed assets only, per Ryan (2026-09-22) -- this whole
approach assumes the workpaper follows GRP's own template conventions
(the "Trial Balance " tab, the itemized-prepaid-schedule column headers).
A non-GRP-managed acquisition's historical workpaper (different PM company,
different template) is out of scope until we see a real sample.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple


# ── Trial Balance extraction (all BS + P&L accounts) ───────────────────────

_TB_SHEET_CANDIDATES = ('Trial Balance ', 'Trial Balance', 'TB', 'Trial Balance  ')


def extract_trial_balance(filepath: str):
    """
    Find and parse the embedded Trial Balance tab in a legacy workpaper.
    Returns (TBResult, sheet_name) or (None, None) if no matching tab/format
    was found -- callers should treat that as "this workpaper doesn't have
    a parseable TB tab", not as an empty result.
    """
    import openpyxl
    from parsers.yardi_trial_balance import parse as _parse_tb

    try:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        sheetnames = wb.sheetnames
        wb.close()
    except Exception:
        return None, None

    for candidate in _TB_SHEET_CANDIDATES:
        if candidate in sheetnames:
            try:
                result = _parse_tb(filepath, sheet_name=candidate)
                if result and result.accounts:
                    return result, candidate
            except Exception:
                continue

    # Fall back to any sheet literally named with "trial balance" in it,
    # in case the property's template uses different spacing/casing.
    for name in sheetnames:
        if 'trial balance' in name.lower() and name not in _TB_SHEET_CANDIDATES:
            try:
                result = _parse_tb(filepath, sheet_name=name)
                if result and result.accounts:
                    return result, name
            except Exception:
                continue

    return None, None


@dataclass
class CoaMismatch:
    account_code: str
    account_name: str
    reason: str   # 'not_on_coa' | 'name_mismatch'
    coa_name:     str = ''


def check_against_coa(tb_accounts, coa_codes: Optional[Dict[str, str]]) -> List[CoaMismatch]:
    """
    Flag every TB account code that isn't on the GRP shared Chart of
    Accounts -- the "hangover from another property" check. coa_codes is
    {account_code: account_name} from app._load_coa_codes(); None means no
    COA is on file (nothing to check against, so nothing is flagged).
    """
    if not coa_codes:
        return []
    mismatches: List[CoaMismatch] = []
    for acct in tb_accounts:
        code = str(getattr(acct, 'account_code', '') or '').strip()
        name = str(getattr(acct, 'account_name', '') or '').strip()
        if not code:
            continue
        if code not in coa_codes:
            mismatches.append(CoaMismatch(account_code=code, account_name=name, reason='not_on_coa'))
    return mismatches


# ── Itemized prepaid schedule extraction ────────────────────────────────────

# The real column signature, confirmed on 135150 Prepaid Other and 1310-140
# PPD Operating Exp: a two-row header where the row immediately above and
# the row containing 'Description' combine (column-wise) into these labels.
# Matched on the lower row alone (unique enough on its own) to avoid relying
# on the upper row's exact wrapping, which varies slightly between tabs.
_PREPAID_HEADER_TOKENS = ('description', 'account', 'date', 'amount', 'elapsed')


def _find_header_row(rows: List[tuple]) -> Optional[int]:
    for i, row in enumerate(rows):
        cells = [str(c).strip().lower() if c is not None else '' for c in row]
        if 'description' in cells and 'elapsed' in cells and 'amount' in cells:
            return i
    return None


def _col_index(cells: List[str], token: str) -> Optional[int]:
    for i, c in enumerate(cells):
        if c == token:
            return i
    return None


def _coerce_gl_code(v) -> str:
    if v is None:
        return ''
    if isinstance(v, (int, float)):
        return str(int(v))
    return str(v).strip()


def extract_prepaid_items(filepath: str, coa_codes: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Scan every tab in a legacy workpaper for the itemized prepaid-schedule
    layout and return items in the shape prepaid_ledger.generate_seed()
    expects. gl_account (the name) is filled from coa_codes when the code
    is recognized, since the schedule tabs themselves don't carry a name.
    """
    import openpyxl

    items: List[Dict[str, Any]] = []
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
    except Exception:
        return items

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = [list(r) for r in ws.iter_rows(values_only=True)]
        header_idx = _find_header_row(rows)
        if header_idx is None:
            continue

        cells = [str(c).strip().lower() if c is not None else '' for c in rows[header_idx]]
        idx_desc   = _col_index(cells, 'description')
        idx_acct   = _col_index(cells, 'account')
        idx_date   = _col_index(cells, 'date')
        idx_amount = _col_index(cells, 'amount')
        idx_period = _col_index(cells, 'covered')  # first 'covered' = Period Covered text
        idx_expmo  = _col_index(cells, 'month')     # 'Exp Per Month' -> last token 'month'
        idx_elapsed = _col_index(cells, 'elapsed')
        idx_balance = _col_index(cells, 'balance')  # 'Prepaid Balance' -- remaining $ as of this workpaper
        if idx_desc is None or idx_acct is None or idx_date is None or idx_amount is None:
            continue

        for row in rows[header_idx + 1:]:
            if all(c is None for c in row):
                continue
            desc = row[idx_desc] if idx_desc < len(row) else None
            if desc is None or str(desc).strip() == '':
                continue
            if str(desc).strip().lower().startswith('ending balance'):
                break  # footer row -- this tab's item list is done
            if str(desc).strip().upper() in ('NOTES:', 'TOTAL', 'GRAND TOTAL'):
                continue

            gl_code = _coerce_gl_code(row[idx_acct]) if idx_acct < len(row) else ''
            if not gl_code:
                continue

            # Fully-amortized legacy items (nothing left to release) have a
            # $0 Prepaid Balance -- skip them rather than seeding dead rows
            # that generate_seed() would carry forward as still-active.
            if idx_balance is not None and idx_balance < len(row):
                try:
                    remaining_balance = float(row[idx_balance] or 0)
                except (TypeError, ValueError):
                    remaining_balance = 0.0
                if abs(remaining_balance) < 0.01:
                    continue

            payment_date = row[idx_date] if idx_date < len(row) else None
            payment_amount = row[idx_amount] if idx_amount < len(row) else 0
            period_text = str(row[idx_period]).strip() if (idx_period is not None and idx_period < len(row) and row[idx_period]) else ''
            exp_per_month = row[idx_expmo] if (idx_expmo is not None and idx_expmo < len(row)) else 0
            months_elapsed = row[idx_elapsed] if (idx_elapsed is not None and idx_elapsed < len(row)) else 0

            s_start, s_end = _parse_period_range(period_text)

            try:
                monthly_amount = float(exp_per_month or 0)
            except (TypeError, ValueError):
                monthly_amount = 0.0
            try:
                total_amount = float(payment_amount or 0)
            except (TypeError, ValueError):
                total_amount = 0.0
            try:
                months_amortized = int(round(float(months_elapsed or 0)))
            except (TypeError, ValueError):
                months_amortized = 0

            items.append({
                'vendor':            str(desc).strip(),
                'description':       str(desc).strip(),
                'gl_account_number': gl_code,
                'gl_account':        (coa_codes or {}).get(gl_code, ''),
                'total_amount':      total_amount,
                'monthly_amount':    monthly_amount,
                'service_start':     s_start,
                'service_end':       s_end,
                'months_amortized':  months_amortized,
                'invoice_date':      payment_date if isinstance(payment_date, (date, datetime)) else None,
                '_source_sheet':     sheet_name,
            })

    return items


_PERIOD_RANGE_DOT_RE = re.compile(
    r'(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*(\d{1,2})\.(\d{1,2})\.(\d{2})'
)
_PERIOD_RANGE_SLASH_RE = re.compile(
    r'(\d{1,2})/(\d{2})\s*-\s*(\d{1,2})/(\d{2})'
)


def _parse_period_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    """
    Parse the 'Period Covered' free-text column, seen in two real formats:
      'MM.DD.YY-MM.DD.YY'  e.g. '11.01.25-10.01.26'
      'MM/YY-MM/YY'        e.g. '05/26-07/26' (day assumed: 1st .. last day)
    Returns (None, None) if the text doesn't match either -- caller falls
    back to service_start/end being unset, which generate_seed() tolerates
    (total_months then falls back to the raw months-covered column).
    """
    if not text:
        return None, None
    m = _PERIOD_RANGE_DOT_RE.match(text)
    if m:
        m1, d1, y1, m2, d2, y2 = (int(x) for x in m.groups())
        try:
            return date(2000 + y1, m1, d1), date(2000 + y2, m2, d2)
        except ValueError:
            return None, None
    m = _PERIOD_RANGE_SLASH_RE.match(text)
    if m:
        m1, y1, m2, y2 = (int(x) for x in m.groups())
        import calendar
        try:
            start = date(2000 + y1, m1, 1)
            end_day = calendar.monthrange(2000 + y2, m2)[1]
            end = date(2000 + y2, m2, end_day)
            return start, end
        except ValueError:
            return None, None
    return None, None
