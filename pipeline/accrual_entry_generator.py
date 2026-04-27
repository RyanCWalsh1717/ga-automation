"""
Accrual Entry Generator for GA Automation Pipeline
====================================================
Generates journal entries for accruals from three sources:
  Layer 1: Nexus pending invoices (AP-side accruals)
  Layer 2: Budget gap detection (accounts with budget but no GL activity)
  Layer 3: Historical pattern detection (recurring expenses from prior months)

Outputs:
  1. Yardi JE import file (Excel) — ready for direct upload
  2. Workpaper review schedule — DR/CR detail for review before posting

Each accrual generates a two-line entry:
  DR  [Expense GL Account]
  CR  211200 Accrued Expenses (standard accrual liability)
"""

import os
import re
import calendar
from collections import defaultdict
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional
from openpyxl import Workbook

from accounting_utils import _round
from property_config import is_expense_account
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side


# ── GL dedup utilities ──────────────────────────────────────

def _normalize_vendor(name: str) -> str:
    """
    Reduce a vendor name to a stable comparison key.

    Lowercases, strips punctuation, collapses whitespace, and takes the first
    24 characters.  This lets "OpenPath Security, Inc." match "Openpath" and
    "Stewart Title" match "Stewart Title Guaranty Co." without false positives
    on short generic words.

    Returns '' if the name is blank or consists only of punctuation/whitespace.
    """
    if not name:
        return ''
    key = re.sub(r'[^a-z0-9 ]', ' ', name.lower())
    key = ' '.join(key.split())   # collapse whitespace
    return key[:24].strip()


def _build_gl_invoice_lookup(gl_data) -> dict:
    """
    Build lookup structures to check if an invoice is already in GL.

    Returns a dict with three keys:
      'by_reference'    : {reference_str: [txns]}   — exact invoice-number match
      'by_control'      : {control_str:   [txns]}   — control-number substring match
      'by_vendor_amount': {(account_code, vendor_key, amount_cents): [txns]}
                          — secondary dedup when invoice number is absent;
                            amount_cents is int(round(debit * 100)) for expense debits
    """
    lookup = {'by_reference': {}, 'by_control': {}, 'by_vendor_amount': {}}
    if not gl_data or not hasattr(gl_data, 'all_transactions'):
        return lookup

    for txn in gl_data.all_transactions:
        ref = (txn.reference or '').strip()
        if ref:
            lookup['by_reference'].setdefault(ref, []).append(txn)
        ctrl = (txn.control or '').strip()
        if ctrl:
            lookup['by_control'].setdefault(ctrl, []).append(txn)

        # Vendor+amount index — only for expense debit postings (debit > 0)
        if txn.debit > 0:
            vendor_key = _normalize_vendor(txn.description or '')
            if vendor_key:
                amount_cents = int(round(txn.debit * 100))
                va_key = (str(txn.account_code).strip(), vendor_key, amount_cents)
                lookup['by_vendor_amount'].setdefault(va_key, []).append(txn)

    return lookup


def _is_invoice_in_gl(invoice_number: str, gl_lookup: dict) -> bool:
    """Check if an invoice number already appears in GL transactions,
    either as a direct reference match or as a substring of a control number."""
    if not invoice_number:
        return False
    inv = invoice_number.strip()
    if inv in gl_lookup['by_reference']:
        return True
    for ctrl in gl_lookup['by_control']:
        if inv in ctrl:
            return True
    return False


def _is_in_gl_by_vendor_amount(
    vendor: str, amount: float, account_code: str, gl_lookup: dict
) -> bool:
    """
    Secondary dedup: check whether an expense posting with matching vendor name
    and amount already exists in the GL for this account.

    Used only when the invoice number is absent (no reference field to match on).
    A $0.02 tolerance band is applied — we check ±2 cents — to absorb rounding
    differences between the AP system and GL.

    Strategy: vendor name normalization via _normalize_vendor(), then amount
    comparison by integer cents to avoid float equality traps.

    Returns True if a plausible match is found; False otherwise.
    """
    if not vendor or not amount or not account_code:
        return False
    vendor_key = _normalize_vendor(vendor)
    if not vendor_key:
        return False
    acct = str(account_code).strip()
    amount_cents = int(round(abs(amount) * 100))
    by_va = gl_lookup.get('by_vendor_amount', {})

    # Check exact match and ±2 cent tolerance
    for delta in (0, 1, -1, 2, -2):
        key = (acct, vendor_key, amount_cents + delta)
        if key in by_va:
            return True
    return False


# ── Constants ────────────────────────────────────────────────

AP_ACCRUAL_ACCOUNT    = '211200'
AP_ACCRUAL_NAME       = 'Accrued Expenses'

# Known periodic-billing contract accounts.
# The pipeline auto-detects the monthly portion via partial-contract coverage,
# but these accounts often carry quarterly or semi-annual billings that won't
# appear in the GL until the invoice arrives.  The UI surfaces these accounts
# with a supplement input so the reviewer can add the periodic amount on top
# of whatever the pipeline detected automatically.
#   billing_cycle: 'monthly' | 'quarterly' | 'semi-annual'
PERIODIC_CONTRACT_ACCOUNTS: dict = {
    '617110': {'label': 'HVAC Contract',       'billing_cycle': 'quarterly'},
    '619120': {'label': 'PPM Water Treatment', 'billing_cycle': 'monthly'},
    '627230': {'label': 'Fire / Life Safety',  'billing_cycle': 'monthly'},
}
# Tenant sub-metered utility recovery accounts.
# Each month the meter read company provides per-tenant consumption data.
# The property manager posts a JE:
#   DR 131100  Accounts Receivable - Control  (per tenant)
#   CR 440500  Recovery - Electricity         (electric portion)
#   CR 440700  Recovery - Misc Utilities      (gas portion, reclassed from 440500)
#
# If the meter read JE hasn't been posted yet at close, the pipeline accrues
# the budget amount so NOI is not understated. When the actual meter read
# data is available, the sidebar overrides with per-tenant actual amounts.
TENANT_UTILITY_AR_ACCOUNT   = '131100'
TENANT_UTILITY_AR_NAME      = 'Accounts Receivable - Control'
TENANT_UTILITY_ACCOUNTS: dict = {
    '440500': {'label': 'Tenant Electric Recovery',     'budget_key': '440500'},
    '440700': {'label': 'Tenant Gas Recovery',          'budget_key': '440700'},
}

PREPAID_ASSET_ACCOUNT = '130000'
PREPAID_ASSET_NAME    = 'Prepaid Expenses'

# ── Payroll bonus accounts ───────────────────────────────────────────────────
# Bonuses post to the same account codes as regular payroll.  The annual bonus
# is paid once or twice a year (Jan and Jul at Revolution Labs) but Kardin
# budgets it evenly across all 12 months.
#
# Monthly bonus accrual = (Kardin annual budget ÷ 12) − standard_monthly
#   where standard_monthly = the minimum monthly value across M1–M12
#   (i.e., the non-payment months that carry only base payroll).
#
# The accrual posts every month UNLESS the GL net_change for the period already
# equals or exceeds the monthly average (meaning the actual payment hit the GL
# in that period — no separate accrual needed).
#
# 'kardin_keywords' are matched against the Kardin row's 'description' field
# to select only the bonus-inclusive budget row (not the SX/admin-overhead rows).
PAYROLL_BONUS_ACCOUNTS: dict = {
    '615110': {
        'label':            'RM-Pay/Wages',
        'kardin_keywords':  ['bonus', 'payroll', 'ot'],
    },
    '637110': {
        'label':            'Admin-Pay/Wages',
        'kardin_keywords':  ['bonus', 'salary'],
    },
}

THIN_BORDER = Border(
    left=Side(style='thin'), right=Side(style='thin'),
    top=Side(style='thin'), bottom=Side(style='thin'),
)
DOUBLE_BOTTOM = Border(bottom=Side(style='double'))

DARK_BLUE = '1F4E78'
MED_BLUE = '2E75B6'
LIGHT_BLUE = 'D6E4F0'
LIGHT_GRAY = 'F2F2F2'
WHITE = 'FFFFFF'


def _apply(cell, font=None, fill=None, fmt=None, border=None, align=None):
    if font:
        cell.font = font
    if fill:
        cell.fill = fill
    if fmt:
        cell.number_format = fmt
    if border:
        cell.border = border
    if align:
        cell.alignment = align


def _hdr_font():
    return Font(name='Calibri', size=11, bold=True, color='FFFFFF')

def _hdr_fill():
    return PatternFill(start_color=DARK_BLUE, end_color=DARK_BLUE, fill_type='solid')

def _subhdr_fill():
    return PatternFill(start_color=LIGHT_BLUE, end_color=LIGHT_BLUE, fill_type='solid')


# ── Layer 1b: Insurance prepaid amortization ─────────────────

# Insurance prepaid account and expense accounts
_PREPAID_INSURANCE_ACCT = '135110'
_INSURANCE_EXPENSE_ACCTS = {'639110', '639120'}

def detect_insurance_amortization(gl_data, budget_data) -> List[Dict[str, Any]]:
    """
    Generate monthly insurance expense entries from Prepaid Insurance (135110).

    JLL method: Annual premiums are paid upfront and held in 135110 Prepaid
    Insurance. Each month JLL posts:
        DR 639110  Insurance-Property         (budget PTD amount)
        DR 639120  Insurance-General Liability (budget PTD amount)
        CR 135110  Prepaid Insurance           (combined monthly total)

    We generate these only when:
      1. 135110 has a positive ending balance (prepaid exists to draw down)
      2. The insurance expense account has no current-period GL activity
         (i.e., JLL hasn't posted the entry yet — normal for pre-close GL)

    Monthly amounts come from the budget PTD column, which is set from the
    actual policy premium ÷ policy months (matches JLL's calculation within
    a few cents due to rounding).

    Returns a list of dicts: one per expense account line, with
    'credit_account' / 'credit_name' keys so build_accrual_entries() can
    generate the correct CR 135110 offset instead of the default 211200.
    """
    results: List[Dict[str, Any]] = []

    if not gl_data or not budget_data:
        return results

    # 1. Check that 135110 has a positive balance to amortise from
    prepaid_balance = 0.0
    for acct in (gl_data.accounts if hasattr(gl_data, 'accounts') else []):
        if str(acct.account_code).strip() == _PREPAID_INSURANCE_ACCT:
            prepaid_balance = acct.ending_balance
            break

    if prepaid_balance <= 0:
        return results

    # 2. Find which insurance expense accounts already have March activity
    already_posted: set = set()
    for acct in (gl_data.accounts if hasattr(gl_data, 'accounts') else []):
        code = str(acct.account_code).strip()
        if code in _INSURANCE_EXPENSE_ACCTS and abs(acct.net_change) > 0.01:
            already_posted.add(code)

    # 3. Pull monthly amounts from the budget PTD column
    budget_rows = budget_data if isinstance(budget_data, list) else []
    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        if code not in _INSURANCE_EXPENSE_ACCTS:
            continue
        if code in already_posted:
            continue  # JLL already posted it this period

        name       = str(row.get('account_name', '') or code)
        monthly    = abs(float(row.get('ptd_budget', 0) or 0))
        if monthly < 1.0:
            continue

        results.append({
            'account_code':   code,
            'account_name':   name,
            'amount':         _round(monthly),
            'credit_account': _PREPAID_INSURANCE_ACCT,
            'credit_name':    'Prepaid Insurance',
            'source':         'prepaid_amortization',
            'confidence':     'high',
            'description': (
                f'Insurance prepaid amortization — {name}: '
                f'${monthly:,.2f}/month (DR {code} / CR {_PREPAID_INSURANCE_ACCT}; '
                f'prepaid balance ${prepaid_balance:,.2f})'
            ),
        })

    return results


# ── Layer 1c: Real Estate Tax amortization ───────────────────

_RETAX_EXPENSE_ACCT = '641110'   # Real Estate Taxes (income statement)
_RETAX_ESCROW_ACCT  = '115200'   # Restricted Cash - RE Tax Escrow (balance sheet)

def detect_retax_amortization(gl_data, period: str = '') -> Optional[Dict[str, Any]]:
    """
    Generate the monthly real estate tax expense entry.

    JLL method: Lexington taxes are paid quarterly (due Jan 1, Apr 1, Jul 1,
    Oct 1).  Each quarter's payment draws from the RE Tax Escrow (115200) and
    is split evenly across the three months it covers:

        DR 641110  Real Estate Taxes      (1/3 of quarterly payment)
        CR 115200  RE Tax Escrow          (draws down escrow balance)

    The pre-close GL we receive has the Jan and Feb entries already posted but
    is missing the March entry — JLL posts it at month-end close.

    Monthly amount derivation:
        beginning_balance(641110) ÷ (period_month − 1)

    e.g. for March (period_month = 3):
        $498,750.81 ÷ 2 = $249,375.40/month

    This is more accurate than the budget because it reflects the actual
    quarterly payment that Berkadia made to the town.

    Returns None if:
      - 641110 has no beginning balance (no prior-period payments this year)
      - 641110 already has current-period activity (already posted)
      - period_month is 1 (January — no prior months to average from; the
        full quarterly payment posts directly in January)
    """
    if not gl_data or not hasattr(gl_data, 'accounts'):
        return None

    # Parse period month from period string ("Mar-2026" → 3)
    _MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    period_month = 0
    for abbr, num in _MONTH_MAP.items():
        if abbr in (period or '').lower():
            period_month = num
            break

    # January: the full quarterly payment is posted directly — no proration needed
    if period_month <= 1:
        return None

    # Find 641110 in GL
    retax_acct = None
    for acct in gl_data.accounts:
        if str(acct.account_code).strip() == _RETAX_EXPENSE_ACCT:
            retax_acct = acct
            break

    if retax_acct is None:
        return None

    # Need prior-period history to derive the monthly rate
    beg_bal = abs(retax_acct.beginning_balance)
    if beg_bal < 1.0:
        return None

    # Skip if already posted this period
    if abs(retax_acct.net_change) > 0.01:
        return None

    # Derive monthly amount: beginning balance covers months 1 → (period_month − 1)
    prior_months = period_month - 1
    monthly_amt  = _round(beg_bal / prior_months)

    # Verify escrow account exists and has a balance to draw from
    escrow_balance = 0.0
    for acct in gl_data.accounts:
        if str(acct.account_code).strip() == _RETAX_ESCROW_ACCT:
            escrow_balance = acct.ending_balance
            break

    return {
        'account_code':   _RETAX_EXPENSE_ACCT,
        'account_name':   'Real Estate Taxes',
        'amount':         monthly_amt,
        'credit_account': _RETAX_ESCROW_ACCT,
        'credit_name':    'Restricted Cash - RE Tax Escrow',
        'source':         'prepaid_amortization',
        'confidence':     'high',
        'description': (
            f'RE Tax monthly allocation — ${monthly_amt:,.2f}/month '
            f'(${beg_bal:,.2f} YTD ÷ {prior_months} prior months; '
            f'escrow balance ${escrow_balance:,.2f})'
        ),
    }


# ── Tenant utility billing detection ────────────────────────

def detect_tenant_utility_billing(gl_data, budget_data) -> List[Dict[str, Any]]:
    """
    Check whether the tenant sub-metered utility billing JE (meter read) has
    been posted this period for 440500 (electric) and 440700 (gas).

    When NOT posted:  returns budget accrual candidates so the income side of
    NOI is not understated while the expense proration is accruing the full
    building bill.

    When already posted: returns nothing (GL already has the income entry).

    The pipeline accrues ONE aggregate line per account (budget amount) as a
    placeholder.  When the sidebar provides per-tenant actual amounts, those
    replace the budget aggregate and generate one JE line per tenant.

    Returns list of dicts:
        account_code, account_name, amount (budget), label,
        source='tenant_utility_billing', confidence='medium'
    """
    results: List[Dict[str, Any]] = []
    if not gl_data or not budget_data:
        return results

    # Build budget amount lookup
    budget_by_code: Dict[str, float] = {}
    rows = budget_data if isinstance(budget_data, list) else getattr(budget_data, 'line_items', [])
    for row in rows:
        code = str((row.get('account_code') if isinstance(row, dict)
                    else getattr(row, 'account_code', '')) or '').strip()
        ptd  = (row.get('ptd_budget') if isinstance(row, dict)
                else getattr(row, 'ptd_budget', 0)) or 0
        budget_by_code[code] = abs(float(ptd))

    # Check each tenant utility account
    gl_accounts_by_code: Dict[str, Any] = {}
    for acct in (gl_data.accounts if hasattr(gl_data, 'accounts') else []):
        gl_accounts_by_code[str(acct.account_code).strip()] = acct

    for code, info in TENANT_UTILITY_ACCOUNTS.items():
        acct = gl_accounts_by_code.get(code)
        # Activity = any net_change (income = credit = negative net_change)
        if acct and abs(acct.net_change) > 0.01:
            continue   # already posted this period

        budget_amt = budget_by_code.get(code, 0.0)
        if budget_amt < 1.0:
            continue

        results.append({
            'account_code': code,
            'account_name': info['label'],
            'amount':       _round(budget_amt),
            'label':        info['label'],
            'source':       'tenant_utility_billing',
            'confidence':   'medium',
            'description': (
                f'Tenant utility accrual — {info["label"]}: '
                f'meter read JE not yet posted. '
                f'Accruing budget ${budget_amt:,.2f}. '
                f'Update with actual per-tenant amounts when meter read received.'
            ),
        })

    return results


# ── Layer 2: Invoice-period proration ────────────────────────

# Billing date range: "01.31.26-03.02.26" or "01.31.26 - 03.02.26"
_DATE_RANGE_RE = re.compile(
    r'(\d{2})\.(\d{2})\.(\d{2})\s*-\s*(\d{2})\.(\d{2})\.(\d{2})'
)
# Single date: "03.13.26"
_SINGLE_DATE_RE = re.compile(r'(\d{2})\.(\d{2})\.(\d{2})')

# Account name fragments that indicate a payroll line
_PAYROLL_NAME_KW  = ('pay/wages', 'pay wages', 'payroll')
# Transaction description fragments that confirm a payroll entry
_PAYROLL_DESC_KW  = ('payroll', 'eng payroll', 'admin payroll', 'pay/wages')



def _parse_date_range(text: str):
    """
    Parse 'MM.DD.YY-MM.DD.YY' billing period from a GL description/remarks string.

    Returns (start: date, end: date) or (None, None) if not found.
    Years are assumed 20xx (adequate through 2099).
    """
    m = _DATE_RANGE_RE.search(text or '')
    if not m:
        return None, None
    try:
        start = date(2000 + int(m.group(3)),  int(m.group(1)),  int(m.group(2)))
        end   = date(2000 + int(m.group(6)),  int(m.group(4)),  int(m.group(5)))
        return (start, end) if end >= start else (None, None)
    except ValueError:
        return None, None


def _parse_single_date(text: str) -> Optional[date]:
    """Parse the first 'MM.DD.YY' date in text. Returns None if none found."""
    m = _SINGLE_DATE_RE.search(text or '')
    if not m:
        return None
    try:
        return date(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def _month_end_from_period(period_str: str) -> Optional[date]:
    """
    Derive the last calendar day of the reporting month from a period string.

    Handles formats:
      'Mar-2026'  →  date(2026, 3, 31)
      'Mar 2026'  →  date(2026, 3, 31)
    """
    _MONTH_MAP = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,  'May': 5,  'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    }
    if not period_str:
        return None
    m = re.match(r'([A-Za-z]{3})[\s\-](\d{4})', period_str.strip())
    if not m:
        return None
    month = _MONTH_MAP.get(m.group(1).capitalize())
    year  = int(m.group(2))
    if not month:
        return None
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def detect_invoice_proration_accruals(
    gl_data,
    period: str = '',
    month_end: Optional[date] = None,
    materiality: float = 500.0,
) -> List[Dict[str, Any]]:
    """
    Layer 2 — Invoice-period proration accruals.

    Scans GL transactions for billing date-range references in the remarks /
    description field (format ``MM.DD.YY-MM.DD.YY``).  For each expense account
    where the latest invoiced period ends *before* the close of the reporting
    month, it accrues the uncovered portion at the same daily rate as the most
    recent invoice.

    Algorithm
    ---------
    For each expense account (6xxxxx, 5xxxxx, …):

    **Vendor billing-period accounts** (electricity, gas, security, elevator, …)
      1. Parse ``(start, end, amount)`` from every transaction with a date range.
      2. Group by billing end date; identify the *latest* end date.
      3. For the latest group: compute daily rate = total amount / period days.
      4. Uncovered days  = calendar month-end  −  latest billing end.
      5. Accrual = daily rate × uncovered days   (if > materiality threshold).

    **Payroll accounts** (account name contains "Pay/Wages" or "Payroll")
      1. Identify payroll runs by description keyword.
      2. Determine pay period length from the gap between consecutive run dates.
      3. Sum all charges in the latest pay period (regular + OT, etc.).
      4. Daily rate = period total / pay-period days.
      5. Accrual = daily rate × days from last run to month-end.

    For multi-vendor accounts (e.g., electricity has both Eversource and
    Hudson Energy), invoices sharing the same billing end date are *combined*:
    the daily rate is the sum across all vendors, accurately reflecting the
    total daily cost of service.

    Args:
        gl_data:     GLParseResult (from parsers.yardi_gl.parse_gl)
        period:      Accounting period string, e.g. 'Mar-2026' (used to derive
                     month-end when ``month_end`` is not supplied explicitly)
        month_end:   Override: last day of the reporting month.  If None, derived
                     from ``period`` or from gl_data.metadata.period.
        materiality: Minimum accrual (default $500) — smaller amounts are skipped.

    Returns:
        List of candidate dicts::

            account_code, account_name, accrual_amount, source ('invoice_proration'),
            description, daily_rate, uncovered_days, period_days, invoice_total
    """
    candidates: List[Dict[str, Any]] = []

    if not gl_data or not hasattr(gl_data, 'accounts'):
        return candidates

    # ── Resolve reporting month-end ────────────────────────────────────────────
    if month_end is None:
        month_end = _month_end_from_period(period)
    if month_end is None:
        # Try GL metadata
        try:
            month_end = _month_end_from_period(gl_data.metadata.period)
        except Exception:
            pass
    if month_end is None:
        return candidates   # can't prorate without knowing when the month ends

    for acct in gl_data.accounts:
        code = str(acct.account_code).strip()
        if not code or code[0] not in ('5', '6', '7', '8'):
            continue

        # ── VENDOR BILLING-PERIOD PRORATION ───────────────────────────────────
        # Group transactions that carry a billing date range by their end date.
        by_end: Dict[date, List[tuple]] = defaultdict(list)
        has_range_txns = False

        for txn in acct.transactions:
            amt = (txn.debit or 0) - (txn.credit or 0)
            if amt <= 0:
                continue
            start, end = _parse_date_range(txn.remarks or '')
            if start is None:
                start, end = _parse_date_range(txn.description or '')
            if start and end:
                by_end[end].append((start, end, amt))
                has_range_txns = True

        if has_range_txns:
            latest_end = max(by_end.keys())
            uncovered  = (month_end - latest_end).days

            if uncovered <= 0:
                # Latest invoice already covers the full month
                continue

            # Build daily rate from the most recently invoiced period.
            # Combine all vendors that share this billing end date.
            group = by_end[latest_end]
            total_amount = sum(g[2] for g in group)
            min_start    = min(g[0] for g in group)
            period_days  = max(1, (latest_end - min_start).days)

            # Sanity cap: don't extrapolate more than 2× the billing period.
            # This filters short-duration service calls (e.g., 10-day HVAC
            # repair invoiced in Feb with 47 uncovered March days → wrong)
            # while allowing 30-day utility cycles to bleed into the next month
            # by up to 30 extra days (gas billed in Feb covering all of March).
            if uncovered > period_days * 2.0:
                continue

            daily_rate   = total_amount / period_days
            accrual      = daily_rate * uncovered

            if accrual < materiality:
                continue

            candidates.append({
                'account_code':   code,
                'account_name':   acct.account_name,
                'accrual_amount': _round(accrual),
                'source':         'invoice_proration',
                'description': (
                    f'Invoice proration — {acct.account_name}: '
                    f'last invoice {min_start.strftime("%m/%d/%y")}'
                    f'-{latest_end.strftime("%m/%d/%y")} '
                    f'(${total_amount:,.0f}/{period_days}d = '
                    f'${daily_rate:,.2f}/day x {uncovered} days uncovered)'
                ),
                'daily_rate':     round(daily_rate, 4),
                'uncovered_days': uncovered,
                'period_days':    period_days,
                'invoice_total':  _round(total_amount),
            })
            continue   # Don't also run payroll check for this account

        # ── PAYROLL PRORATION ─────────────────────────────────────────────────
        # Only applicable to accounts whose name suggests payroll.
        name_lower = (acct.account_name or '').lower()
        if not any(kw in name_lower for kw in _PAYROLL_NAME_KW):
            continue

        # Collect payroll runs: debit entries where description mentions payroll.
        payroll_runs: List[tuple] = []   # (run_date: date, amount: float)
        for txn in acct.transactions:
            amt = (txn.debit or 0) - (txn.credit or 0)
            if amt <= 0:
                continue
            combined = ((txn.remarks or '') + ' ' + (txn.description or '')).lower()
            if not any(kw in combined for kw in _PAYROLL_DESC_KW):
                continue
            run_date = _parse_single_date(txn.remarks or '')
            if run_date is None:
                run_date = _parse_single_date(txn.description or '')
            if run_date is None:
                # Fall back to the transaction's posted date
                run_date = txn.date if isinstance(txn.date, date) else None
            if run_date:
                payroll_runs.append((run_date, amt))

        if len(payroll_runs) < 2:
            continue   # Need ≥ 2 runs to infer pay period length

        payroll_runs.sort(key=lambda x: x[0])

        # Pay period length = gap between the two most-recent distinct run dates.
        # Group by date and sum amounts so we can identify the "main" payroll
        # runs vs. small off-cycle entries (e.g., a $1,554 mid-cycle run).
        dates_only = sorted({r[0] for r in payroll_runs})
        if len(dates_only) < 2:
            continue

        # Use the last-two-date gap but enforce a 13-day floor.
        # Off-cycle payroll entries (e.g., a small catch-up run mid-cycle)
        # can create 7-day gaps between payroll dates that make the detected
        # period half the true bi-weekly cycle.  13 days is safely below any
        # bi-weekly (14d) or semi-monthly (13-16d) schedule while filtering out
        # the 7-day false periods from off-cycle runs.
        raw_gap = (dates_only[-1] - dates_only[-2]).days
        pay_period_days = max(13, raw_gap)

        # Latest run date and total amount for that run (regular + OT combined).
        latest_run_date = dates_only[-1]
        latest_run_total = sum(amt for rd, amt in payroll_runs if rd == latest_run_date)

        # Days from last run to month-end = uncovered payroll days.
        uncovered = (month_end - latest_run_date).days
        if uncovered <= 0:
            continue

        daily_rate = latest_run_total / pay_period_days
        accrual    = daily_rate * uncovered

        if accrual < materiality:
            continue

        candidates.append({
            'account_code':   code,
            'account_name':   acct.account_name,
            'accrual_amount': _round(accrual),
            'source':         'invoice_proration',
            'description': (
                f'Payroll accrual — {acct.account_name}: '
                f'last run {latest_run_date.strftime("%m/%d/%y")} '
                f'(${latest_run_total:,.2f}/{pay_period_days}d = '
                f'${daily_rate:,.2f}/day x {uncovered} days uncovered)'
            ),
            'daily_rate':     round(daily_rate, 4),
            'uncovered_days': uncovered,
            'period_days':    pay_period_days,
            'invoice_total':  _round(latest_run_total),
        })

        continue   # payroll path handled — skip recurring-vendor check

    # ── PASS 2: Recurring month-start vendor accruals ─────────────────────────
    # Pattern: monthly service billed in arrears at the start of the following
    # month (e.g., Casella trash on 03/01 covers February service → March
    # service is unbilled and needs an accrual).
    #
    # Detection criteria — ALL must be true:
    #   1. No billing date ranges in any transaction (already handled above)
    #   2. All current-period transactions posted within the first 5 days of
    #      the reporting month (strong signal of "prior month billed at open")
    #   3. Account has prior-period history (beginning_balance > 0) — confirms
    #      it's a recurring expense, not a one-off
    #   4. Total debit for the period exceeds materiality threshold
    #   5. Expense account (6xxx / 5xxx / 7xxx / 8xxx)

    period_month_start = date(month_end.year, month_end.month, 1)

    _already_coded = {c['account_code'] for c in candidates}

    for acct in gl_data.accounts:
        code = str(acct.account_code).strip()
        if not code or code[0] not in ('5', '6', '7', '8'):
            continue
        if code in _already_coded:
            continue   # already handled by date-range or payroll path

        # Must have some GL activity this period
        if not acct.transactions:
            continue

        # Must have prior-period history
        beg_bal = getattr(acct, 'beginning_balance', 0) or 0
        if abs(beg_bal) < 1.0:
            continue

        # All debits in the period must be posted within the first 5 days
        period_debits = []
        all_early = True
        for txn in acct.transactions:
            amt = (txn.debit or 0) - (txn.credit or 0)
            if amt <= 0:
                continue
            txn_date = txn.date
            if not isinstance(txn_date, date):
                all_early = False
                break
            if txn_date < period_month_start or txn_date > month_end:
                # Transaction outside the reporting month — not an early-month bill
                all_early = False
                break
            if txn_date.day > 5:
                all_early = False
                break
            period_debits.append(amt)

        if not all_early or not period_debits:
            continue

        invoice_total = sum(period_debits)
        if invoice_total < materiality:
            continue

        # All criteria met — accrual = same amount as the current-period invoices
        # (current invoices cover prior month; current month is unbilled at same rate)
        vendors = list({(txn.description or '').split('(')[0].strip()
                        for txn in acct.transactions
                        if (txn.debit or 0) > 0})
        vendor_str = ', '.join(v for v in vendors if v)[:60]

        candidates.append({
            'account_code':   code,
            'account_name':   acct.account_name,
            'accrual_amount': _round(invoice_total),
            'source':         'invoice_proration',
            'description': (
                f'Recurring monthly accrual — {acct.account_name}: '
                f'{vendor_str} invoiced {period_month_start.strftime("%m/%d/%y")} '
                f'(prior month billing in arrears, current month unbilled = '
                f'${invoice_total:,.2f})'
            ),
            'daily_rate':     0.0,
            'uncovered_days': 0,
            'period_days':    0,
            'invoice_total':  _round(invoice_total),
        })

    return candidates


# ── Layer 3: Budget gap detection ────────────────────────────

def detect_budget_gaps(gl_data, budget_data, period: str = '') -> List[Dict[str, Any]]:
    """
    Identify accounts that have a budget amount but zero GL activity.

    Each candidate is classified with a **confidence** tier:

    HIGH — Fixed predictable monthly cost.  The monthly budget equals exactly
        1/12 of the annual budget (within 2%).  Typical examples: property
        insurance, real estate taxes, fixed-fee service contracts.  These can
        be posted without additional review.

    MEDIUM — Regular recurring cost whose invoice timing is slightly
        inconsistent (e.g., landscaping contracts, training programs, amenity
        services).  The budget is a reasonable estimate; reviewer should confirm
        no invoice is already in transit.

    LOW — Irregular or discretionary spend (repairs, one-time maintenance,
        variable operating costs).  The account name contains a repair/
        discretionary keyword.  An absence of an invoice in the GL likely means
        the work did NOT happen this month.  These are included in the JE CSV
        marked 'REVIEW REQUIRED' so the reviewer can decide whether to keep,
        reduce, or delete the entry before posting.

    Returns list of dicts including: account_code, account_name, budget_amount,
        source ('budget_gap'), confidence ('high'|'medium'|'low'), description.
    """
    # ── Escrow-funded expense accounts ────────────────────────────────────────
    # These expenses are recognized on the income statement when the actual
    # payment is made from the dedicated escrow account (115200, 115300), NOT
    # accrued monthly.  The lender funds the escrow from the monthly mortgage
    # payment; Berkadia/servicer disburses when the bill is due.  Accruing them
    # monthly would double-count against payments already booked YTD.
    #
    # Lender escrow account mapping:
    #   115200  RE Tax Escrow      →  641110  Real Estate Taxes
    #   115300  Insurance Escrow   →  639110  Insurance-Property
    #                              →  639120  Insurance-General Liability
    #
    # Do NOT add these to the budget gap — they are budget-to-actual timing
    # differences only, not missing accruals.
    # Note: 641110, 639110, 639120 are handled by Layer 0b amortization entries
    # (detect_retax_amortization / detect_insurance_amortization) and are added
    # to _covered before budget gap runs, so they never reach this layer.
    # Any future escrow-funded accounts with no dedicated amortization function
    # can be listed here as a safety net.
    _ESCROW_FUNDED: set = set()

    # Account name keywords that signal irregular / repair-type spend.
    # Accounts matching any of these get LOW confidence.
    _REPAIR_KW = (
        'repair', 'repairs', 'maint-repair', 'maint repair',
        'one-time', 'one time', 'discretionary',
    )
    # Account name keywords that signal fixed / scheduled costs → HIGH confidence.
    _FIXED_KW = (
        'insurance', 'tax', 'taxes', 'real estate', 'interest',
    )

    # ── Seasonal suppression ──────────────────────────────────────────────────
    # Accounts matching these name keywords are only active Apr–Oct (months 4–10).
    # Outside that window: suppress entirely (no accrual, no REVIEW flag).
    # Inside the window: normalise monthly amount to annual / 7 (active months).
    _SEASONAL_KW = ('landscap',)
    _SEASONAL_ACTIVE_MONTHS = frozenset(range(4, 11))   # Apr=4 … Oct=10
    _SEASONAL_ACTIVE_COUNT  = 7                          # Apr–Oct

    # Parse period month (0 = unknown → seasonal filter disabled, accrue normally)
    _MONTH_MAP_BG = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    _period_month = 0
    for _abbr, _num in _MONTH_MAP_BG.items():
        if _abbr in (period or '').lower():
            _period_month = _num
            break

    candidates = []

    if not budget_data or not gl_data:
        return candidates

    # Build set of GL accounts with activity this period
    gl_active = set()
    # Also build a lookup for beginning balance (proxy for prior-period history)
    gl_beg_bal: Dict[str, float] = {}
    if hasattr(gl_data, 'accounts'):
        for acct in gl_data.accounts:
            if abs(acct.net_change) > 0.01:
                gl_active.add(acct.account_code)
            gl_beg_bal[str(acct.account_code).strip()] = acct.beginning_balance

    # Check budget items
    budget_items = []
    if isinstance(budget_data, list):
        budget_items = budget_data
    elif hasattr(budget_data, 'line_items'):
        budget_items = budget_data.line_items

    for item in budget_items:
        if isinstance(item, dict):
            code = str(item.get('account_code', '') or '').strip()
            name = str(item.get('account_name', '') or '').strip()
            ptd_budget = item.get('ptd_budget', 0) or 0
            ptd_actual = item.get('ptd_actual', 0) or 0
            ytd_budget = item.get('ytd_budget', 0) or 0
            annual = item.get('annual', 0) or 0
        else:
            code = str(getattr(item, 'account_code', '') or '').strip()
            name = str(getattr(item, 'account_name', '') or '').strip()
            ptd_budget = getattr(item, 'ptd_budget', 0) or 0
            ptd_actual = getattr(item, 'ptd_actual', 0) or 0
            ytd_budget = getattr(item, 'ytd_budget', 0) or 0
            annual = getattr(item, 'annual', 0) or 0

        if not code or 'TOTAL' in name.upper():
            continue

        # Skip escrow-funded accounts — recognized at payment date, not monthly
        if code in _ESCROW_FUNDED:
            continue

        # ── Seasonal suppression ───────────────────────────────────────────────
        # Interior landscaping (633120) is year-round — only suppress exterior.
        name_lower_pre = name.lower()
        is_seasonal = (any(kw in name_lower_pre for kw in _SEASONAL_KW)
                       and 'interior' not in name_lower_pre)
        if is_seasonal and _period_month > 0:
            if _period_month not in _SEASONAL_ACTIVE_MONTHS:
                # Off-season: suppress entirely — no accrual, no REVIEW flag
                continue
            # In-season: normalise to annual / active_months so the PTD budget
            # (which spreads annual cost across 12 months) is replaced with the
            # correct per-active-month rate.
            # Guard: if the budget is already on 7 months (ptd_budget ≈ annual/7),
            # the Kardin budget has been corrected — use ptd_budget as-is.
            if abs(annual) > 0:
                already_normalised = (
                    abs(ptd_budget) > 0 and
                    abs(abs(ptd_budget) - abs(annual) / _SEASONAL_ACTIVE_COUNT) < 1.0
                )
                if not already_normalised:
                    ptd_budget = abs(annual) / _SEASONAL_ACTIVE_COUNT

        # Only expense accounts — uses per-property COA config (defaults to 5/6/7/8xxxxx)
        if not is_expense_account(code):
            continue

        # Materiality: budget must exceed $500 with no GL activity
        if abs(ptd_budget) <= 500 or abs(ptd_actual) >= 1:
            continue

        # Skip if YTD budget is zero but annual exists (not yet allocated)
        if abs(ytd_budget) < 1 and abs(annual) > 0:
            continue

        # Seasonality: if PTD budget is less than 30% of monthly average,
        # this is likely a low-budget month — don't accrue
        if abs(annual) > 0:
            monthly_avg = abs(annual) / 12
            if monthly_avg > 0 and abs(ptd_budget) < monthly_avg * 0.3:
                continue

        # ── Confidence classification ──────────────────────────────────────
        name_lower = name.lower()

        # Does this account have ANY history in the GL this fiscal year?
        # (beginning_balance reflects Jan–prior month cumulative activity)
        has_prior_history = abs(gl_beg_bal.get(code, 0.0)) > 50.0
        # Is it a known fixed-cost account (insurance, RE taxes)?
        is_fixed = any(kw in name_lower for kw in _FIXED_KW)
        # Is it a repair / irregular / discretionary account?
        is_repair = any(kw in name_lower for kw in _REPAIR_KW)

        if is_repair:
            # Repair accounts are inherently irregular — always LOW
            confidence = 'low'
        elif is_fixed:
            # Fixed costs (insurance, RE taxes): budget = precise monthly amount.
            # Even if GL history is zero (e.g., first year, or prepaid route),
            # these are contractually fixed → keep HIGH.
            confidence = 'high'
        elif not has_prior_history:
            # No prior GL activity this fiscal year + not a known fixed cost.
            # Two likely causes:
            #   a) Seasonal expense (e.g., landscaping in winter) — don't accrue.
            #   b) New contract with first invoice pending — might need accrual.
            # Both are LOW confidence: reviewer must decide.
            confidence = 'low'
        elif abs(annual) > 0:
            monthly_avg = abs(annual) / 12
            deviation = abs(abs(ptd_budget) - monthly_avg) / monthly_avg if monthly_avg > 0 else 1.0
            # Within 2% of the flat monthly rate → effectively fixed → HIGH
            confidence = 'high' if deviation <= 0.02 else 'medium'
        else:
            confidence = 'medium'

        # ── Build human-readable description ──────────────────────────────
        if confidence == 'high':
            desc = (
                f'Budget gap accrual — {name}: ${abs(ptd_budget):,.2f}/month '
                f'(fixed monthly cost, no GL activity this period)'
            )
        elif confidence == 'low' and not has_prior_history and not is_repair:
            desc = (
                f'REVIEW REQUIRED — {name}: budget ${abs(ptd_budget):,.2f} but '
                f'no GL activity in any prior month this year. '
                f'Confirm whether the expense was incurred (may be seasonal).'
            )
        elif confidence == 'low':
            desc = (
                f'REVIEW REQUIRED — {name}: budget ${abs(ptd_budget):,.2f} but '
                f'no invoice received. Confirm whether work was performed before posting.'
            )
        else:
            desc = (
                f'Budget gap accrual — {name}: budgeted ${abs(ptd_budget):,.2f}, '
                f'no GL activity this period. Confirm invoice is in transit.'
            )

        candidates.append({
            'account_code':  code,
            'account_name':  name,
            'budget_amount': abs(ptd_budget),
            'source':        'budget_gap',
            'confidence':    confidence,
            'description':   desc,
        })

    # ── PASS 2: Partial contract coverage ─────────────────────────────────────
    # Accounts that have SOME GL activity this period but are significantly below
    # their PTD budget, and whose name contains "contract" — indicating a known
    # recurring service contract where one or more invoices may be missing.
    #
    # Example: HVAC-Contract Svc budgeted $12,676/month; GL shows $3,526 (one
    # vendor's monthly payment received, another's March invoice not yet arrived).
    #
    # Detection criteria — ALL must be true:
    #   1. Account name contains 'contract'
    #   2. ptd_actual > 0 (has some activity — distinguishes from zero-activity gaps)
    #   3. ptd_actual < ptd_budget × 0.5 (below 50% of expected monthly spend)
    #   4. Gap (ptd_budget − ptd_actual) > $500 materiality
    #   5. Has prior-period history (beginning_balance > 0)
    #   6. Not already captured by the main gap loop (ptd_actual was > 0 so main loop skipped it)
    #
    # Suggested amount: smallest debit from the current period — proxy for the
    # missing monthly contract payment (e.g., DAC $1,000 within a $3,526 period).
    # Confidence: always LOW — reviewer must confirm which invoice is missing.

    _partial_coded = {c['account_code'] for c in candidates}

    for item in budget_items:
        if isinstance(item, dict):
            code     = str(item.get('account_code', '') or '').strip()
            name     = str(item.get('account_name', '') or '').strip()
            ptd_b    = float(item.get('ptd_budget', 0) or 0)
            ptd_a    = float(item.get('ptd_actual', 0) or 0)
        else:
            code     = str(getattr(item, 'account_code', '') or '').strip()
            name     = str(getattr(item, 'account_name', '') or '').strip()
            ptd_b    = float(getattr(item, 'ptd_budget', 0) or 0)
            ptd_a    = float(getattr(item, 'ptd_actual', 0) or 0)

        if not code or 'TOTAL' in name.upper():
            continue
        if code[0] not in ('5', '6', '7', '8'):
            continue
        if code in _partial_coded:
            continue
        if code in _ESCROW_FUNDED:
            continue
        if 'contract' not in name.lower():
            continue
        if abs(ptd_a) < 1:
            continue   # zero-activity — already handled by main loop
        if abs(ptd_b) <= 500:
            continue
        gap = abs(ptd_b) - abs(ptd_a)
        if gap < 500:
            continue
        if abs(ptd_a) >= abs(ptd_b) * 0.5:
            continue   # >= 50% covered — normal variation, not a missing invoice

        # Check prior history
        has_history = abs(gl_beg_bal.get(code, 0.0)) > 50.0
        if not has_history:
            continue

        # Suggested amount: smallest positive debit this period from GL
        gl_acct = None
        if hasattr(gl_data, 'accounts'):
            for a in gl_data.accounts:
                if str(a.account_code).strip() == code:
                    gl_acct = a
                    break
        if gl_acct:
            debits = [abs((t.debit or 0) - (t.credit or 0))
                      for t in gl_acct.transactions
                      if (t.debit or 0) - (t.credit or 0) > 0]
            suggested = min(debits) if debits else gap
        else:
            suggested = gap

        # Check if this is a known periodic-billing account (quarterly, etc.)
        _periodic_info = PERIODIC_CONTRACT_ACCOUNTS.get(code)
        _periodic_gap  = _round(gap - suggested)   # estimated outstanding beyond min invoice

        _entry: Dict[str, Any] = {
            'account_code':  code,
            'account_name':  name,
            'budget_amount': _round(suggested),
            'source':        'budget_gap',
            'confidence':    'low',
            'description': (
                f'REVIEW — Partial contract coverage: {name} — '
                f'${abs(ptd_a):,.2f} billed of ${abs(ptd_b):,.2f} budget '
                f'({abs(ptd_a)/abs(ptd_b)*100:.0f}% covered). '
                f'Suggested accrual ${suggested:,.2f} (smallest invoice this period). '
                f'Confirm which contract invoices are still outstanding.'
            ),
        }

        if _periodic_info:
            _entry['periodic_flag']      = True
            _entry['periodic_label']     = _periodic_info['label']
            _entry['periodic_billing']   = _periodic_info['billing_cycle']
            _entry['periodic_suggested'] = _periodic_gap  # remaining gap after min invoice

        candidates.append(_entry)

    return candidates


# ── Layer 3: Historical pattern detection ────────────────────

def detect_historical_recurring(gl_data, budget_data) -> List[Dict[str, Any]]:
    """
    Identify recurring expense patterns by comparing GL beginning balance
    (YTD proxy) to budget. If an account had YTD activity through the prior
    month but nothing this month, it may need an accrual.

    Uses beginning_balance as a proxy for prior-month YTD activity.
    If beginning_balance shows consistent prior activity but net_change is
    zero, flag as a recurring accrual candidate.

    Returns list of dicts: account_code, account_name, estimated_amount, source='historical'
    """
    candidates = []

    if not gl_data or not hasattr(gl_data, 'accounts'):
        return candidates

    # Determine current month number from period
    period_str = getattr(gl_data.metadata, 'period', '') if hasattr(gl_data, 'metadata') else ''
    month_num = 1
    if '-' in period_str:
        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
        }
        month_name = period_str.split('-')[0]
        month_num = month_map.get(month_name, 1)

    # Require at least 2 prior months of data to extrapolate
    prior_months = month_num - 1
    if prior_months < 2:
        return candidates

    # Build budget lookup for cross-reference
    budget_by_code = {}
    if budget_data:
        budget_items = budget_data if isinstance(budget_data, list) else getattr(budget_data, 'line_items', [])
        for item in budget_items:
            if isinstance(item, dict):
                bcode = str(item.get('account_code', '') or '').strip()
                budget_by_code[bcode] = item
            else:
                bcode = str(getattr(item, 'account_code', '') or '').strip()
                budget_by_code[bcode] = item

    for acct in gl_data.accounts:
        code = acct.account_code
        # Only expense accounts — uses per-property COA config (defaults to 5/6/7/8xxxxx)
        if not is_expense_account(code):
            continue

        # Skip if there's activity this month
        if abs(acct.net_change) > 0.01:
            continue

        # Check if beginning balance suggests recurring prior activity
        begin = abs(acct.beginning_balance)
        if begin < 100:
            continue

        # Cross-reference against budget: zero budget + zero activity = likely discontinued
        if code in budget_by_code:
            bi = budget_by_code[code]
            if isinstance(bi, dict):
                bi_budget = bi.get('ptd_budget', 0) or 0
                bi_annual = bi.get('annual', 0) or 0
            else:
                bi_budget = getattr(bi, 'ptd_budget', 0) or 0
                bi_annual = getattr(bi, 'annual', 0) or 0

            if abs(bi_budget) < 1 and abs(bi_annual) < 1:
                continue  # Zero budget everywhere — likely discontinued

        # Estimate monthly amount from YTD / months elapsed
        est_monthly = begin / prior_months

        # Only flag if estimated monthly > $500 (material recurring expense)
        if est_monthly >= 500:
            candidates.append({
                'account_code': code,
                'account_name': acct.account_name,
                'estimated_amount': _round(est_monthly),
                'ytd_prior': begin,
                'months_prior': prior_months,
                'source': 'historical',
                'description': f'Recurring — {acct.account_name} avg ${est_monthly:,.0f}/mo ({prior_months} prior months), no activity this period',
            })

    return candidates


# ── Payroll bonus detection ──────────────────────────────────────────────────

def detect_payroll_bonus_accrual(
    gl_data,
    kardin_records: List[Dict],
    period_month: int,
) -> List[Dict[str, Any]]:
    """
    Generate monthly bonus accrual entries for payroll accounts.

    Business rule
    -------------
    The annual engineering and admin bonuses are paid in January and July
    but should be expensed evenly across all 12 months.  Kardin reflects
    this intent — the two payment months carry higher values while the
    remaining months carry only base payroll.

    Monthly bonus accrual = (Kardin annual ÷ 12) − standard_month
      where standard_month = min(M1..M12) for the bonus-inclusive row.

    The accrual is suppressed if the GL net_change for the period already
    equals or exceeds the monthly average (the actual bonus payment is in
    the GL — no separate accrual needed).

    Args:
        gl_data:        GLParseResult from yardi_gl parser
        kardin_records: List of dicts from parsers.kardin_budget.parse()
        period_month:   Integer month of the reporting period (1=Jan … 12=Dec)

    Returns:
        List of candidate dicts (same shape as budget_gap candidates) with
        source='bonus_accrual'.
    """
    results: List[Dict[str, Any]] = []

    if not gl_data or not kardin_records or not period_month:
        return results

    # Build GL net_change lookup for payroll accounts
    gl_net: dict = {}
    for acct in (gl_data.accounts if hasattr(gl_data, 'accounts') else []):
        code = str(acct.account_code).strip()
        if code in PAYROLL_BONUS_ACCOUNTS:
            gl_net[code] = acct.net_change

    for acct_code, config in PAYROLL_BONUS_ACCOUNTS.items():
        keywords = [k.lower() for k in config['kardin_keywords']]

        # Find Kardin rows for this account that include the bonus component
        bonus_rows = [
            r for r in kardin_records
            if str(r.get('account_code', '') or '').strip() == acct_code
            and any(kw in (r.get('description', '') or '').lower() for kw in keywords)
        ]
        if not bonus_rows:
            continue

        # Sum annual and all monthly amounts across matching rows
        annual = sum(float(r.get('m_total', 0) or 0) for r in bonus_rows)
        if annual <= 0:
            continue

        monthly_avg = annual / 12.0

        # Standard month = minimum Kardin monthly value (non-payment months)
        all_monthly: List[float] = []
        for r in bonus_rows:
            for m in range(1, 13):
                val = float(r.get(f'M{m}', 0) or 0)
                if val > 0:
                    all_monthly.append(val)
        if not all_monthly:
            continue
        standard_monthly = min(all_monthly)

        monthly_bonus = monthly_avg - standard_monthly

        # Skip if not material (< $100)
        if monthly_bonus < 100.0:
            continue

        # Check current-period GL activity
        net = gl_net.get(acct_code, 0.0)

        # Suppress in payment months: GL already ≥ monthly average
        # (the actual bonus payment is in the GL — no accrual needed)
        if net >= monthly_avg:
            continue

        results.append({
            'account_code':    acct_code,
            'account_name':    config['label'],
            'estimated_amount': _round(monthly_bonus),
            'source':          'bonus_accrual',
            'confidence':      'high',
            'description': (
                f'Monthly bonus accrual — {config["label"]}: '
                f'Kardin annual ${annual:,.2f} / 12 = ${monthly_avg:,.2f}/mo avg; '
                f'standard month ${standard_monthly:,.2f}; '
                f'bonus component ${monthly_bonus:,.2f}/mo'
            ),
        })

    return results


# ── Build JE lines from all sources ─────────────────────────

def build_accrual_entries(nexus_data: list, period: str = '',
                          property_name: str = '',
                          status_filter: list = None,
                          gl_data=None, budget_data=None,
                          period_month_end: Optional[date] = None,
                          manual_accruals: Optional[List[Dict]] = None,
                          tenant_utility_rows: Optional[List[Dict]] = None,
                          kardin_records: Optional[List[Dict]] = None,
                          bonus_overrides: Optional[Dict[str, float]] = None,
                          ) -> List[Dict[str, Any]]:
    """
    Build accrual journal entry lines from six sources:
      Layer 0: Manual overrides — user-supplied amounts for accounts that
               cannot be auto-calculated (e.g., semi-annual water/sewer bills)
      Layer 1: Nexus pending invoices (AP-side, deduped against GL)
      Layer 2: Invoice-period proration (billing date ranges in GL descriptions)
      Layer 3: Budget gap detection (accounts with budget but no GL activity)
      Layer 4: Historical recurring detection (prior-month YTD extrapolation)
      Layer 5: Payroll bonus accrual (Kardin annual ÷ 12 − standard month)

    Manual overrides take absolute priority and suppress all lower layers for
    the same account.  Layers 1-2 are high-fidelity and suppress Layers 3-4.

    Args:
        nexus_data:        List of invoice dicts from Nexus parser
        period:            Accounting period string (e.g., 'Mar-2026')
        property_name:     Property name for the JE header
        status_filter:     Invoice statuses to include (default: all)
        gl_data:           GLParseResult — required for Layers 2-4
        budget_data:       BC rows — required for Layer 3 (budget gap)
        period_month_end:  Override for the last calendar day of the reporting
                           month (date object).  If None, derived from ``period``
                           or gl_data.metadata.period automatically.
        manual_accruals:   List of dicts for user-supplied accrual amounts::

                               [{
                                   'account_code': '613310',
                                   'account_name': 'Utilities-Water/Sewer',
                                   'amount':        16635.75,   # semi-annual invoice / 6
                                   'description':   'Water/sewer semi-annual invoice $99,814.50 / 6 months',
                               }, ...]

                           Amount is the *monthly* accrual to post.  Description
                           should note the invoice amount and divisor so the
                           reviewer can verify.  Accounts in manual_accruals are
                           excluded from all automated layers.

    Returns:
        List of JE line dicts with keys:
          je_number, line, date, account_code, account_name,
          description, reference, debit, credit, vendor, invoice_number, source
    """
    invoices = nexus_data if isinstance(nexus_data, list) else []

    if status_filter:
        invoices = [inv for inv in invoices
                    if (inv.get('invoice_status', '') or '').lower()
                    in [s.lower() for s in status_filter]]

    # Build GL lookup for Layer 1 deduplication
    gl_lookup = _build_gl_invoice_lookup(gl_data) if gl_data else {'by_reference': {}, 'by_control': {}}

    je_lines = []
    je_num = 1

    # ── Layer 0: Manual accrual overrides ──────────────────────────────────────
    # User-supplied amounts for accounts the engine cannot auto-calculate
    # (e.g., semi-annual water/sewer billing where the invoice amount is known
    # to the property manager but cannot be reliably derived from GL data).
    _manual_accounts: set = set()
    for override in (manual_accruals or []):
        acct_code = str(override.get('account_code', '') or '').strip()
        acct_name = str(override.get('account_name', '') or acct_code)
        amount    = float(override.get('amount', 0) or 0)
        desc      = str(override.get('description', '') or
                        f'Manual accrual — {acct_name}')
        if not acct_code:
            continue

        # Register the account as manually handled BEFORE the amount check so
        # that app.py's dedup pattern (amount=0, non-empty account_code) correctly
        # suppresses Layers 1-4 for this account even when no JE is being generated.
        _manual_accounts.add(acct_code)

        if amount <= 0:
            continue  # account registered for dedup; no JE generated

        je_id = f'MAN-{je_num:04d}'
        je_lines.append({
            'je_number':      je_id,
            'line':           1,
            'date':           '',
            'account_code':   acct_code,
            'account_name':   acct_name,
            'description':    desc,
            'reference':      'MANUAL',
            'debit':          _round(amount),
            'credit':         0,
            'vendor':         '[Manual Override]',
            'invoice_number': '',
            'source':         'manual',
            'confidence':     'high',
        })
        je_lines.append({
            'je_number':      je_id,
            'line':           2,
            'date':           '',
            'account_code':   AP_ACCRUAL_ACCOUNT,
            'account_name':   AP_ACCRUAL_NAME,
            'description':    desc,
            'reference':      'MANUAL',
            'debit':          0,
            'credit':         _round(amount),
            'vendor':         '[Manual Override]',
            'invoice_number': '',
            'source':         'manual',
            'confidence':     'high',
        })
        _manual_accounts.add(acct_code)
        je_num += 1

    # ── Tenant utility billing (meter read JE) ─────────────────────────────────
    # Revenue side of the utility accrual: ensures NOI is not understated while
    # the expense proration (Layer 2) accrues the full building bill.
    #
    # Two modes:
    #   a) Actual per-tenant amounts (tenant_utility_rows provided by sidebar):
    #      One DR 131100 / CR 440500 line per tenant for electric.
    #      One DR 131100 / CR 440700 line per tenant for gas.
    #   b) Budget aggregate (no rows provided, account has no GL activity):
    #      Single DR 131100 / CR 440500 (electric budget).
    #      Single DR 131100 / CR 440700 (gas budget).
    #
    # When the meter read JE is already in GL (440500/440700 have activity),
    # this block is skipped entirely for that account.
    _tub_accounts: set = set()

    def _post_tub_line(cr_code: str, cr_name: str, amount: float,
                       tenant: str, desc: str) -> None:
        """Append DR 131100 / CR recovery-account pair for one tenant billing."""
        nonlocal je_num
        je_id = f'TUB-{je_num:04d}'
        je_lines.append({
            'je_number':      je_id, 'line': 1, 'date': '',
            'account_code':   TENANT_UTILITY_AR_ACCOUNT,
            'account_name':   TENANT_UTILITY_AR_NAME,
            'description':    desc,
            'reference':      'METER-READ',
            'debit':          _round(amount), 'credit': 0,
            'vendor':         tenant or '[Tenant Billing]',
            'invoice_number': '',
            'source':         'tenant_utility_billing', 'confidence': 'medium',
        })
        je_lines.append({
            'je_number':      je_id, 'line': 2, 'date': '',
            'account_code':   cr_code,
            'account_name':   cr_name,
            'description':    desc,
            'reference':      'METER-READ',
            'debit':          0, 'credit': _round(amount),
            'vendor':         tenant or '[Tenant Billing]',
            'invoice_number': '',
            'source':         'tenant_utility_billing', 'confidence': 'medium',
        })
        je_num += 1

    if gl_data and budget_data:
        # Determine which accounts already have GL activity this period
        _tub_gl: Dict[str, Any] = {
            str(a.account_code).strip(): a
            for a in (gl_data.accounts if hasattr(gl_data, 'accounts') else [])
        }

        if tenant_utility_rows:
            # Mode (a): per-tenant actuals from sidebar
            for row in (tenant_utility_rows or []):
                tenant_name = str(row.get('tenant', '') or '').strip()
                elec_amt    = float(row.get('electric', 0) or 0)
                gas_amt     = float(row.get('gas',     0) or 0)
                if not tenant_name:
                    continue

                if elec_amt > 0:
                    _acct = _tub_gl.get('440500')
                    if _acct is None or abs(_acct.net_change) < 0.01:
                        _post_tub_line(
                            '440500', 'Recovery - Electricity', elec_amt,
                            tenant_name,
                            f'Tenant electric billing — {tenant_name} '
                            f'(per meter read) ${elec_amt:,.2f}',
                        )
                        _tub_accounts.add('440500')

                if gas_amt > 0:
                    _acct = _tub_gl.get('440700')
                    if _acct is None or abs(_acct.net_change) < 0.01:
                        _post_tub_line(
                            '440700', 'Recovery - Misc Utilities', gas_amt,
                            tenant_name,
                            f'Tenant gas billing — {tenant_name} '
                            f'(per meter read) ${gas_amt:,.2f}',
                        )
                        _tub_accounts.add('440700')

        else:
            # Mode (b): auto-detect from GL + budget (aggregate budget accrual)
            for cand in detect_tenant_utility_billing(gl_data, budget_data):
                cr_code = cand['account_code']
                cr_name = 'Recovery - Electricity' if cr_code == '440500' else 'Recovery - Misc Utilities'
                _post_tub_line(
                    cr_code, cr_name, cand['amount'],
                    '[Budget Accrual]',
                    cand['description'],
                )
                _tub_accounts.add(cr_code)

    # ── Layer 0b: Prepaid / escrow amortization ────────────────────────────────
    # Entries that draw down a balance sheet asset/escrow rather than creating
    # a new liability (211200).  Each generates DR expense / CR asset account.
    #
    #   Insurance:     DR 639110/639120  /  CR 135110  Prepaid Insurance
    #   RE Taxes:      DR 641110         /  CR 115200  RE Tax Escrow
    #
    # Generated whenever the asset account has a balance and the expense account
    # has no current-period activity (normal for the pre-close GL from JLL).
    _amort_accounts: set = set()

    def _post_amort(entry: Dict[str, Any], prefix: str, ref: str, vendor: str) -> None:
        """Append a DR/CR amortization pair to je_lines and register the account."""
        nonlocal je_num
        acct_code = entry['account_code']
        if acct_code in _manual_accounts:
            return  # user override takes precedence
        je_id  = f'{prefix}-{je_num:04d}'
        amount = entry['amount']
        desc   = entry['description']
        je_lines.append({
            'je_number':      je_id, 'line': 1, 'date': '',
            'account_code':   acct_code,
            'account_name':   entry['account_name'],
            'description':    desc, 'reference': ref,
            'debit':          _round(amount), 'credit': 0,
            'vendor':         vendor, 'invoice_number': '',
            'source':         'prepaid_amortization', 'confidence': 'high',
        })
        je_lines.append({
            'je_number':      je_id, 'line': 2, 'date': '',
            'account_code':   entry['credit_account'],
            'account_name':   entry['credit_name'],
            'description':    desc, 'reference': ref,
            'debit':          0, 'credit': _round(amount),
            'vendor':         vendor, 'invoice_number': '',
            'source':         'prepaid_amortization', 'confidence': 'high',
        })
        _amort_accounts.add(acct_code)
        je_num += 1

    # Insurance: DR 639110/639120 / CR 135110
    if gl_data and budget_data:
        for ins in detect_insurance_amortization(gl_data, budget_data):
            _post_amort(ins, 'INS', 'INS-AMORT', '[Insurance Amortization]')

    # RE Taxes: DR 641110 / CR 115200
    if gl_data:
        retax = detect_retax_amortization(gl_data, period=period)
        if retax:
            _post_amort(retax, 'TAX', 'TAX-AMORT', '[RE Tax Amortization]')

    for inv in invoices:
        vendor = str(inv.get('vendor', '') or '')
        inv_num = str(inv.get('invoice_number', '') or '')
        inv_date = inv.get('invoice_date', '')
        # Use numeric account number if available (e.g. "637370" not "Admin-Computer/Software (637370)")
        gl_account = str(inv.get('gl_account_number', '') or inv.get('gl_account', '') or '')
        gl_category = str(inv.get('gl_category', '') or '')
        description = str(inv.get('line_description', '') or '')
        amount = inv.get('amount', 0) or 0

        if amount == 0:
            continue

        # Skip if user has manually specified this account in the One-Off table —
        # their override (with amount > 0) or suppression (amount = 0) takes precedence.
        if gl_account in _manual_accounts:
            continue

        # Dedup — two strategies, first-match wins:
        #   Strategy 1 (exact):     invoice number matches GL reference/control
        #   Strategy 2 (fuzzy):     vendor name + amount already posted to same account
        #                           (fires only when invoice number is absent)
        if inv_num and _is_invoice_in_gl(inv_num, gl_lookup):
            continue
        if not inv_num and _is_in_gl_by_vendor_amount(vendor, amount, gl_account, gl_lookup):
            continue

        # Format date
        if isinstance(inv_date, datetime):
            date_str = inv_date.strftime('%m/%d/%Y')
        elif isinstance(inv_date, str):
            date_str = inv_date
        else:
            date_str = str(inv_date) if inv_date else ''

        # Build description for JE
        je_desc = f"Accrual — {vendor}"
        if inv_num:
            je_desc += f" #{inv_num}"
        if description:
            je_desc += f" — {description[:50]}"

        # ── Prepaid split: accrue only current-month portion to expense;
        #    remaining future months go to prepaid asset (130000).
        #    Month 1 of N: DR expense (1/N) + DR prepaid (N-1/N) / CR 211200 (full)
        is_prepaid = inv.get('is_prepaid', False)
        prepaid_months = int(inv.get('prepaid_months', 1) or 1)

        if is_prepaid and prepaid_months > 1:
            monthly_amt = _round(abs(amount) / prepaid_months)
            rounding_adj = _round(abs(amount) - monthly_amt * prepaid_months)
            current_amt = monthly_amt + rounding_adj          # this period's expense
            future_amt  = abs(amount) - current_amt           # prepaid asset to book
        else:
            current_amt = abs(amount)
            future_amt  = 0.0

        je_id = f"ACC-{je_num:04d}"
        acct_name = gl_category or description[:30]

        # DR line: Expense account (current month only)
        # 'source': 'nexus' is REQUIRED — the _covered exclusion set at the
        # bottom of this function filters on source == 'nexus' to prevent
        # Layers 2-4 from generating duplicate entries for these accounts.
        je_lines.append({
            'je_number':      je_id,
            'line':           1,
            'date':           date_str,
            'account_code':   gl_account,
            'account_name':   acct_name,
            'description':    je_desc,
            'reference':      inv_num,
            'debit':          current_amt,
            'credit':         0,
            'vendor':         vendor,
            'invoice_number': inv_num,
            'source':         'nexus',
        })

        # CR line: AP Accrual (211200) — current month
        je_lines.append({
            'je_number':      je_id,
            'line':           2,
            'date':           date_str,
            'account_code':   AP_ACCRUAL_ACCOUNT,
            'account_name':   AP_ACCRUAL_NAME,
            'description':    je_desc,
            'reference':      inv_num,
            'debit':          0,
            'credit':         current_amt,
            'vendor':         vendor,
            'invoice_number': inv_num,
            'source':         'nexus',
        })

        je_num += 1

        # Second JE: book future months to prepaid asset (130000)
        if future_amt > 0:
            je_id_ppd = f"ACC-{je_num:04d}"
            ppd_desc = f"Prepaid booking — {vendor} #{inv_num} ({prepaid_months - 1} future mo)"

            je_lines.append({
                'je_number':      je_id_ppd,
                'line':           1,
                'date':           date_str,
                'account_code':   PREPAID_ASSET_ACCOUNT,
                'account_name':   PREPAID_ASSET_NAME,
                'description':    ppd_desc,
                'reference':      inv_num,
                'debit':          future_amt,
                'credit':         0,
                'vendor':         vendor,
                'invoice_number': inv_num,
                'source':         'nexus',
            })
            je_lines.append({
                'je_number':      je_id_ppd,
                'line':           2,
                'date':           date_str,
                'account_code':   AP_ACCRUAL_ACCOUNT,
                'account_name':   AP_ACCRUAL_NAME,
                'description':    ppd_desc,
                'reference':      inv_num,
                'debit':          0,
                'credit':         future_amt,
                'vendor':         vendor,
                'invoice_number': inv_num,
                'source':         'nexus',
            })
            je_num += 1

    # ── Resolve reporting month-end (used by Layers 2 and onward) ──
    _month_end = period_month_end or _month_end_from_period(period)
    if _month_end is None and gl_data:
        try:
            _month_end = _month_end_from_period(gl_data.metadata.period)
        except Exception:
            pass

    # Collect accounts already covered by Layers 0 (manual), 0b (amortization),
    # and 1 (Nexus). Seeding _covered here prevents budget gap and historical
    # layers from generating duplicate entries for the same account.
    _covered = _manual_accounts | _amort_accounts | set(
        l['account_code'] for l in je_lines
        if l.get('line') == 1 and l.get('source') == 'nexus'
    )

    # ── Layer 2: Invoice-period proration ──
    if gl_data:
        prorations = detect_invoice_proration_accruals(
            gl_data, period=period, month_end=_month_end
        )
        for pro in prorations:
            if pro['account_code'] in _covered:
                continue   # already handled by Nexus

            je_id   = f"IPR-{je_num:04d}"
            je_desc = pro['description']

            je_lines.append({
                'je_number':      je_id,
                'line':           1,
                'date':           _month_end.strftime('%m/%d/%Y') if _month_end else '',
                'account_code':   pro['account_code'],
                'account_name':   pro['account_name'],
                'description':    je_desc,
                'reference':      'INV-PRORATION',
                'debit':          pro['accrual_amount'],
                'credit':         0,
                'vendor':         '[Invoice Proration]',
                'invoice_number': '',
                'source':         'invoice_proration',
            })
            je_lines.append({
                'je_number':      je_id,
                'line':           2,
                'date':           _month_end.strftime('%m/%d/%Y') if _month_end else '',
                'account_code':   AP_ACCRUAL_ACCOUNT,
                'account_name':   AP_ACCRUAL_NAME,
                'description':    je_desc,
                'reference':      'INV-PRORATION',
                'debit':          0,
                'credit':         pro['accrual_amount'],
                'vendor':         '[Invoice Proration]',
                'invoice_number': '',
                'source':         'invoice_proration',
            })
            _covered.add(pro['account_code'])
            je_num += 1

    # ── Layer 3: Budget gap accruals ──
    # Fallback for accounts with a budget but no GL activity AND no proration data.
    if gl_data and budget_data:
        budget_gaps = detect_budget_gaps(gl_data, budget_data, period=period)

        for gap in budget_gaps:
            if gap['account_code'] in _covered:
                continue   # already handled by Nexus or proration

            confidence = gap.get('confidence', 'medium')
            je_id   = f"BGA-{je_num:04d}"
            # Use the rich description from detect_budget_gaps (includes confidence note)
            je_desc = gap.get('description') or f"Budget gap accrual — {gap['account_name']}"

            # LOW confidence gaps are still included but clearly flagged so the
            # reviewer can decide whether to keep, adjust, or delete before posting.
            _gap_line: Dict[str, Any] = {
                'je_number':    je_id,
                'line':         1,
                'date':         '',
                'account_code': gap['account_code'],
                'account_name': gap['account_name'],
                'description':  je_desc,
                'reference':    'BUDGET-GAP',
                'debit':        gap['budget_amount'],
                'credit':       0,
                'vendor':       '[Budget Gap]',
                'invoice_number': '',
                'source':       'budget_gap',
                'confidence':   confidence,
            }
            # Carry periodic-billing flag through so the UI can surface
            # a supplement input for the remaining undetected portion.
            if gap.get('periodic_flag'):
                _gap_line['periodic_flag']      = True
                _gap_line['periodic_label']     = gap.get('periodic_label', '')
                _gap_line['periodic_billing']   = gap.get('periodic_billing', '')
                _gap_line['periodic_suggested'] = gap.get('periodic_suggested', 0.0)
            je_lines.append(_gap_line)
            je_lines.append({
                'je_number':    je_id,
                'line':         2,
                'date':         '',
                'account_code': AP_ACCRUAL_ACCOUNT,
                'account_name': AP_ACCRUAL_NAME,
                'description':  je_desc,
                'reference':    'BUDGET-GAP',
                'debit':        0,
                'credit':       gap['budget_amount'],
                'vendor':       '[Budget Gap]',
                'invoice_number': '',
                'source':       'budget_gap',
                'confidence':   confidence,
            })
            _covered.add(gap['account_code'])
            je_num += 1

    # ── Layer 4: Historical recurring accruals ──
    if gl_data:
        historicals = detect_historical_recurring(gl_data, budget_data)
        for hist in historicals:
            if hist['account_code'] in _covered:
                continue

            je_id = f"REC-{je_num:04d}"
            je_desc = f"Recurring accrual — {hist['account_name']} (est. ${hist['estimated_amount']:,.0f}/mo)"

            je_lines.append({
                'je_number': je_id,
                'line': 1,
                'date': '',
                'account_code': hist['account_code'],
                'account_name': hist['account_name'],
                'description': je_desc,
                'reference': 'RECURRING',
                'debit': hist['estimated_amount'],
                'credit': 0,
                'vendor': '[Historical Recurring]',
                'invoice_number': '',
                'source': 'historical',
            })
            je_lines.append({
                'je_number': je_id,
                'line': 2,
                'date': '',
                'account_code': AP_ACCRUAL_ACCOUNT,
                'account_name': AP_ACCRUAL_NAME,
                'description': je_desc,
                'reference': 'RECURRING',
                'debit': 0,
                'credit': hist['estimated_amount'],
                'vendor': '[Historical Recurring]',
                'invoice_number': '',
                'source': 'historical',
            })
            _covered.add(hist['account_code'])
            je_num += 1

    # ── Layer 5: Payroll bonus accruals (Kardin-driven) ─────────────────────
    # Monthly bonus component = (Kardin annual ÷ 12) − standard payroll month.
    # Posts every month UNLESS GL already shows the actual bonus payment.
    if gl_data and kardin_records:
        # Derive period month from period string (e.g., 'Mar-2026' -> 3)
        _month_num = 0
        _month_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,
                          Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
        if period:
            import re as _re
            _m = _re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', period)
            if _m:
                _month_num = _month_map.get(_m.group(1), 0)

        if _month_num:
            bonus_candidates = detect_payroll_bonus_accrual(
                gl_data, kardin_records, _month_num
            )
            # Apply sidebar overrides: replace Kardin amount for any account
            # where the user supplied an explicit value.
            _overrides = bonus_overrides or {}
            _override_codes = set(_overrides.keys())
            # Add override accounts that Kardin didn't detect (e.g. no Kardin file)
            for _oc, _oa in _overrides.items():
                if _oa > 0 and not any(b['account_code'] == _oc for b in bonus_candidates):
                    bonus_candidates.append({
                        'account_code':    _oc,
                        'account_name':    PAYROLL_BONUS_ACCOUNTS.get(_oc, {}).get('label', _oc),
                        'estimated_amount': _round(_oa),
                        'source':          'bonus_accrual',
                        'confidence':      'high',
                        'description':     f'Bonus accrual override (manual) — ${_oa:,.2f}',
                    })
            for bonus in bonus_candidates:
                # Replace Kardin amount with override if provided
                if bonus['account_code'] in _override_codes:
                    ovr = _overrides[bonus['account_code']]
                    if ovr <= 0:
                        continue  # Override explicitly set to $0 → skip
                    bonus = {**bonus,
                             'estimated_amount': _round(ovr),
                             'description': f'Bonus accrual override (manual) — ${ovr:,.2f}',
                             'confidence': 'high'}
                # Bonus accounts are NOT suppressed by _covered — they coexist
                # with the regular payroll proration for the same account.
                je_id   = f'BON-{je_num:04d}'
                je_desc = bonus['description']
                je_lines.append({
                    'je_number':      je_id,
                    'line':           1,
                    'date':           '',
                    'account_code':   bonus['account_code'],
                    'account_name':   bonus['account_name'],
                    'description':    je_desc,
                    'reference':      'BONUS-ACCRUAL',
                    'debit':          bonus['estimated_amount'],
                    'credit':         0,
                    'vendor':         '[Bonus Accrual]',
                    'invoice_number': '',
                    'source':         'bonus_accrual',
                    'confidence':     'high',
                })
                je_lines.append({
                    'je_number':      je_id,
                    'line':           2,
                    'date':           '',
                    'account_code':   AP_ACCRUAL_ACCOUNT,
                    'account_name':   AP_ACCRUAL_NAME,
                    'description':    je_desc,
                    'reference':      'BONUS-ACCRUAL',
                    'debit':          0,
                    'credit':         bonus['estimated_amount'],
                    'vendor':         '[Bonus Accrual]',
                    'invoice_number': '',
                    'source':         'bonus_accrual',
                    'confidence':     'high',
                })
                je_num += 1

    return je_lines


# ── Prepaid amortization schedule ───────────────────────────

def build_prepaid_amortization(nexus_data: list, close_period: str = '') -> List[Dict[str, Any]]:
    """
    Build a prepaid expense amortization schedule from Nexus invoices whose
    service period spans more than one calendar month.

    For each qualifying invoice, produces one amortization line per month:
      - current period month  → expense account (normal accrual, not prepaid)
      - future months         → prepaid asset to be released in later months

    Args:
        nexus_data: Parsed Nexus records (from nexus_accrual.parse())
        close_period: Accounting period string e.g. 'Mar-2026'

    Returns:
        List of dicts:
          vendor, invoice_number, description, service_start, service_end,
          total_amount, monthly_amount, amort_month (date), period_label,
          gl_account_number, gl_account, is_current_period, month_index
    """
    lines = []

    # Parse close_period to determine current month
    close_month = None
    if close_period:
        month_map = dict(Jan=1, Feb=2, Mar=3, Apr=4, May=5, Jun=6,
                         Jul=7, Aug=8, Sep=9, Oct=10, Nov=11, Dec=12)
        for mn, mv in month_map.items():
            if mn in close_period:
                year_m = None
                import re
                yr = re.search(r'(\d{4})', close_period)
                if yr:
                    year_m = int(yr.group(1))
                if year_m:
                    close_month = date(year_m, mv, 1)
                break

    for inv in nexus_data:
        if not inv.get('is_prepaid'):
            continue

        svc_start = inv.get('service_start')
        svc_end = inv.get('service_end')
        total_months = inv.get('prepaid_months', 1)
        if not svc_start or not svc_end or total_months <= 1:
            continue

        total_amount = inv.get('amount', 0)
        monthly_amount = _round(total_amount / total_months)
        # Distribute any rounding to first month
        rounding_adj = _round(total_amount - monthly_amount * total_months)

        vendor = inv.get('vendor', '')
        inv_num = inv.get('invoice_number', '')
        desc = inv.get('line_description', '')
        gl_acct_num = inv.get('gl_account_number', inv.get('gl_account', ''))
        gl_acct = inv.get('gl_account', '')

        current_month_start = date(svc_start.year, svc_start.month, 1)
        for i in range(total_months):
            amort_month = current_month_start + relativedelta(months=i)
            month_amt = monthly_amount + (rounding_adj if i == 0 else 0)
            period_label = amort_month.strftime('%b-%Y')
            is_current = (close_month is not None and
                          amort_month.year == close_month.year and
                          amort_month.month == close_month.month)

            lines.append({
                'vendor': vendor,
                'invoice_number': inv_num,
                'description': desc,
                'service_start': svc_start,
                'service_end': svc_end,
                'total_amount': total_amount,
                'monthly_amount': month_amt,
                'amort_month': amort_month,
                'period_label': period_label,
                'gl_account_number': gl_acct_num,
                'gl_account': gl_acct,
                'is_current_period': is_current,
                'month_index': i + 1,
                'total_months': total_months,
            })

    return lines


def write_prepaid_amortization_tab(wb: Workbook, amort_lines: List[Dict],
                                   period: str = '', property_name: str = ''):
    """
    Add a 'Prepaid Amortization' tab to an existing workbook.
    Shows one row per invoice per month with current period highlighted.
    """
    ws = wb.create_sheet('Prepaid Amortization')
    AMBER = 'FFF2CC'
    GREEN_LIGHT = 'E2EFDA'

    row = 1
    c = ws.cell(row=row, column=1, value=f'Prepaid Expense Amortization Schedule — {property_name}')
    c.font = Font(name='Calibri', size=14, bold=True, color=DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=10)
    row += 1

    c = ws.cell(row=row, column=1,
                value=f'Period: {period}  |  Invoices with service period > 1 month  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = Font(name='Calibri', size=11, italic=True, color='666666')
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=10)
    row += 2

    # Column headers
    headers = ['Vendor', 'Invoice #', 'Description', 'GL Account',
               'Total Amount', 'Service Start', 'Service End', 'Total Months',
               'Period', 'Monthly Amount']
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_hdr_fill(), border=THIN_BORDER,
               align=Alignment(horizontal='center', vertical='center', wrap_text=True))
    row += 1

    # Group lines by invoice, showing all months
    for line in amort_lines:
        is_cur = line.get('is_current_period', False)
        fill_color = AMBER if is_cur else None
        fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type='solid') if fill_color else None

        vals = [
            line['vendor'],
            line['invoice_number'],
            line['description'],
            f"{line['gl_account_number']} — {line['gl_account'].split('(')[0].strip()}",
            line['total_amount'] if line['month_index'] == 1 else '',  # Only show on first row
            line['service_start'].strftime('%m/%d/%Y') if line['service_start'] else '',
            line['service_end'].strftime('%m/%d/%Y') if line['service_end'] else '',
            line['total_months'] if line['month_index'] == 1 else '',
            line['period_label'] + (' ← CURRENT' if is_cur else ''),
            line['monthly_amount'],
        ]
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=row, column=ci, value=v)
            c.border = THIN_BORDER
            if fill:
                c.fill = fill
            if ci == 5 and v != '':
                c.number_format = '$#,##0.00'
            if ci == 10:
                c.number_format = '$#,##0.00'
                if is_cur:
                    c.font = Font(name='Calibri', size=11, bold=True)
        row += 1

    # Summary: total current period prepaid expense
    current_total = sum(l['monthly_amount'] for l in amort_lines if l.get('is_current_period'))
    future_total = sum(l['monthly_amount'] for l in amort_lines if not l.get('is_current_period'))
    row += 1
    ws.cell(row=row, column=9, value='Current Period Total').font = Font(name='Calibri', size=11, bold=True)
    c = ws.cell(row=row, column=10, value=current_total)
    c.number_format = '$#,##0.00'
    c.font = Font(name='Calibri', size=11, bold=True)
    c.border = DOUBLE_BOTTOM
    row += 1
    ws.cell(row=row, column=9, value='Future Periods (Prepaid Asset)').font = Font(name='Calibri', size=11, italic=True)
    c = ws.cell(row=row, column=10, value=future_total)
    c.number_format = '$#,##0.00'
    c.font = Font(name='Calibri', size=11, italic=True)

    # Note explaining prepaid accounting
    row += 2
    note = (
        'Note: Current period amounts are expensed via accrual JE (DR expense / CR accrued liabilities). '
        'Future period amounts are recorded as prepaid assets (DR prepaid / CR cash) upon payment, '
        'then amortized monthly (DR expense / CR prepaid).'
    )
    c = ws.cell(row=row, column=1, value=note)
    c.font = Font(name='Calibri', size=10, italic=True, color='666666')
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=10)
    c.alignment = Alignment(wrap_text=True)
    ws.row_dimensions[row].height = 30

    # Column widths
    widths = [25, 15, 40, 35, 14, 14, 14, 10, 18, 16]
    for ci, w in enumerate(widths, 1):
        ws.column_dimensions[chr(64 + ci)].width = w

    ws.sheet_properties.tabColor = 'ED7D31'  # Orange for prepaid


# ── Prepaid release JEs from ledger ─────────────────────────

def build_prepaid_release_je(ledger_amort_lines: List[Dict],
                              period: str = '',
                              je_start: int = 1) -> List[Dict]:
    """
    Convert prepaid ledger amortization lines (month 2+) into JE line dicts.

    Each entry:
      DR  [expense account]       monthly_amount   (releasing prepaid to expense)
      CR  130000 Prepaid Expenses monthly_amount

    Args:
        ledger_amort_lines: from prepaid_ledger.get_current_amortization()
        period: close period string
        je_start: starting JE number (to avoid collisions with Nexus JEs)

    Returns list of JE line dicts compatible with generate_yardi_je_import()
    """
    je_lines = []
    je_num = je_start

    for item in ledger_amort_lines:
        vendor      = str(item.get('vendor', '') or '')
        inv_num     = str(item.get('invoice_number', '') or '')
        desc        = str(item.get('description', '') or '')
        gl_acct     = str(item.get('gl_account_number', '') or '')
        amount      = item.get('monthly_amount', 0) or 0
        period_lbl  = item.get('period_label', period)
        month_idx   = item.get('month_index', '')
        total_mo    = item.get('total_months', '')

        if amount == 0:
            continue

        je_id   = f"PPD-{je_num:04d}"
        je_desc = f"Prepaid amortization — {vendor} #{inv_num} ({period_lbl}, mo {month_idx}/{total_mo})"

        # DR: Expense account
        je_lines.append({
            'je_number':      je_id,
            'line':           1,
            'date':           period_lbl,
            'account_code':   gl_acct,
            'account_name':   desc[:40],
            'description':    je_desc,
            'reference':      inv_num,
            'debit':          abs(amount),
            'credit':         0,
            'vendor':         vendor,
            'invoice_number': inv_num,
            'source':         'prepaid_ledger',
        })
        # CR: Prepaid asset
        je_lines.append({
            'je_number':      je_id,
            'line':           2,
            'date':           period_lbl,
            'account_code':   PREPAID_ASSET_ACCOUNT,
            'account_name':   PREPAID_ASSET_NAME,
            'description':    je_desc,
            'reference':      inv_num,
            'debit':          0,
            'credit':         abs(amount),
            'vendor':         vendor,
            'invoice_number': inv_num,
            'source':         'prepaid_ledger',
        })
        je_num += 1

    return je_lines


# ── Generate Yardi JE import file ────────────────────────────

def generate_yardi_je_import(je_lines: List[Dict], output_path: str,
                              period: str = '', property_name: str = '') -> str:
    """
    Generate a Yardi-compatible journal entry import file (Excel).

    Yardi JE import expects columns:
      Property, Journal #, Date, Account, Description, Reference, Debit, Credit

    Args:
        je_lines: List of JE line dicts from build_accrual_entries()
        output_path: Where to write the Excel file
        period: Accounting period
        property_name: Property code/name

    Returns:
        Output path
    """
    wb = Workbook()
    ws = wb.active
    ws.title = 'Journal Entries'

    # Header row
    headers = ['Property', 'Journal #', 'Date', 'Account', 'Description',
               'Reference', 'Debit', 'Credit']
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_hdr_fill(), border=THIN_BORDER,
               align=Alignment(horizontal='center', vertical='center'))

    # Data rows
    prop_code = property_name.split()[0] if property_name else 'REVLABS'

    for ri, line in enumerate(je_lines, 2):
        alt_fill = PatternFill(start_color=LIGHT_GRAY, end_color=LIGHT_GRAY, fill_type='solid') if ri % 2 == 0 else None

        ws.cell(row=ri, column=1, value=prop_code)
        ws.cell(row=ri, column=2, value=line['je_number'])
        ws.cell(row=ri, column=3, value=line['date'])
        ws.cell(row=ri, column=4, value=line['account_code'])
        ws.cell(row=ri, column=5, value=line['description'])
        ws.cell(row=ri, column=6, value=line['reference'])

        c_dr = ws.cell(row=ri, column=7, value=line['debit'])
        c_dr.number_format = '$#,##0.00'
        c_cr = ws.cell(row=ri, column=8, value=line['credit'])
        c_cr.number_format = '$#,##0.00'

        for ci in range(1, 9):
            ws.cell(row=ri, column=ci).border = THIN_BORDER
            if alt_fill:
                ws.cell(row=ri, column=ci).fill = alt_fill

    # Totals row
    total_row = len(je_lines) + 2
    ws.cell(row=total_row, column=6, value='TOTAL').font = Font(name='Calibri', size=11, bold=True)
    total_dr = sum(l['debit'] for l in je_lines)
    total_cr = sum(l['credit'] for l in je_lines)
    c_dr = ws.cell(row=total_row, column=7, value=total_dr)
    c_dr.number_format = '$#,##0.00'
    c_dr.font = Font(name='Calibri', size=11, bold=True)
    c_dr.border = DOUBLE_BOTTOM
    c_cr = ws.cell(row=total_row, column=8, value=total_cr)
    c_cr.number_format = '$#,##0.00'
    c_cr.font = Font(name='Calibri', size=11, bold=True)
    c_cr.border = DOUBLE_BOTTOM

    # Validation check
    balance_row = total_row + 1
    ws.cell(row=balance_row, column=6, value='Balance Check').font = Font(name='Calibri', size=10, italic=True)
    diff = total_dr - total_cr
    c_bal = ws.cell(row=balance_row, column=7, value=diff)
    c_bal.number_format = '$#,##0.00'
    c_bal.font = Font(name='Calibri', size=10, italic=True,
                      color='008000' if abs(diff) < 0.01 else 'FF0000')

    # Auto column widths
    for col in range(1, 9):
        letter = chr(64 + col)
        best = 12
        for cell in ws[letter]:
            try:
                if cell.value:
                    best = max(best, len(str(cell.value)) + 2)
            except:
                pass
        ws.column_dimensions[letter].width = min(best, 45)

    wb.save(output_path)
    return output_path


# ── Generate Yardi CSV import (exact Yardi format) ────────────

def generate_yardi_je_csv(je_lines: List[Dict], output_path: str,
                           period: str = '', property_code: str = 'revlabpm') -> str:
    """
    Generate a Yardi-compatible journal entry import CSV.

    Format (no headers, comma-delimited):
      J, batch#, , , date, date, , description, property_code, signed_amount,
      gl_account, , , , reference, , , Standard Journal Display Type

    Positive amount = Debit, Negative amount = Credit.
    Each unique je_number gets its own sequential batch number.

    Args:
        je_lines:      List of JE line dicts from build_accrual_entries()
        output_path:   Where to write the .csv file
        period:        Accounting period label (e.g. 'Mar-2026') — used to derive date
        property_code: Yardi property code (default 'revlabpm')

    Returns:
        output_path
    """
    import csv
    from datetime import datetime, date
    from calendar import monthrange

    # Derive period end date from period string (e.g. 'Mar-2026' → 03/31/2026)
    period_date = ''
    try:
        dt = datetime.strptime(period, '%b-%Y')
        last_day = monthrange(dt.year, dt.month)[1]
        period_date = date(dt.year, dt.month, last_day).strftime('%m/%d/%Y')
    except Exception:
        period_date = datetime.now().strftime('%m/%d/%Y')

    # Assign sequential batch numbers per unique JE
    batch_map = {}
    batch_counter = 1
    for line in je_lines:
        je_num = line.get('je_number', '')
        if je_num not in batch_map:
            batch_map[je_num] = batch_counter
            batch_counter += 1

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for line in je_lines:
            je_num   = line.get('je_number', '')
            batch    = batch_map.get(je_num, 1)
            desc     = line.get('description', '')[:60]
            ref      = line.get('reference', '') or je_num
            gl_acct  = line.get('account_code', '')
            debit    = line.get('debit', 0) or 0
            credit   = line.get('credit', 0) or 0
            # Signed amount: positive = DR, negative = CR
            amount   = debit - credit

            writer.writerow([
                'J',         # col 1: type
                batch,       # col 2: batch/JE number
                '',          # col 3: empty
                '',          # col 4: empty
                period_date, # col 5: reference date
                period_date, # col 6: period date
                '',          # col 7: empty
                desc,        # col 8: description
                property_code,  # col 9: property code
                amount,      # col 10: signed amount
                gl_acct,     # col 11: GL account
                '',          # col 12: empty
                '',          # col 13: empty
                '',          # col 14: empty
                ref,         # col 15: reference
                '',          # col 16: empty
                '',          # col 17: empty
                'Standard Journal Display Type',  # col 18
            ])

    return output_path


# ── Add review tab to workpapers ─────────────────────────────

def write_accrual_entries_workpaper_tab(wb: Workbook, je_lines: List[Dict],
                                         period: str = '', property_name: str = ''):
    """
    Add an 'Accrual Entries' review tab to an existing workbook.
    Shows JE detail with DR/CR, grouped by vendor, for review before posting.

    Args:
        wb: Existing workbook to add the tab to
        je_lines: List of JE line dicts from build_accrual_entries()
        period: Accounting period
        property_name: Property name
    """
    ws = wb.create_sheet('Accrual Entries')

    # Title
    row = 1
    c = ws.cell(row=row, column=1, value=f'Accrual Journal Entries — {property_name}')
    c.font = Font(name='Calibri', size=14, bold=True, color=DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=9)
    row += 1

    c = ws.cell(row=row, column=1,
                value=f'Period: {period}  |  CR Account: {AP_ACCRUAL_ACCOUNT} {AP_ACCRUAL_NAME}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = Font(name='Calibri', size=11, italic=True, color='666666')
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=9)
    row += 1

    # Summary
    total_entries = len([l for l in je_lines if l['line'] == 1])
    total_amount = sum(l['debit'] for l in je_lines)
    c = ws.cell(row=row, column=1,
                value=f'Total Entries: {total_entries}  |  Total Amount: ${total_amount:,.2f}')
    c.font = Font(name='Calibri', size=11, bold=True)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=9)
    row += 2

    # Column headers
    headers = ['JE #', 'Line', 'Vendor', 'Invoice #', 'Date',
               'Account', 'Description', 'Debit', 'Credit']
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_hdr_fill(), border=THIN_BORDER,
               align=Alignment(horizontal='center', vertical='center', wrap_text=True))
    row += 1

    # Data rows
    current_je = None
    for i, line in enumerate(je_lines):
        alt = (i // 2) % 2 == 1  # Alternate every JE pair
        fill = PatternFill(start_color=LIGHT_GRAY, end_color=LIGHT_GRAY, fill_type='solid') if alt else None

        # JE group separator
        if line['je_number'] != current_je:
            current_je = line['je_number']

        ws.cell(row=row, column=1, value=line['je_number'])
        ws.cell(row=row, column=2, value=line['line'])
        ws.cell(row=row, column=3, value=line['vendor'] if line['line'] == 1 else '')
        ws.cell(row=row, column=4, value=line['invoice_number'] if line['line'] == 1 else '')
        ws.cell(row=row, column=5, value=line['date'] if line['line'] == 1 else '')
        ws.cell(row=row, column=6, value=line['account_code'])

        # Shorten description for CR line
        desc = line['description'] if line['line'] == 1 else f"  CR {AP_ACCRUAL_ACCOUNT}"
        ws.cell(row=row, column=7, value=desc)

        c_dr = ws.cell(row=row, column=8, value=line['debit'] if line['debit'] > 0 else '')
        if line['debit'] > 0:
            c_dr.number_format = '$#,##0.00'

        c_cr = ws.cell(row=row, column=9, value=line['credit'] if line['credit'] > 0 else '')
        if line['credit'] > 0:
            c_cr.number_format = '$#,##0.00'

        for ci in range(1, 10):
            ws.cell(row=row, column=ci).border = THIN_BORDER
            if fill:
                ws.cell(row=row, column=ci).fill = fill

        row += 1

    # Totals
    row += 1
    ws.cell(row=row, column=7, value='TOTAL').font = Font(name='Calibri', size=11, bold=True)
    total_dr = sum(l['debit'] for l in je_lines)
    total_cr = sum(l['credit'] for l in je_lines)
    c_dr = ws.cell(row=row, column=8, value=total_dr)
    c_dr.number_format = '$#,##0.00'
    c_dr.font = Font(name='Calibri', size=11, bold=True)
    c_dr.border = DOUBLE_BOTTOM
    c_cr = ws.cell(row=row, column=9, value=total_cr)
    c_cr.number_format = '$#,##0.00'
    c_cr.font = Font(name='Calibri', size=11, bold=True)
    c_cr.border = DOUBLE_BOTTOM

    # Balance check
    row += 1
    diff = total_dr - total_cr
    ws.cell(row=row, column=7, value='Balance Check').font = Font(name='Calibri', size=10, italic=True)
    c = ws.cell(row=row, column=8, value=diff)
    c.number_format = '$#,##0.00'
    c.font = Font(name='Calibri', size=10, bold=True,
                  color='008000' if abs(diff) < 0.01 else 'FF0000')

    # Account summary section
    row += 3
    c = ws.cell(row=row, column=1, value='Account Summary')
    c.font = Font(name='Calibri', size=12, bold=True, color=DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=4)
    row += 1

    sum_headers = ['Account Code', 'Description', 'Total Debit', 'Entry Count']
    for ci, h in enumerate(sum_headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=Font(name='Calibri', size=11, bold=True, color='000000'),
               fill=_subhdr_fill(), border=THIN_BORDER)
    row += 1

    # Aggregate by GL account (DR side only)
    acct_totals = {}
    for line in je_lines:
        if line['debit'] > 0:
            code = line['account_code']
            if code not in acct_totals:
                acct_totals[code] = {'name': line['account_name'], 'total': 0, 'count': 0}
            acct_totals[code]['total'] += line['debit']
            acct_totals[code]['count'] += 1

    for code, data in sorted(acct_totals.items()):
        ws.cell(row=row, column=1, value=code); ws.cell(row=row, column=1).border = THIN_BORDER
        ws.cell(row=row, column=2, value=data['name']); ws.cell(row=row, column=2).border = THIN_BORDER
        c = ws.cell(row=row, column=3, value=data['total'])
        c.number_format = '$#,##0.00'
        c.border = THIN_BORDER
        ws.cell(row=row, column=4, value=data['count']); ws.cell(row=row, column=4).border = THIN_BORDER
        row += 1

    # Auto-width
    for col in range(1, 10):
        letter = chr(64 + col) if col <= 26 else 'A'
        best = 12
        for cell in ws[letter]:
            try:
                if cell.value:
                    best = max(best, len(str(cell.value)) + 2)
            except:
                pass
        ws.column_dimensions[letter].width = min(best, 50)

    ws.column_dimensions['C'].width = 25
    ws.column_dimensions['G'].width = 45
    ws.sheet_properties.tabColor = '7030A0'  # Purple for accrual entries
