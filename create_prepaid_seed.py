"""
create_prepaid_seed.py
======================
One-time script to build the starting GA_Prepaid_Ledger_Updated.xlsx from
the March 2026 RCW workpaper.

Run once:
    python create_prepaid_seed.py

Output:
    GA_Prepaid_Ledger_Seed_Mar2026.xlsx

Upload this file as the "Prior Prepaid Ledger" in Pass 1 for the April close.
After April runs it will be replaced by the pipeline's own updated ledger.

Data sources in the RCW workpaper
----------------------------------
  '135150 PPD Other'   — active / completed non-insurance prepaids
  'Insurance Analysis' — property, GL, umbrella policies (GL 639110/639120/135110)
"""

import sys
import os
import re
import shutil
import tempfile
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# ── Make sure we can import the pipeline's own save() ──────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, 'pipeline'))

from prepaid_ledger import save as _ledger_save   # noqa: E402

# ── RCW file path ───────────────────────────────────────────────────────────
RCW_PATH = (
    r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents'
    r'\Portfolio\Revolution Labs\10 - Finance\Claude\March Data\JLL Final Data'
    r'\03.26 Workpapers REVLABS RCW.xlsx'
)

OUT_PATH = os.path.join(_HERE, 'GA_Prepaid_Ledger_Seed_Mar2026.xlsx')

# As of the end of March 2026 — seed period
SEED_PERIOD = 'Mar-2026'
SEED_DATE   = date(2026, 3, 31)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _ensure_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%m-%d-%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _parse_term(term_str: str):
    """
    Parse 'MM/DD/YY-MM/DD/YY' or 'MM/YY-MM/YY' or similar into (start, end) dates.
    Returns (None, None) if unparseable.
    """
    if not term_str:
        return None, None
    # Normalise separators
    term_str = str(term_str).strip().replace(' ', '')
    # Split on dash-between-date-chunks
    # Patterns to try:
    patterns = [
        # '6/12/2025-6/12/2026'
        r'(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{2,4})',
        # '03/01/25-02/28/26' (2-digit year)
        r'(\d{1,2}/\d{1,2}/\d{2})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{2})',
        # '04/01/25-03/31/26' with spaces
        r'(\d{1,2}/\d{2}/\d{2})\s*[-–]\s*(\d{1,2}/\d{2}/\d{2})',
        # '01/26-06/26' (month/year only)
        r'(\d{2}/\d{2})\s*[-–]\s*(\d{2}/\d{2})',
        # '9/2025-9/2026'
        r'(\d{1,2}/\d{4})\s*[-–]\s*(\d{1,2}/\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, term_str)
        if m:
            s, e = m.group(1), m.group(2)
            # Try to parse both
            sd = _ensure_date(s)
            ed = _ensure_date(e)
            if sd and ed:
                return sd, ed
            # Try month/year only patterns
            try:
                sd = datetime.strptime(s, '%m/%y').date().replace(day=1)
                ed_raw = datetime.strptime(e, '%m/%y').date()
                # end = last day of that month
                import calendar
                last = calendar.monthrange(ed_raw.year, ed_raw.month)[1]
                ed = ed_raw.replace(day=last)
                return sd, ed
            except ValueError:
                pass
            try:
                sd = datetime.strptime(s, '%m/%Y').date().replace(day=1)
                ed_raw = datetime.strptime(e, '%m/%Y').date()
                import calendar
                last = calendar.monthrange(ed_raw.year, ed_raw.month)[1]
                ed = ed_raw.replace(day=last)
                return sd, ed
            except ValueError:
                pass
    return None, None


# ── GL account name lookup ───────────────────────────────────────────────────
_GL_NAMES = {
    '637150': 'Admin-Tenant Relations',
    '637290': 'Admin-Telephone',
    '637330': 'Admin-Printing/Reproduction',
    '637370': 'Admin-Other Admin Expense',
    '680110': 'Management-Professional Fees',
    '680510': 'Management-Software',
    '615320': 'Maintenance-Security',
    '712210': 'Depreciation',
    '639110': 'Insurance-Property',
    '639120': 'Insurance-Liability',
    '135110': 'Prepaid Insurance',
    '213300': 'Accrued Insurance',
    '135120': 'Prepaid RE Tax',
    '641110': 'RE Tax Expense',
    '115200': 'Escrow RE Tax',
}


# ══════════════════════════════════════════════════════════════════════════════
# Read 135150 PPD Other tab
# ══════════════════════════════════════════════════════════════════════════════

def _read_135150(ws) -> list:
    """
    Read the 135150 PPD Other tab from the RCW workpaper.

    Column layout (rows 7–8 headers):
      B  = Vendor
      C  = G/L Account
      D  = Payment Date
      E  = Payment Amount
      F  = Period Covered
      G  = # Of Mos. Covered (total_months)
      H  = Exp Per Month (monthly_amount)
      I  = Months Elapsed (months_amortized)
      J  = # Of Mos. Prepaid / remaining (remaining_months)
      K  = Prepaid Balance
    """
    items = []
    for row in ws.iter_rows(min_row=9, values_only=True):
        vendor      = row[1]   # col B (0-indexed: index 1)
        gl_acct_raw = row[2]   # col C
        pay_date    = row[3]   # col D
        total_amt   = row[4]   # col E
        period_cov  = row[5]   # col F
        tot_months  = row[6]   # col G
        monthly_amt = row[7]   # col H
        months_done = row[8]   # col I
        months_left = row[9]   # col J
        # col K = prepaid balance (index 10) — not needed, recomputed

        # Skip blank / total / header rows
        if not vendor or not isinstance(vendor, str):
            continue
        vendor = vendor.strip()
        if not vendor or vendor.lower() in ('vendor', '\\', ''):
            continue
        # Skip rows that look like summary/total lines
        if str(vendor).startswith('Ending Balance') or str(vendor).startswith('Total'):
            continue

        # Normalise GL account: integers/floats → plain 6-digit string
        if isinstance(gl_acct_raw, (int, float)):
            gl_acct = str(int(gl_acct_raw))
        elif gl_acct_raw:
            # String like '637370' or '637370.0' — strip whitespace and .0 suffix only
            s = str(gl_acct_raw).strip()
            gl_acct = s[:-2] if s.endswith('.0') else s
        else:
            gl_acct = ''
        total_months_v  = int(_safe_float(tot_months))  if tot_months  else 0
        monthly_amt_v   = _safe_float(monthly_amt)
        months_amort    = int(_safe_float(months_done)) if months_done else 0
        remaining       = int(_safe_float(months_left)) if months_left else 0
        total_amount_v  = _safe_float(total_amt)

        # Parse service dates from period_covered string
        service_start, service_end = _parse_term(str(period_cov or ''))
        invoice_date_v = _ensure_date(pay_date)

        # Determine daily_rate: if total_months looks like days (>= 28 and
        # period is < 2 months), treat as day-based
        daily_rate = 0.0
        if service_start and service_end and total_months_v >= 28:
            span_months = (
                (service_end.year - service_start.year) * 12
                + service_end.month - service_start.month
            )
            if span_months <= 1:
                # day-based
                if total_months_v > 0:
                    daily_rate = round(total_amount_v / total_months_v, 6)
                total_months_v = total_months_v   # keep as days

        item = {
            'vendor':             vendor,
            'invoice_number':     '',
            'invoice_date':       invoice_date_v,
            'description':        f'{vendor} — {period_cov or ""}',
            'gl_account_number':  gl_acct,
            'gl_account':         _GL_NAMES.get(gl_acct, gl_acct),
            'total_amount':       total_amount_v,
            'monthly_amount':     monthly_amt_v,
            'service_start':      service_start,
            'service_end':        service_end,
            'total_months':       total_months_v,
            'months_amortized':   months_amort,
            'remaining_months':   remaining,
            'first_added_period': SEED_PERIOD,
            'daily_rate':         daily_rate,
        }
        items.append(item)

    return items


# ══════════════════════════════════════════════════════════════════════════════
# Read Insurance Analysis tab
# ══════════════════════════════════════════════════════════════════════════════

def _read_insurance(ws) -> list:
    """
    Extract the active insurance policies from the Insurance Analysis tab.

    Row layout (cols B–G, row 7 is header with date cols):
      col C  = Accrual marker (blank or 'Accr')
      col D  = Policy type
      col E  = Term (e.g. '6/12/2025- 6/12/2026')
      col F  = Premium amount
      col G  = Per Month amount

    We only include rows where col F (premium) > 0 and col E (term) is parseable.
    The service end must be AFTER the seed date to be 'active'.
    """
    items = []
    last_policy_type = ''   # carry-forward when col D is blank

    # Scan rows 8–30
    for r in range(8, 35):
        accrual_marker = ws.cell(row=r, column=3).value   # col C
        policy_type    = ws.cell(row=r, column=4).value   # col D
        term_str       = ws.cell(row=r, column=5).value   # col E
        premium        = ws.cell(row=r, column=6).value   # col F
        monthly        = ws.cell(row=r, column=7).value   # col G

        # Track the last non-blank policy type for blank-type active rows
        if policy_type and isinstance(policy_type, str) and policy_type.strip():
            last_policy_type = policy_type.strip()

        # Skip blank rows, accrual sub-rows, and summary rows
        if not premium:
            continue
        if isinstance(accrual_marker, str) and accrual_marker.strip().lower() == 'accr':
            continue
        if not term_str:
            continue

        # Use carried-forward policy type if this row's col D is blank
        if not policy_type or not str(policy_type).strip():
            policy_type = last_policy_type

        premium_v = _safe_float(premium)
        monthly_v = _safe_float(monthly)
        if premium_v <= 0 or monthly_v <= 0:
            continue

        service_start, service_end = _parse_term(str(term_str or ''))
        if not service_start or not service_end:
            continue
        if service_end <= SEED_DATE:
            continue  # already expired

        # Map policy type to GL accounts
        pt = str(policy_type or '').strip().lower()
        if 'property' in pt:
            gl_acct     = '639110'
            gl_name     = 'Insurance-Property'
        elif 'umbrella' in pt:
            gl_acct     = '639120'
            gl_name     = 'Insurance-Liability (Umbrella)'
        elif 'general' in pt or 'liability' in pt:
            gl_acct     = '639120'
            gl_name     = 'Insurance-General Liability'
        else:
            gl_acct     = '639120'
            gl_name     = f'Insurance-{policy_type}'

        # Compute months elapsed as of seed date
        # months from service_start to end of SEED_DATE month
        seed_end_month = date(SEED_DATE.year, SEED_DATE.month, 1)
        svc_start_month = date(service_start.year, service_start.month, 1)
        months_elapsed = (
            (seed_end_month.year - svc_start_month.year) * 12
            + seed_end_month.month - svc_start_month.month
        )
        # Total months in policy
        total_months = (
            (service_end.year - service_start.year) * 12
            + service_end.month - service_start.month
        )
        remaining = max(0, total_months - months_elapsed)

        desc = str(policy_type or '').strip()

        item = {
            'vendor':             'Insurance',
            'invoice_number':     '',
            'invoice_date':       service_start,
            'description':        desc,
            'gl_account_number':  gl_acct,
            'gl_account':         gl_name,
            'total_amount':       premium_v,
            'monthly_amount':     round(monthly_v, 2),
            'service_start':      service_start,
            'service_end':        service_end,
            'total_months':       total_months,
            'months_amortized':   months_elapsed,
            'remaining_months':   remaining,
            'first_added_period': SEED_PERIOD,
            'daily_rate':         0.0,
        }
        items.append(item)

    return items


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f'Reading: {RCW_PATH}')
    if not os.path.exists(RCW_PATH):
        print('ERROR: RCW file not found. Check the path.')
        sys.exit(1)

    # Copy to temp to avoid OneDrive lock issues
    tmp = tempfile.mktemp(suffix='.xlsx')
    shutil.copy2(RCW_PATH, tmp)

    try:
        from openpyxl import load_workbook
        wb = load_workbook(tmp, data_only=True)

        # ── Read 135150 PPD Other ──────────────────────────────────────────
        all_items   = []
        completed   = []

        if '135150 PPD Other' in wb.sheetnames:
            raw = _read_135150(wb['135150 PPD Other'])
            for item in raw:
                if item['remaining_months'] <= 0:
                    # Fully amortized — move to completed
                    item['completed_period'] = SEED_PERIOD
                    completed.append(item)
                else:
                    all_items.append(item)
            print(f'  135150 PPD Other: {len(all_items)} active, {len(completed)} completed')
        else:
            print('  WARNING: 135150 PPD Other tab not found')

        # ── Read Insurance Analysis ────────────────────────────────────────
        if 'Insurance Analysis' in wb.sheetnames:
            ins_items = _read_insurance(wb['Insurance Analysis'])
            all_items.extend(ins_items)
            print(f'  Insurance Analysis: {len(ins_items)} active policies')
        else:
            print('  WARNING: Insurance Analysis tab not found')

    finally:
        os.unlink(tmp)

    # ── Summary ────────────────────────────────────────────────────────────
    print(f'\nActive items ({len(all_items)}):')
    for item in all_items:
        print(f"  {item['vendor'][:35]:35s}  GL {item['gl_account_number']}  "
              f"  ${item['monthly_amount']:8.2f}/mo  "
              f"  {item['months_amortized']} elapsed / {item['remaining_months']} remaining")

    print(f'\nCompleted items ({len(completed)}):')
    for item in completed:
        print(f"  {item['vendor'][:35]:35s}  GL {item['gl_account_number']}  "
              f"  ${item['total_amount']:,.2f} total")

    # ── Save ───────────────────────────────────────────────────────────────
    _ledger_save(all_items, completed, OUT_PATH)
    print(f'\nSaved: {OUT_PATH}')
    print('\nNext step: upload this file as "Prior Prepaid Ledger" in Pass 1 for April.')


if __name__ == '__main__':
    main()
