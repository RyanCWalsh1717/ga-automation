"""
Parser for Nexus Invoice Detail (Paid) reports (.xls / .xlsx format).

Expected format:
  Row 1:  Title "Invoice Detail\\nGenerated: MM/DD/YYYY ..."
  Row 3:  Headers — Vendor | Property | Inv. No. | Inv. Date | Line Description |
                     GL Account Description | Line Amount | Submitted Date |
                     Created By | Last Approved by | Status
  Row 4+: Data rows — same vendor-group pattern as Nexus Accrual Detail:
            Vendor rows:   col 0 = vendor name, col 1 empty
            Invoice rows:  col 0 empty, col 1 = property
            Sub-total rows: "sub-total" in col 0 or col 1

Prepaid detection:
  A paid invoice is classified as a prepaid when its line description contains a
  date range whose span exceeds 35 days (catches annual/semi-annual contracts while
  excluding single-period utility billing cycles).

  Supported description date formats:
    MM.DD.YY-MM.DD.YY          e.g.  12.19.25-12.18.26
    MM.DD.YY - MM.DD.YY        e.g.  02.10.26 - 02.09.27
    MM/DD/YYYY - MM/DD/YYYY    e.g.  04/01/2026 - 03/31/2027
    MM/DD/YYYY-MM/DD/YYYY      e.g.  04/01/2025-03/31/2026
    MM.YY-MM.YY                e.g.  03.26-05.26  (month-year only)

Returns:
  List of invoice dicts. Prepaid invoices include:
    is_prepaid:     True
    prepaid_months: int — number of months to amortize
    service_start:  date
    service_end:    date
"""

import re
import xlrd
import openpyxl
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple


# ── Date-range patterns in line descriptions ───────────────────────────────────

# MM.DD.YY-MM.DD.YY  (with or without spaces around dash)
_RE_DOTDATE_RANGE = re.compile(
    r'(\d{2})\.(\d{2})\.(\d{2})\s*-\s*(\d{2})\.(\d{2})\.(\d{2})'
)

# MM/DD/YYYY - MM/DD/YYYY  (with or without spaces)
_RE_SLASHDATE_RANGE = re.compile(
    r'(\d{1,2})/(\d{1,2})/(\d{4})\s*-\s*(\d{1,2})/(\d{1,2})/(\d{4})'
)

# MM.YY-MM.YY  (month-year only, no day — must NOT be followed by more digits)
_RE_MONTHYEAR_RANGE = re.compile(
    r'(\d{2})\.(\d{2})\s*-\s*(\d{2})\.(\d{2})(?!\d)'
)


def parse(filepath: str) -> List[Dict[str, Any]]:
    """
    Parse a Nexus Invoice Detail (paid) report.

    Returns a list of invoice dicts with keys:
      vendor, property, invoice_number, invoice_date, received_date,
      line_description, gl_account_description, gl_account_number,
      amount, submitted_date, status,
      service_start, service_end, is_prepaid, prepaid_months
    """
    ext = str(filepath).lower().rsplit('.', 1)[-1]
    if ext == 'xlsx':
        return _parse_xlsx(filepath)
    else:
        return _parse_xls(filepath)


# ── XLS parser ─────────────────────────────────────────────────────────────────

def _parse_xls(filepath: str) -> List[Dict[str, Any]]:
    try:
        wb = xlrd.open_workbook(filepath)
    except Exception as e:
        return [{'_parse_error': str(e)}]

    ws = wb.sheet_by_index(0)
    rows = [[ws.cell_value(r, c) for c in range(ws.ncols)]
            for r in range(ws.nrows)]
    return _extract_records(rows, wb=wb)


# ── XLSX parser ────────────────────────────────────────────────────────────────

def _parse_xlsx(filepath: str) -> List[Dict[str, Any]]:
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
    except Exception as e:
        return [{'_parse_error': str(e)}]

    ws = wb.active
    rows = [[cell.value for cell in row] for row in ws.iter_rows()]
    return _extract_records(rows)


# ── Shared extraction logic ─────────────────────────────────────────────────────

def _extract_records(rows: List[List], wb=None) -> List[Dict[str, Any]]:
    # Find header row (contains 'Vendor' and 'Inv')
    header_idx = None
    for i, row in enumerate(rows):
        strs = [str(c).strip() if c else '' for c in row]
        joined = ' '.join(strs)
        if 'Vendor' in joined and ('Inv' in joined or 'Invoice' in joined):
            header_idx = i
            break

    if header_idx is None:
        return []

    records = []
    current_vendor = None

    for row in rows[header_idx + 1:]:
        if not row or all(c is None or str(c).strip() == '' for c in row):
            continue

        col0 = str(row[0]).strip() if row[0] else ''
        col1 = str(row[1]).strip() if len(row) > 1 and row[1] else ''

        # Sub-total rows — skip
        if 'sub-total' in col0.lower() or 'sub-total' in col1.lower():
            continue
        if 'grand total' in col0.lower():
            continue

        # Vendor header row: vendor in col0, property empty
        if col0 and not col1:
            current_vendor = col0
            continue

        # Invoice detail row: property in col1
        if current_vendor and col1:
            try:
                inv_date  = _parse_date(row[3] if len(row) > 3 else None, wb)
                sub_date  = _parse_date(row[7] if len(row) > 7 else None, wb)
                amount    = _parse_amount(row[6] if len(row) > 6 else None)
                gl_raw    = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                line_desc = str(row[4]).strip() if len(row) > 4 and row[4] else ''
                status    = str(row[10]).strip() if len(row) > 10 and row[10] else ''

                svc_start, svc_end = _parse_service_period(line_desc)
                is_prep   = _is_prepaid(svc_start, svc_end)
                months    = _count_months(svc_start, svc_end) if is_prep else 1

                records.append({
                    'vendor':                current_vendor,
                    'property':              col1,
                    'invoice_number':        str(row[2]).strip() if len(row) > 2 and row[2] else '',
                    'invoice_date':          inv_date,
                    'line_description':      line_desc,
                    'gl_account_description': gl_raw,
                    'gl_account_number':     _extract_gl_number(gl_raw),
                    'gl_account':            _extract_gl_name(gl_raw),
                    'amount':                amount,
                    'submitted_date':        sub_date,
                    'received_date':         sub_date,   # alias for prepaid_ledger compat
                    'status':                status,
                    'service_start':         svc_start,
                    'service_end':           svc_end,
                    'is_prepaid':            is_prep,
                    'prepaid_months':        months,
                })
            except Exception:
                continue

    return records


# ── Service period parsing ──────────────────────────────────────────────────────

def _parse_service_period(description: str) -> Tuple[Optional[date], Optional[date]]:
    """Extract (start, end) dates from a line description. Returns (None, None) if not found."""

    # 1. MM.DD.YY-MM.DD.YY  (with/without spaces)
    m = _RE_DOTDATE_RANGE.search(description)
    if m:
        try:
            sm, sd, sy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            em, ed, ey = int(m.group(4)), int(m.group(5)), int(m.group(6))
            return date(2000 + sy, sm, sd), date(2000 + ey, em, ed)
        except ValueError:
            pass

    # 2. MM/DD/YYYY - MM/DD/YYYY
    m = _RE_SLASHDATE_RANGE.search(description)
    if m:
        try:
            sm, sd, sy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            em, ed, ey = int(m.group(4)), int(m.group(5)), int(m.group(6))
            return date(sy, sm, sd), date(ey, em, ed)
        except ValueError:
            pass

    # 3. MM.YY-MM.YY  (month-year only)
    m = _RE_MONTHYEAR_RANGE.search(description)
    if m:
        try:
            sm, sy = int(m.group(1)), int(m.group(2))
            em, ey = int(m.group(3)), int(m.group(4))
            start = date(2000 + sy, sm, 1)
            end   = date(2000 + ey, em, 1) + relativedelta(months=1) - relativedelta(days=1)
            return start, end
        except ValueError:
            pass

    return None, None


def _is_prepaid(start: Optional[date], end: Optional[date]) -> bool:
    """True when service period spans more than 35 days (> 1 billing cycle)."""
    if not start or not end:
        return False
    return (end - start).days > 35


def _count_months(start: Optional[date], end: Optional[date]) -> int:
    """Number of calendar months spanned (inclusive)."""
    if not start or not end or end <= start:
        return 1
    r = relativedelta(end, start)
    return r.years * 12 + r.months + 1


# ── GL account field helpers ────────────────────────────────────────────────────

def _extract_gl_number(gl_str: str) -> str:
    """'617130 (HVAC Maint-Mat/Supplies)' → '617130'"""
    m = re.match(r'(\d{6})', gl_str.strip())
    if m:
        return m.group(1)
    # Also try trailing parens format
    m = re.search(r'\((\d+)\)\s*$', gl_str.strip())
    if m:
        return m.group(1)
    return gl_str


def _extract_gl_name(gl_str: str) -> str:
    """'617130 (HVAC Maint-Mat/Supplies)' → 'HVAC Maint-Mat/Supplies'"""
    m = re.search(r'\((.+)\)\s*$', gl_str.strip())
    if m:
        return m.group(1)
    # Remove leading account number
    cleaned = re.sub(r'^\d{6}\s*', '', gl_str.strip())
    return cleaned


# ── Date / amount helpers ───────────────────────────────────────────────────────

def _parse_date(value: Any, wb=None) -> Optional[date]:
    if value is None or value == '':
        return None
    if isinstance(value, (date, datetime)):
        return value.date() if isinstance(value, datetime) else value
    if isinstance(value, float) and wb is not None:
        try:
            return xlrd.xldate.xldate_as_datetime(value, wb.datemode).date()
        except Exception:
            pass
    if isinstance(value, (int, float)):
        try:
            return xlrd.xldate.xldate_as_datetime(value, 0).date()
        except Exception:
            pass
    if isinstance(value, str):
        for fmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d'):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
    return None


def _parse_amount(value: Any) -> float:
    if value is None or value == '':
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip().replace(',', ''))
        except ValueError:
            return 0.0
    return 0.0


# ── Quick summary helper ────────────────────────────────────────────────────────

def summarize_prepaids(records: List[Dict]) -> List[Dict]:
    """Return only the prepaid-flagged records with key fields for display."""
    return [
        {
            'vendor':         r['vendor'],
            'invoice_number': r['invoice_number'],
            'description':    r['line_description'],
            'gl_account':     r['gl_account_number'],
            'amount':         r['amount'],
            'months':         r['prepaid_months'],
            'monthly':        round(r['amount'] / max(r['prepaid_months'], 1), 2),
            'service_start':  r['service_start'],
            'service_end':    r['service_end'],
        }
        for r in records
        if r.get('is_prepaid')
    ]


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        recs = parse(sys.argv[1])
        prepaids = summarize_prepaids(recs)
        print(f'Total invoices: {len(recs)}   Prepaids detected: {len(prepaids)}')
        print()
        for p in prepaids:
            print(f"  {p['vendor']:35s} {p['invoice_number']:15s} "
                  f"${p['amount']:>10,.2f} / {p['months']}mo = ${p['monthly']:>8,.2f}/mo  "
                  f"{p['service_start']} → {p['service_end']}  [{p['gl_account']}]")
        print()
        print(f'Non-prepaid invoices: {len(recs) - len(prepaids)}')
    else:
        print('Usage: python nexus_paid_invoices.py <filepath>')
