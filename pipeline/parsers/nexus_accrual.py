"""
Parser for Nexus invoice detail reports (.xls or .xlsx format).

Accepts either the "Accrual Detail" or the full "Invoice Detail" export from
Nexus. parse() itself only excludes invoices that should never be processed
for any purpose (Rejected/Void/On Hold) — it does NOT filter down to
Layer 1's narrower accrual-ready status list (Pending/In Progress/Pending
Approval/In Yardi/Submitted for Payment/Completed). That filtering is
Layer 1's own concern (accrual_entry_generator.build_accrual_entries applies
it separately), because a "Paid" invoice still needs to reach
prepaid_ledger.merge_nexus() if its service period spans multiple months —
payment status has nothing to do with whether the expense needs amortization.

Status filtering in parse() (case-insensitive)
───────────────────────────────────────────────
EXCLUDED (never processed): Rejected, Void, On Hold
Everything else (including Paid) is returned.

Column layout: detected dynamically from the header row's text (see
_detect_columns), not hardcoded — Nexus has shipped more than one column
ordering for the "Invoice Detail" export, e.g.:
  Layout A: ['', Vendor, Property, Received Date, Invoice Number, Invoice
             Date, Line Description, GL Category, GL Account #, Invoice
             Status, Amount]
  Layout B: [Vendor, Property, Inv. No., Inv. Date, Line Description,
             GL Account Description, Line Amount, Submitted Date, Created By,
             Last Approved by, Status]
- Row 0-2: Title/blank rows before the header
- Row 3 (typically): Headers, detected by column-name keywords
- Row 4+: Data rows
  - Vendor rows have the vendor name with the property column blank
  - Invoice rows have property, dates, invoice info, and amounts
  - Subtotal/Grand Total rows are detected by searching the whole row text

The parser handles:
- Empty months (no invoices, just headers and totals)
- Months with data (multiple vendors with multiple invoices)
- Subtotal and grand total rows
- Date parsing (M/D/YYYY format)
"""

import os
import re
import xlrd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple, Any, Optional


# ── Workbook loader (handles both .xls and .xlsx) ────────────

def _load_sheet(filepath: str) -> Tuple[List[List[Any]], str]:
    """
    Load the first sheet from an .xls or .xlsx file.

    Returns (rows, sheet_name) where rows is a list of lists of raw cell
    values.  Callers iterate over rows without knowing the underlying library.

    .xls  → xlrd  (legacy Excel 97-2003 binary format)
    .xlsx → openpyxl with data_only=True (Excel 2007+ XML format)
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext == '.xlsx':
        from openpyxl import load_workbook as _openpyxl_load
        wb = _openpyxl_load(filepath, data_only=True)
        ws = wb.worksheets[0]
        rows = [list(row) for row in ws.iter_rows(values_only=True)]
        # Pad short rows so every row has the same width
        ncols = max((len(r) for r in rows), default=0)
        rows = [r + [None] * (ncols - len(r)) for r in rows]
        return rows, ws.title

    # Default: .xls via xlrd
    wb = xlrd.open_workbook(filepath)
    ws = wb.sheet_by_index(0)
    rows = [[ws.cell_value(r, c) for c in range(ws.ncols)]
            for r in range(ws.nrows)]
    return rows, ws.name


# ── Status filtering ─────────────────────────────────────────
# Layer 1 accrual detection (build_accrual_entries) only wants invoices at
# these statuses — work done or in-flight that needs an accrual JE because
# it isn't in the GL yet. That's Layer 1's OWN concern and Layer 1 already
# applies this exact list itself (accrual_entry_generator.py, status_filter
# default) as a secondary filter on whatever parse() returns.
_INCLUDE_STATUSES = frozenset({
    'in progress',
    'pending',
    'pending approval',
    'in yardi',
    'submitted for payment',
    'completed',
})

# parse() itself only excludes statuses that mean "this invoice should never
# be processed at all, for any purpose" — Rejected/Void/On Hold. It must NOT
# also exclude e.g. "Paid" here: payment status has nothing to do with
# whether a multi-month invoice needs prepaid amortization (prepaid_ledger.
# merge_nexus() consumes these same records and has no status filter of its
# own), even though a paid invoice is correctly excluded from Layer 1's own
# accrual candidates by Layer 1's separate, stricter filter.
_EXCLUDE_STATUSES = frozenset({
    'rejected',
    'void',
    'on hold',
})


def _status_included(status: str) -> bool:
    """True unless the status means this invoice should never be processed at all."""
    return (status or '').strip().lower() not in _EXCLUDE_STATUSES


# ── Column layout detection ──────────────────────────────────
# Nexus exports at least two different "Invoice Detail" column layouts:
#   Layout A (older): ['', Vendor, Property, Received Date, Invoice Number,
#                       Invoice Date, Line Description, GL Category,
#                       GL Account #, Invoice Status, Amount]
#   Layout B (current, as of 2026): [Vendor, Property, Inv. No., Inv. Date,
#                       Line Description, GL Account Description, Line Amount,
#                       Submitted Date, Created By, Last Approved by, Status]
# Rather than hardcode indices for one layout, detect each field's column by
# matching keywords in the header row text — works for both layouts and any
# future column reordering Nexus introduces.
def _detect_columns(header_row: List[Any]) -> Dict[str, Optional[int]]:
    cols: Dict[str, Optional[int]] = {
        'vendor': None, 'property': None, 'received_date': None,
        'invoice_number': None, 'invoice_date': None, 'line_description': None,
        'gl_category': None, 'gl_account': None, 'status': None, 'amount': None,
    }
    for idx, cell in enumerate(header_row):
        h = str(cell or '').strip().lower()
        if not h:
            continue
        if cols['vendor'] is None and 'vendor' in h:
            cols['vendor'] = idx
        elif cols['property'] is None and 'propert' in h:
            cols['property'] = idx
        elif cols['received_date'] is None and 'received' in h and 'date' in h:
            cols['received_date'] = idx
        elif cols['invoice_number'] is None and ('inv' in h) and ('no' in h or 'number' in h or '#' in h) and 'date' not in h:
            cols['invoice_number'] = idx
        elif cols['invoice_date'] is None and ('inv' in h) and 'date' in h:
            cols['invoice_date'] = idx
        elif cols['line_description'] is None and 'gl' not in h and 'description' in h:
            cols['line_description'] = idx
        elif cols['gl_category'] is None and 'gl' in h and 'categ' in h:
            cols['gl_category'] = idx
        elif cols['gl_account'] is None and 'gl' in h and 'account' in h:
            cols['gl_account'] = idx
        elif cols['status'] is None and 'status' in h:
            cols['status'] = idx
        elif cols['amount'] is None and 'amount' in h:
            cols['amount'] = idx
    return cols


def parse(filepath: str) -> List[Dict[str, Any]]:
    """
    Parse a Nexus invoice detail report and return records for every invoice
    that isn't Rejected/Void/On Hold — regardless of payment status.

    Accepts both the "Accrual Detail" and full "Invoice Detail" exports, with
    column positions detected from the header row rather than hardcoded (see
    _detect_columns) — Nexus has shipped more than one column ordering.

    Note: Layer 1's narrower "needs an accrual JE" status list (Pending, In
    Progress, Pending Approval, In Yardi, Submitted for Payment, Completed)
    is NOT applied here — that filtering happens downstream in
    build_accrual_entries(), since a "Paid" invoice must still reach
    prepaid_ledger.merge_nexus() if it spans multiple months.

    Args:
        filepath: Path to .xls file

    Returns:
        List of dictionaries with keys:
        - vendor: Vendor name
        - property: Property name
        - received_date: Date invoice was received (datetime or None)
        - invoice_number: Invoice number
        - invoice_date: Invoice date (datetime or None)
        - line_description: Description of invoice line
        - gl_category: GL Category (empty string if this layout has none)
        - gl_account: GL Account raw field (code+name combined, either ordering)
        - gl_account_number: Numeric GL code extracted from gl_account
        - invoice_status: Status string, unfiltered except for the dead-status exclusion above
        - amount: Amount as float
        - service_start / service_end: Parsed from description if present
        - is_prepaid: True if service period spans > 35 days
        - prepaid_months: Number of months spanned (1 if not prepaid)
    """
    rows, _sheet_name = _load_sheet(filepath)

    records = []
    current_vendor = None
    _skipped_rows: list = []   # rows that failed to parse — surfaced in validation

    # Find header row (typically row 3) and detect column positions from its text —
    # Nexus has shipped at least two different column layouts (see _detect_columns).
    header_row_idx = None
    col: Dict[str, Optional[int]] = {}
    for row_idx in range(min(10, len(rows))):
        row = rows[row_idx]
        if row_idx > 0 and 'Vendor' in str(row):
            header_row_idx = row_idx
            col = _detect_columns(row)
            break

    if header_row_idx is None or col.get('vendor') is None or col.get('property') is None:
        return records

    _vc, _pc = col['vendor'], col['property']
    _rdc, _inc, _idc = col.get('received_date'), col.get('invoice_number'), col.get('invoice_date')
    _ldc, _gcc, _gac = col.get('line_description'), col.get('gl_category'), col.get('gl_account')
    _stc, _amc = col.get('status'), col.get('amount')

    def _cell(row: list, idx: Optional[int]):
        return row[idx] if idx is not None and idx < len(row) else None

    # Parse data rows
    for row_idx in range(header_row_idx + 1, len(rows)):
        row = rows[row_idx]

        # Skip empty rows
        if all(cell == '' or cell is None for cell in row):
            continue

        # Skip subtotal / grand total rows — search the whole row rather than a
        # fixed column, since "Sub-Total" lands in a different column per layout
        # (vendor-level subtotal uses the vendor column, property-level uses the
        # property column).
        _row_text = ' '.join(str(c) for c in row if c not in (None, ''))
        if 'sub-total' in _row_text.lower() or 'grand total' in _row_text.lower():
            continue

        # Check if this is a vendor row (vendor name present, property blank)
        vendor = _cell(row, _vc)
        property_val = _cell(row, _pc)

        if vendor and not property_val:
            # This is a vendor header row
            current_vendor = vendor
            continue

        # This is an invoice detail row
        if current_vendor and property_val:
            try:
                # Parse dates
                received_date = _parse_date(_cell(row, _rdc))
                invoice_date = _parse_date(_cell(row, _idc))

                # Parse amount
                amount = _parse_amount(_cell(row, _amc)) if _amc is not None else 0.0

                gl_account_raw = str(_cell(row, _gac) or '')
                line_desc = str(_cell(row, _ldc) or '')
                svc_start, svc_end = _parse_service_period(line_desc)
                is_prepaid = _is_prepaid(svc_start, svc_end)

                invoice_status = str(_cell(row, _stc) or '').strip()

                # Status gate — skip Rejected, Void, On Hold, and any other
                # status meaning "never process this invoice." Does NOT gate
                # on Layer 1's narrower accrual-ready status list — that's
                # Layer 1's own concern, applied separately downstream, and
                # gating here would also block a "Paid" invoice from ever
                # reaching prepaid ledger detection, which cares about the
                # service period, not payment status.
                if not _status_included(invoice_status):
                    continue

                record = {
                    'vendor': str(current_vendor),
                    'property': str(property_val),
                    'received_date': received_date,
                    'invoice_number': str(_cell(row, _inc) or ''),
                    'invoice_date': invoice_date,
                    'line_description': line_desc,
                    'gl_category': str(_cell(row, _gcc) or ''),
                    'gl_account': gl_account_raw,
                    'gl_account_number': _extract_gl_account_number(gl_account_raw),
                    'invoice_status': invoice_status,
                    'amount': amount,
                    'service_start': svc_start,
                    'service_end': svc_end,
                    'is_prepaid': is_prepaid,
                    'prepaid_months': _count_months(svc_start, svc_end) if is_prepaid else 1,
                }
                records.append(record)
            except Exception as _e:
                _skipped_rows.append({'row': row_idx, 'error': str(_e), 'data': str(row)[:120]})
                continue

    if _skipped_rows:
        import warnings as _w
        _w.warn(
            f'nexus_accrual: {len(_skipped_rows)} row(s) skipped due to parse errors. '
            f'First: row {_skipped_rows[0]["row"]} — {_skipped_rows[0]["error"]}. '
            f'Check the Nexus export for malformed rows; skipped rows will NOT be accrued.',
            UserWarning,
            stacklevel=2,
        )

    return records


def validate(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate that a file has the expected Nexus accrual format.

    Args:
        filepath: Path to .xls file

    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []

    try:
        rows, sheet_name = _load_sheet(filepath)
    except Exception as e:
        return False, [f"Cannot open file: {str(e)}"]

    if not rows:
        issues.append("No sheets found in workbook")
        return False, issues

    # Check basic structure
    if len(rows) < 5:
        issues.append("File has fewer than 5 rows - might be empty or wrong format")

    # Check for header row with expected columns
    found_header = False
    for row_idx in range(min(10, len(rows))):
        row_str = ' '.join(str(cell) for cell in rows[row_idx])
        if 'Vendor' in row_str and 'Invoice' in row_str:
            found_header = True
            break

    if not found_header:
        issues.append("Could not find expected header row with 'Vendor' and 'Invoice' columns")

    # Check sheet name — accept both Accrual Detail and Invoice Detail exports
    _accepted_sheet_names = {'accrual detail', 'invoice detail', 'ap invoice detail',
                              'nexus invoice detail', 'nexus accrual detail'}
    if sheet_name.strip().lower() not in _accepted_sheet_names:
        # Warn but don't hard-fail — Nexus sometimes uses custom sheet names
        issues.append(
            f"Sheet name is '{sheet_name}' — expected 'Accrual Detail' or "
            f"'Invoice Detail'. File may still parse correctly."
        )

    return len(issues) == 0, issues


def _extract_gl_account_number(gl_account_str: str) -> str:
    """Extract the numeric GL account code from a combined GL field.

    Handles both orderings Nexus exports use:
      'Admin-Computer/Software (637370)'  — name first, code in trailing parens
      '637150 (Admin-Tenant Relations)'   — code first, name in parens

    Returns the numeric code, or the original string if no code is found.
    """
    s = gl_account_str.strip()
    m = re.search(r'\((\d+)\)\s*$', s)          # name (CODE) — code in trailing parens
    if m:
        return m.group(1)
    m = re.match(r'^(\d+)\s*\(', s)              # CODE (name) — code leads the string
    if m:
        return m.group(1)
    m = re.match(r'^(\d+)\s*$', s)               # bare numeric code, no parens at all
    if m:
        return m.group(1)
    return s


# Patterns for service period date ranges in descriptions
_DATE_FULL = r'(\d{2})\.(\d{2})\.(\d{2})'   # MM.DD.YY
_DATE_MONTH = r'(\d{2})\.(\d{2})'            # MM.YY

_RE_FULL_RANGE = re.compile(rf'{_DATE_FULL}-{_DATE_FULL}')
_RE_MONTH_RANGE = re.compile(r'(\d{2})\.(\d{2})-(\d{2})\.(\d{2})(?!\d)')

# Slash-separated date ranges, e.g. '12/10/25 - 1/6/26', '11/20/25-12/22/25'.
_RE_SLASH_FULL = re.compile(
    r'(\d{1,2})/(\d{1,2})/(\d{2,4})\s*(?:-|through|to)\s*(\d{1,2})/(\d{1,2})/(\d{2,4})',
    re.IGNORECASE,
)
# Slash range with the end year missing entirely — a real data-entry gap seen
# in Nexus invoice text (e.g. 'Yardi Accounting Software - 12/1/2025 through
# 11/3', clearly meant to run through Nov 2026 but the year got dropped). The
# day is taken literally even though it may itself be truncated (e.g. '11/3'
# instead of '11/30') — the *inclusive month count* used for amortization
# comes out the same either way, since neither day crosses a month boundary.
_RE_SLASH_PARTIAL_END = re.compile(
    r'(\d{1,2})/(\d{1,2})/(\d{2,4})\s*(?:-|through|to)\s*(\d{1,2})/(\d{1,2})(?!\s*/\s*\d)',
    re.IGNORECASE,
)
# Slash range with the START year missing — the common "stated once" style,
# e.g. '1/1 - 12/31/26 Annual Firewall, Switch, Wireless' (year applies to
# both ends). The year is shared unless the start month is AFTER the end
# month, which means the range wraps a calendar year boundary (e.g.
# '12/1 - 1/31/26' = Dec of the prior year through Jan 2026).
_RE_SLASH_PARTIAL_START = re.compile(
    r'(?<!/)(\d{1,2})/(\d{1,2})\s*(?:-|through|to)\s*(\d{1,2})/(\d{1,2})/(\d{2,4})',
    re.IGNORECASE,
)

# Quarter references, e.g. 'Q1 & Q2 2026', 'Q1-Q4 2026', 'Q3 2026'. The year
# applies to both quarters — Nexus descriptions don't span quarters across
# different years in this pattern (a true cross-year span would show up as an
# explicit date range instead, caught by the patterns above).
_RE_QUARTER_RANGE = re.compile(
    r'\bQ([1-4])\s*(?:&|-|to|through|,)?\s*(?:Q([1-4]))?\s*(\d{4})\b',
    re.IGNORECASE,
)


def _norm_year(y: int) -> int:
    """2-digit year -> 2000s; 4-digit year passed through unchanged."""
    return y if y >= 100 else 2000 + y


def _parse_service_period(description: str) -> Tuple[Optional[date], Optional[date]]:
    """Parse a service period date range from an invoice line description.

    Handles:
      MM.DD.YY-MM.DD.YY   (e.g., '02.01.26-01.31.27')
      MM.YY-MM.YY         (e.g., '03.26-05.26')
      M/D/YYYY-M/D/YYYY   (e.g., '12/10/25 - 1/6/26')
      M/D/YYYY through M/D  — end year missing, inferred by rolling forward
                              from the start date (e.g. '12/1/2025 through 11/3')
      Q1 & Q2 2026 / Q1-Q4 2026 / Q3 2026 (quarter references)

    Returns (start_date, end_date) or (None, None) if not found.
    """
    # Try full date range first: MM.DD.YY-MM.DD.YY
    m = _RE_FULL_RANGE.search(description)
    if m:
        try:
            sm, sd, sy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            em, ed, ey = int(m.group(4)), int(m.group(5)), int(m.group(6))
            start = date(2000 + sy, sm, sd)
            end = date(2000 + ey, em, ed)
            return start, end
        except ValueError:
            pass

    # Try month-year range: MM.YY-MM.YY
    m = _RE_MONTH_RANGE.search(description)
    if m:
        try:
            sm, sy = int(m.group(1)), int(m.group(2))
            em, ey = int(m.group(3)), int(m.group(4))
            start = date(2000 + sy, sm, 1)
            # End date = last day of end month
            next_month = date(2000 + ey, em, 1) + relativedelta(months=1)
            end = next_month - relativedelta(days=1)
            return start, end
        except ValueError:
            pass

    # Try slash-separated full range: M/D/YYYY - M/D/YYYY (or 2-digit years)
    m = _RE_SLASH_FULL.search(description)
    if m:
        try:
            sm, sd, sy = int(m.group(1)), int(m.group(2)), _norm_year(int(m.group(3)))
            em, ed, ey = int(m.group(4)), int(m.group(5)), _norm_year(int(m.group(6)))
            start = date(sy, sm, sd)
            end = date(ey, em, ed)
            return start, end
        except ValueError:
            pass

    # Slash range with the end year missing — infer it by rolling forward
    # from the start date: if the end month is before the start month, the
    # range wraps into the following year.
    m = _RE_SLASH_PARTIAL_END.search(description)
    if m:
        try:
            sm, sd, sy = int(m.group(1)), int(m.group(2)), _norm_year(int(m.group(3)))
            em, ed = int(m.group(4)), int(m.group(5))
            ey = sy + 1 if em < sm else sy
            start = date(sy, sm, sd)
            end = date(ey, em, ed)
            return start, end
        except ValueError:
            pass

    # Slash range with the START year missing — the year is stated once, at
    # the end, and applies to both dates unless the range wraps a calendar
    # year boundary (start month after end month).
    m = _RE_SLASH_PARTIAL_START.search(description)
    if m:
        try:
            sm, sd = int(m.group(1)), int(m.group(2))
            em, ed, ey = int(m.group(3)), int(m.group(4)), _norm_year(int(m.group(5)))
            sy = ey - 1 if sm > em else ey
            start = date(sy, sm, sd)
            end = date(ey, em, ed)
            return start, end
        except ValueError:
            pass

    # Try quarter reference: 'Q1 & Q2 2026', 'Q1-Q4 2026', 'Q3 2026'
    m = _RE_QUARTER_RANGE.search(description)
    if m:
        try:
            q1 = int(m.group(1))
            q2 = int(m.group(2)) if m.group(2) else q1
            year = int(m.group(3))
            start_month = (q1 - 1) * 3 + 1
            end_month = q2 * 3
            start = date(year, start_month, 1)
            next_month = date(year, end_month, 1) + relativedelta(months=1)
            end = next_month - relativedelta(days=1)
            return start, end
        except ValueError:
            pass

    return None, None


def _count_months(start: Optional[date], end: Optional[date]) -> int:
    """Return the number of calendar months spanned by a service period (inclusive)."""
    if not start or not end or end <= start:
        return 1
    r = relativedelta(end, start)
    return r.years * 12 + r.months + 1


def _is_prepaid(start: Optional[date], end: Optional[date]) -> bool:
    """Return True if service period spans more than one month (> ~35 days)."""
    if not start or not end:
        return False
    return (end - start).days > 35


def _parse_date(value: Any) -> Any:
    """
    Parse date value from Excel cell.

    Handles:
    - float (Excel date serial number)
    - string in M/D/YYYY format
    - datetime objects

    Returns datetime.date or None if cannot parse
    """
    if value is None or value == '':
        return None

    if isinstance(value, datetime):
        return value.date()

    # openpyxl (data_only=True) may return a bare date object for date-only cells
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, float):
        # Excel date serial number (xlrd format)
        try:
            return xlrd.xldate.xldate_as_datetime(value, 0).date()
        except Exception:
            return None

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        # Try common date formats
        for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

    return None


def _parse_amount(value: Any) -> float:
    """
    Parse amount value from Excel cell.

    Handles numbers and strings.

    Returns float or 0.0 if cannot parse
    """
    if value is None or value == '':
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return 0.0
        try:
            return float(value)
        except ValueError:
            return 0.0

    return 0.0


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        filepath = sys.argv[1]

        # Validate
        is_valid, issues = validate(filepath)
        print(f"Validation: {'PASS' if is_valid else 'FAIL'}")
        if issues:
            for issue in issues:
                print(f"  - {issue}")

        # Parse
        records = parse(filepath)
        print(f"\nTotal invoices parsed: {len(records)}")

        if records:
            total_amount = sum(r['amount'] for r in records)
            print(f"Total amount: ${total_amount:,.2f}")

            print("\nFirst 5 records:")
            for i, record in enumerate(records[:5], 1):
                print(f"  {i}. {record['vendor']} - {record['invoice_number']} - ${record['amount']:,.2f}")
        else:
            print("No invoice records found (may be empty month)")
    else:
        print("Usage: python nexus_accrual.py <filepath>")
