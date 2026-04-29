"""
Parser for Nexus invoice detail reports (.xls format).

Accepts either the "Accrual Detail" or the full "Invoice Detail" export from
Nexus.  When using the full detail, status filtering is applied automatically
so only accrual-worthy invoices reach the pipeline.

Status filtering (case-insensitive)
────────────────────────────────────
INCLUDED (needs accrual):
  In Progress, Pending Approval, Submitted for Payment, Completed

EXCLUDED (no accrual needed):
  Rejected, Void, On Hold  — and any other status not in the include list

Expected file format:
- Row 0: Empty
- Row 1: Title with report name + "Generated: ..." (may contain newlines)
- Row 2: Empty
- Row 3: Headers - ['', 'Vendor', 'Property', 'Received Date', 'Invoice Number',
                     'Invoice Date', 'Line Description', 'GL Category', 'GL Account #',
                     'Invoice Status', 'Amount']
- Row 4+: Data rows
  - Vendor rows have vendor name in column 1, empty columns for detail fields
  - Invoice rows have property, dates, invoice info, and amounts
  - Subtotal rows contain "Sub-Total" in the description column
  - Grand Total row contains "Grand Total" in the GL Category column
- Last rows: Metadata/summary text

The parser handles:
- Empty months (no invoices, just headers and totals)
- Months with data (multiple vendors with multiple invoices)
- Subtotal and grand total rows
- Date parsing (M/D/YYYY format)
"""

import re
import xlrd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple, Any, Optional


# ── Status filtering ─────────────────────────────────────────
# Invoices at these statuses represent work done or in-flight that needs to be
# accrued.  Everything else (Rejected, Void, On Hold, unknown) is excluded.
_INCLUDE_STATUSES = frozenset({
    'in progress',
    'pending approval',
    'submitted for payment',
    'completed',
})


def _status_included(status: str) -> bool:
    """Return True if the invoice status should be included in accruals."""
    return (status or '').strip().lower() in _INCLUDE_STATUSES


def parse(filepath: str) -> List[Dict[str, Any]]:
    """
    Parse a Nexus invoice detail report and return accrual-worthy records.

    Accepts both the "Accrual Detail" and full "Invoice Detail" exports.
    Invoices with status Rejected, Void, or On Hold are silently excluded.
    Only In Progress, Pending Approval, Submitted for Payment, and Completed
    invoices are returned.

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
        - gl_category: GL Category
        - gl_account: GL Account number
        - gl_account_number: Numeric GL code extracted from gl_account
        - invoice_status: Status string (already filtered to include list)
        - amount: Amount as float
        - service_start / service_end: Parsed from description if present
        - is_prepaid: True if service period spans > 35 days
        - prepaid_months: Number of months spanned (1 if not prepaid)
    """
    workbook = xlrd.open_workbook(filepath)
    worksheet = workbook.sheet_by_index(0)

    records = []
    current_vendor = None

    # Find header row (typically row 3)
    header_row_idx = None
    for row_idx in range(min(10, worksheet.nrows)):
        row = [worksheet.cell_value(row_idx, col_idx) for col_idx in range(worksheet.ncols)]
        if row_idx > 0 and 'Vendor' in str(row):
            header_row_idx = row_idx
            break

    if header_row_idx is None:
        return records

    # Parse data rows
    for row_idx in range(header_row_idx + 1, worksheet.nrows):
        row = [worksheet.cell_value(row_idx, col_idx) for col_idx in range(worksheet.ncols)]

        # Skip empty rows
        if all(cell == '' or cell is None for cell in row):
            continue

        # Check for subtotal or grand total rows (skip them)
        if 'Sub-Total' in str(row[6]) or 'Grand Total' in str(row[7]):
            continue

        # Check if this is a vendor row (has vendor name in column 1, empty property)
        vendor = row[1] if len(row) > 1 else None
        property_val = row[2] if len(row) > 2 else None

        if vendor and not property_val:
            # This is a vendor header row
            current_vendor = vendor
            continue

        # This is an invoice detail row
        if current_vendor and property_val:
            try:
                # Parse dates
                received_date = _parse_date(row[3]) if len(row) > 3 else None
                invoice_date = _parse_date(row[5]) if len(row) > 5 else None

                # Parse amount
                amount = _parse_amount(row[10]) if len(row) > 10 else 0.0

                gl_account_raw = str(row[8]) if len(row) > 8 else ''
                line_desc = str(row[6]) if len(row) > 6 else ''
                svc_start, svc_end = _parse_service_period(line_desc)
                is_prepaid = _is_prepaid(svc_start, svc_end)

                invoice_status = str(row[9]).strip() if len(row) > 9 else ''

                # Status gate — skip Rejected, Void, On Hold, and any other
                # non-accrual status before adding to results.
                if not _status_included(invoice_status):
                    continue

                record = {
                    'vendor': str(current_vendor),
                    'property': str(property_val),
                    'received_date': received_date,
                    'invoice_number': str(row[4]) if len(row) > 4 else '',
                    'invoice_date': invoice_date,
                    'line_description': line_desc,
                    'gl_category': str(row[7]) if len(row) > 7 else '',
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
            except Exception:
                # Skip rows with parsing errors
                continue

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
        workbook = xlrd.open_workbook(filepath)
    except Exception as e:
        return False, [f"Cannot open file: {str(e)}"]

    if not workbook.sheet_names():
        issues.append("No sheets found in workbook")
        return False, issues

    worksheet = workbook.sheet_by_index(0)

    # Check basic structure
    if worksheet.nrows < 5:
        issues.append("File has fewer than 5 rows - might be empty or wrong format")

    # Check for header row with expected columns
    found_header = False
    for row_idx in range(min(10, worksheet.nrows)):
        row = [worksheet.cell_value(row_idx, col_idx) for col_idx in range(worksheet.ncols)]
        row_str = ' '.join(str(cell) for cell in row)
        if 'Vendor' in row_str and 'Invoice' in row_str:
            found_header = True
            break

    if not found_header:
        issues.append("Could not find expected header row with 'Vendor' and 'Invoice' columns")

    # Check sheet name — accept both Accrual Detail and Invoice Detail exports
    _accepted_sheet_names = {'accrual detail', 'invoice detail', 'ap invoice detail',
                              'nexus invoice detail', 'nexus accrual detail'}
    if worksheet.name.strip().lower() not in _accepted_sheet_names:
        # Warn but don't hard-fail — Nexus sometimes uses custom sheet names
        issues.append(
            f"Sheet name is '{worksheet.name}' — expected 'Accrual Detail' or "
            f"'Invoice Detail'. File may still parse correctly."
        )

    return len(issues) == 0, issues


def _extract_gl_account_number(gl_account_str: str) -> str:
    """Extract the numeric GL account code from a string like 'Admin-Computer/Software (637370)'.
    Returns the number in parentheses, or the original string if no parens found."""
    m = re.search(r'\((\d+)\)\s*$', gl_account_str.strip())
    if m:
        return m.group(1)
    return gl_account_str


# Patterns for service period date ranges in descriptions
_DATE_FULL = r'(\d{2})\.(\d{2})\.(\d{2})'   # MM.DD.YY
_DATE_MONTH = r'(\d{2})\.(\d{2})'            # MM.YY

_RE_FULL_RANGE = re.compile(rf'{_DATE_FULL}-{_DATE_FULL}')
_RE_MONTH_RANGE = re.compile(r'(\d{2})\.(\d{2})-(\d{2})\.(\d{2})(?!\d)')


def _parse_service_period(description: str) -> Tuple[Optional[date], Optional[date]]:
    """Parse a service period date range from an invoice line description.

    Handles:
      MM.DD.YY-MM.DD.YY  (e.g., '02.01.26-01.31.27')
      MM.YY-MM.YY        (e.g., '03.26-05.26')

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

    if isinstance(value, float):
        # Excel date serial number
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
