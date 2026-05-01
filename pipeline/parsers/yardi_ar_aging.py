"""
Yardi AR Detail Aging Parser
=============================
Parses the Yardi Aging Detail report (.xlsx) to extract the prepayment
balance for use in the management fee cash-received calculation.

Report layout (Report1 sheet):
  Row 1:  Title   — 'Aging Detail'
  Row 2:  Caption — 'DB Caption: ...  Property: revlabpm  ...  Age As Of: MM/DD/YYYY  Post To: MM/YYYY'
  Row 3:  Column headers (top half of split header)
  Row 4:  Column headers (bottom half)
  ...
  Per-tenant section:
    Tenant group header  — ('Tenant Name (tXXXXXXX)', ...)
    C-XXXX rows         — charge entries
    R-XXXX rows         — receipt entries (Prepay rows land in Pre-payments col)
    Tenant subtotal     — (None, None, 'Tenant Name', None, ..., subtotals)
  ...
  Property total row:   ('revlabpm', ...)
  Grand Total row:      ('Grand Total', ...)

Column layout (0-based, standard Yardi Aging Detail format):
  0  Property
  1  Customer
  2  Lease (Tenant Name)
  3  Status
  4  Tran#
  5  Charge Code
  6  Date
  7  Month
  8  Current Owed
  9  0-30 Owed
  10 31-60 Owed
  11 61-90 Owed
  12 Over 90 Owed
  13 Pre-payments       ← key column; negative values = unapplied tenant credits
  14 Total Owed

The "Pre-payments" column header spans two rows ("Pre-" / "payments").

Management fee usage:
  net_receipts = receivable_detail.total_receipts − ar_aging.prepayment_balance
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


# ── Result dataclasses ─────────────────────────────────────────────────────────

@dataclass
class ARAgingTenant:
    tenant_name:  str
    current_owed: float
    prepayments:  float   # negative in Yardi (unapplied credits)
    total_owed:   float


@dataclass
class ARAgingResult:
    property_code:      str
    period:             str                       # e.g. '01/2026'
    as_of_date:         str                       # e.g. '01/31/2026'
    prepayment_balance: float                     # abs(Grand Total Pre-payments) — amount to subtract
    grand_total_owed:   float
    per_tenant:         List[ARAgingTenant] = field(default_factory=list)
    _parse_error:       Optional[str] = None
    _prepayments_col:   int = 13                  # detected column index


# ── Public entry point ─────────────────────────────────────────────────────────

def parse(filepath: str) -> ARAgingResult:
    """
    Parse a Yardi Aging Detail .xlsx file.

    Returns ARAgingResult where prepayment_balance = abs(Grand Total Pre-payments).
    This is the amount to subtract from Receivable Detail total_receipts to arrive
    at the management fee cash-received basis.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        rows = [
            tuple(c for c in row)
            for row in ws.iter_rows(values_only=True)
            if any(c is not None for c in row)
        ]
        return _parse_rows(rows)
    except Exception as exc:
        return ARAgingResult(
            property_code='',
            period='',
            as_of_date='',
            prepayment_balance=0.0,
            grand_total_owed=0.0,
            _parse_error=str(exc),
        )


# ── Internal parsing ───────────────────────────────────────────────────────────

def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_rows(rows: list) -> ARAgingResult:
    # ── Extract period and as_of_date from caption row ─────────────────────────
    period        = ''
    as_of_date    = ''
    property_code = 'revlabpm'

    for row in rows[:5]:
        caption = str(row[0] or '')
        # "Age As Of: 01/31/2026  Post To: 01/2026"
        m = re.search(r'Age\s+As\s+Of[:\s]+(\d{2}/\d{2}/\d{4})', caption, re.IGNORECASE)
        if m:
            as_of_date = m.group(1)
        m2 = re.search(r'Post\s+To[:\s]+(\d{2}/\d{4})', caption, re.IGNORECASE)
        if m2:
            period = m2.group(1)
        m3 = re.search(r'Property[:\s]+(\w+)', caption, re.IGNORECASE)
        if m3:
            property_code = m3.group(1).strip()

    # ── Detect Pre-payments column index dynamically ────────────────────────────
    # Header is split across two rows ("Pre-" on one row, "payments" on the next).
    # Scan the first 6 rows for either half of the split header.
    prepayments_col = 13  # safe default for standard Yardi Aging Detail
    for row in rows[:6]:
        for i, cell in enumerate(row):
            cell_str = str(cell or '').strip().lower()
            if cell_str in ('pre-', 'pre-payments', 'prepayments', 'payments'):
                prepayments_col = i
                break

    # ── Find Grand Total row ────────────────────────────────────────────────────
    grand_prepayments = 0.0
    grand_total_owed  = 0.0

    for row in reversed(rows):
        col0 = str(row[0] or '').strip()
        if col0 == 'Grand Total':
            if len(row) > prepayments_col:
                grand_prepayments = _safe_float(row[prepayments_col])
            if len(row) > prepayments_col + 1:
                grand_total_owed = _safe_float(row[prepayments_col + 1])
            break

    # ── Collect per-tenant subtotals ────────────────────────────────────────────
    per_tenant: List[ARAgingTenant] = []
    for row in rows:
        col0 = str(row[0] or '').strip()
        col2 = str(row[2] or '').strip()
        col3 = str(row[3] or '').strip() if len(row) > 3 else ''

        # Tenant subtotal: col0=None, col2=tenant name, col3=None/empty, has financials
        if row[0] is None and col2 and not col3 and len(row) > prepayments_col:
            # Skip header rows that match this pattern but have no numbers
            has_financials = any(isinstance(row[i], (int, float)) for i in range(8, min(15, len(row))))
            if not has_financials:
                continue
            current_owed = _safe_float(row[8]) if len(row) > 8 else 0.0
            prepay_val   = _safe_float(row[prepayments_col])
            total_owed   = _safe_float(row[prepayments_col + 1]) if len(row) > prepayments_col + 1 else 0.0
            per_tenant.append(ARAgingTenant(
                tenant_name=col2,
                current_owed=current_owed,
                prepayments=prepay_val,     # negative in Yardi = credit
                total_owed=total_owed,
            ))

    return ARAgingResult(
        property_code=property_code,
        period=period,
        as_of_date=as_of_date,
        prepayment_balance=abs(grand_prepayments),   # always positive
        grand_total_owed=grand_total_owed,
        per_tenant=per_tenant,
        _parse_error=None,
        _prepayments_col=prepayments_col,
    )
