"""
Yardi Receivable Summary Parser
=================================
Parses the Yardi Receivable Summary report (.xlsx) for the management fee
cash-received calculation.

Report layout (Report1 sheet):
  Row 1:  Title   — 'Receivable Summary'
  Row 2:  Caption — 'DB Caption: ... Property: revlabspm  Month From: MM/YYYY ...'
  Rows 3-4: Column headers — Property | Charge Code | Balance Forward | Charge | Receipt | Ending Balance
  Data rows: one row per charge code (property in col 0, charge code label in col 1)
    e.g. ('revlabspm', 'BRLAB',       214388.06, 1006377.60, -1220765.66,      0)
         ('revlabspm', 'Prepayment', -328546.57,       0.00,    34383.86, -294162.71)
         ('revlabspm', 'ELECT',        43437.70,   15354.18,   -43437.70,   15354.18)
         ('revlabspm', 'UTILI',         1712.04,    1736.95,    -1712.04,    1736.95)
  Subtotal row:    (property_code, 'Subtotal', ..., total_charge, total_receipt, ending)
  Grand Total row: ('Grand Total',  None, ...,      total_charge, total_receipt, ending)

Column indices (0-based):
  0  Property
  1  Charge Code label
  2  Balance Forward
  3  Charge   (billed this period — positive)
  4  Receipt  (negative = cash received; positive = prior credit applied / reversed)
  5  Ending Balance

Management fee basis:
  Grand Total Receipts (col 4, absolute value) minus Prepayment receipts and
  Tenant Bill Back AR (TBBAR) receipts that are truly NEW cash received in
  the period (only negative rows).

  Prepayment row interpretation:
    Positive receipt  = prior prepayment credit applied to a charge
                        (already reflected in Grand Total — do NOT exclude)
    Negative receipt  = new prepayment cash received this period
                        (must exclude from management fee basis)

  Tenant Bill Back AR (TBBAR) row: excluded on the same logic — it's a
  reimbursement passed through to the property, not fee-bearing revenue.
  Confirmed with Ryan 2026-08-20.

  prepay_to_exclude    = abs(min(0, prepayment_row_receipt))
  billback_to_exclude  = abs(min(0, tbbar_row_receipt))
  net_receipts         = abs(grand_total_receipt) − prepay_to_exclude − billback_to_exclude

This parser provides the cleanest management fee basis because:
  1. Prepayment identification is explicit (a dedicated "Prepayment" row)
  2. No charge-code scanning required
  3. Grand Total is pre-computed by Yardi — no aggregation risk

The Receivable Detail is still useful for per-tenant ELECT/UTILI data
(TUB Mode b) but is no longer required for the management fee calculation
when this Summary is uploaded.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ReceivableSummaryResult:
    """Output of parsing a Yardi Receivable Summary report."""
    property_code:       str
    period:              str                  # e.g. '01/2026'
    total_charges:       float               # Grand Total Charges (absolute)
    total_receipts:      float               # Grand Total Receipts (absolute)
    prepayment_receipts: float               # New-cash prepayments excluded from fee basis
    net_receipts:        float               # total_receipts − prepayment_receipts − tenant_billback_receipts
    ending_balance:      float               # Grand Total ending AR balance
    charges_by_code:     Dict[str, float]   = field(default_factory=dict)
    # charges_by_code — upper-cased charge code → total charges billed (absolute).
    # e.g. {'BRLAB': 1006377.60, 'ELECT': 15354.18, 'UTILI': 1736.95}
    tenant_billback_receipts: float          = 0.0   # New-cash TBBAR receipts excluded from fee basis
    _parse_error:        Optional[str]       = None


# ── Public entry point ─────────────────────────────────────────────────────────

def parse(filepath: str) -> ReceivableSummaryResult:
    """
    Parse a Yardi Receivable Summary .xlsx file.

    Returns a ReceivableSummaryResult with net_receipts ready for use as
    the management fee cash-received basis.
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
        return ReceivableSummaryResult(
            property_code='',
            period='',
            total_charges=0.0,
            total_receipts=0.0,
            prepayment_receipts=0.0,
            net_receipts=0.0,
            ending_balance=0.0,
            _parse_error=str(exc),
        )


# ── Internal helpers ───────────────────────────────────────────────────────────

def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


_PREPAYMENT_KEYWORDS = ('prepay', 'prepm', 'pre-pay', 'prepayment', 'deposit')


def _is_prepayment_label(label: str) -> bool:
    """True if the charge code label indicates a prepayment/deposit row."""
    lo = str(label or '').lower().strip()
    return any(kw in lo for kw in _PREPAYMENT_KEYWORDS)


_TENANT_BILLBACK_KEYWORDS = ('tenant bill back', 'tenant billback', 'tbbar')


def _is_tenant_billback_label(label: str) -> bool:
    """True if the charge code label indicates a Tenant Bill Back AR row."""
    lo = str(label or '').lower().strip()
    return any(kw in lo for kw in _TENANT_BILLBACK_KEYWORDS)


def _extract_code(label: str) -> str:
    """
    Extract the bare charge code from a Yardi label.

    'Recovery - Electricity (ELECT   )' → 'ELECT'
    'BRLAB' → 'BRLAB'
    'Prepayment' → 'PREPAYMENT'
    """
    raw = str(label or '').strip()
    m = re.search(r'\(([A-Z0-9_\-]+)\s*\)\s*$', raw.upper())
    return m.group(1).strip() if m else raw.upper()


def _is_skip_row(col0: str, col1: str) -> bool:
    """True for header / subtotal rows that should not be treated as charge-code data."""
    texts = [col0.lower(), col1.lower()]
    skip_words = ('property', 'charge code', 'balance forward', 'subtotal',
                  'receivable summary', 'db caption')
    return any(any(sw in t for sw in skip_words) for t in texts)


# ── Parser ─────────────────────────────────────────────────────────────────────

def _parse_rows(rows: list) -> ReceivableSummaryResult:
    # ── Extract period and property from caption rows ──────────────────────────
    # Only scan the title/caption rows (first 6) and require a colon after
    # 'Property' to distinguish the caption ('Property: revlabspm') from the
    # column header row (bare word 'Property' with no colon).
    period = ''
    property_code = 'revlabspm'
    summary_by = ''
    for row in rows[:6]:
        caption = str(row[0] or '') + ' ' + (str(row[1] or '') if len(row) > 1 else '')
        m = re.search(r'Month\s+From[:\s]+(\d{2}/\d{4})', caption, re.IGNORECASE)
        if m:
            period = m.group(1)
        # Require colon so header row 'Property | Charge Code' doesn't match
        m2 = re.search(r'Property:\s*(\w+)', caption, re.IGNORECASE)
        if m2:
            property_code = m2.group(1).strip()
        m3 = re.search(r'Summary\s+By:\s*([^\[]+)', caption, re.IGNORECASE)
        if m3:
            summary_by = m3.group(1).strip()

    # Yardi's "Summary By: Tenant" layout inserts an extra Customer column
    # that shifts Charge/Receipt/Ending Balance one column over from what
    # this parser expects — it has no Charge Code column at all, so
    # Prepayment/TBBAR rows can't be identified either way. Reading it with
    # this parser silently misaligns columns and produces a plausible-looking
    # but wrong number instead of failing. Confirmed as a real case
    # 2026-08-20: the wrong file's Grand Total Charge value was misread as
    # Receipt. Only "Summary By: Charge Code" is a valid layout for this
    # parser — reject anything else instead of guessing.
    if summary_by and 'charge code' not in summary_by.lower():
        return ReceivableSummaryResult(
            property_code=property_code,
            period=period,
            total_charges=0.0,
            total_receipts=0.0,
            prepayment_receipts=0.0,
            net_receipts=0.0,
            ending_balance=0.0,
            _parse_error=(
                f'Wrong Receivable Summary layout: "Summary By: {summary_by}". '
                f'This parser requires "Summary By: Charge Code" — re-export the '
                f'Receivable Summary from Yardi grouped by Charge Code, not '
                f'{summary_by}.'
            ),
        )

    # ── Scan data rows ─────────────────────────────────────────────────────────
    grand_charges     = 0.0
    grand_receipts    = 0.0
    grand_balance     = 0.0
    prepay_raw        = 0.0   # raw (signed) Prepayment row receipt value
    prepay_found      = False
    billback_raw      = 0.0   # raw (signed) Tenant Bill Back AR row receipt value
    billback_found    = False
    charges_by_code: Dict[str, float] = {}

    for row in rows:
        # Pad to at least 6 elements for safe indexing
        row = tuple(row) + (None,) * max(0, 6 - len(row))

        col0 = str(row[0] or '').strip()
        col1 = str(row[1] or '').strip()
        col3 = row[3]   # Charge amount
        col4 = row[4]   # Receipt amount
        col5 = row[5]   # Ending Balance

        # Skip rows with no financial data
        if col3 is None and col4 is None:
            continue

        col0_lo = col0.lower()
        col1_lo = col1.lower()

        # ── Grand Total row ────────────────────────────────────────────────────
        if 'grand total' in col0_lo or 'grand total' in col1_lo:
            grand_charges  = abs(_safe_float(col3))
            grand_receipts = abs(_safe_float(col4))
            grand_balance  = _safe_float(col5)   # signed: negative = debit/outstanding AR
            continue

        # ── Skip header / subtotal rows ────────────────────────────────────────
        if _is_skip_row(col0, col1):
            continue

        # ── Identify charge code label ─────────────────────────────────────────
        # Charge code label is typically in col1; col0 holds the property code.
        # Some variants may have the label only in col0 when col1 is blank.
        charge_label = col1 if col1 else col0
        if not charge_label:
            continue

        # ── Prepayment row ─────────────────────────────────────────────────────
        if _is_prepayment_label(charge_label):
            prepay_raw   = _safe_float(col4)   # signed: positive = applied; negative = new cash
            prepay_found = True
            code_key = _extract_code(charge_label)
            charges_by_code[code_key] = (
                charges_by_code.get(code_key, 0.0) + abs(_safe_float(col3))
            )
            continue

        # ── Tenant Bill Back AR row ─────────────────────────────────────────────
        # Excluded from the fee basis the same way as Prepayment — it's a
        # reimbursement passed through to the property, not fee-bearing
        # revenue. Confirmed with Ryan 2026-08-20.
        if _is_tenant_billback_label(charge_label):
            billback_raw   = _safe_float(col4)   # signed: positive = applied; negative = new cash
            billback_found = True
            code_key = _extract_code(charge_label)
            charges_by_code[code_key] = (
                charges_by_code.get(code_key, 0.0) + abs(_safe_float(col3))
            )
            continue

        # ── Normal charge code row ─────────────────────────────────────────────
        code_key = _extract_code(charge_label)
        if col3 is not None:
            charges_by_code[code_key] = (
                charges_by_code.get(code_key, 0.0) + abs(_safe_float(col3))
            )

    # ── Prepayment / Tenant Bill Back exclusion ────────────────────────────────
    # Only negative receipts are NEW cash received this period. Positive
    # values = prior credit applied — already washed into Grand Total.
    prepay_to_exclude   = abs(min(0.0, prepay_raw))   if prepay_found   else 0.0
    billback_to_exclude = abs(min(0.0, billback_raw)) if billback_found else 0.0
    net_receipts = max(0.0, grand_receipts - prepay_to_exclude - billback_to_exclude)

    return ReceivableSummaryResult(
        property_code=property_code,
        period=period,
        total_charges=grand_charges,
        total_receipts=grand_receipts,
        prepayment_receipts=round(prepay_to_exclude, 2),
        net_receipts=round(net_receipts, 2),
        ending_balance=grand_balance,
        charges_by_code=charges_by_code,
        tenant_billback_receipts=round(billback_to_exclude, 2),
        _parse_error=None,
    )
