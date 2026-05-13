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
  Grand Total Receipts (col 4, absolute value) minus Prepayment receipts that
  are truly NEW cash received in the period (only negative Prepayment row receipts).

  Prepayment row interpretation:
    Positive receipt  = prior prepayment credit applied to a charge
                        (already reflected in Grand Total — do NOT exclude)
    Negative receipt  = new prepayment cash received this period
                        (must exclude from management fee basis)

  prepay_to_exclude = abs(min(0, prepayment_row_receipt))
  net_receipts      = abs(grand_total_receipt) − prepay_to_exclude

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
    net_receipts:        float               # total_receipts − prepayment_receipts
    ending_balance:      float               # Grand Total ending AR balance
    charges_by_code:     Dict[str, float]   = field(default_factory=dict)
    # charges_by_code — upper-cased charge code → total charges billed (absolute).
    # e.g. {'BRLAB': 1006377.60, 'ELECT': 15354.18, 'UTILI': 1736.95}
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
    for row in rows[:6]:
        caption = str(row[0] or '') + ' ' + str(row[1] if len(row) > 1 else '')
        m = re.search(r'Month\s+From[:\s]+(\d{2}/\d{4})', caption, re.IGNORECASE)
        if m:
            period = m.group(1)
        # Require colon so header row 'Property | Charge Code' doesn't match
        m2 = re.search(r'Property:\s*(\w+)', caption, re.IGNORECASE)
        if m2:
            property_code = m2.group(1).strip()

    # ── Scan data rows ─────────────────────────────────────────────────────────
    grand_charges     = 0.0
    grand_receipts    = 0.0
    grand_balance     = 0.0
    prepay_raw        = 0.0   # raw (signed) Prepayment row receipt value
    prepay_found      = False
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

        # ── Normal charge code row ─────────────────────────────────────────────
        code_key = _extract_code(charge_label)
        if col3 is not None:
            charges_by_code[code_key] = (
                charges_by_code.get(code_key, 0.0) + abs(_safe_float(col3))
            )

    # ── Prepayment exclusion ───────────────────────────────────────────────────
    # Only negative Prepayment receipts are NEW cash received this period.
    # Positive values = prior credit applied — already washed into Grand Total.
    prepay_to_exclude = abs(min(0.0, prepay_raw)) if prepay_found else 0.0
    net_receipts = max(0.0, grand_receipts - prepay_to_exclude)

    return ReceivableSummaryResult(
        property_code=property_code,
        period=period,
        total_charges=grand_charges,
        total_receipts=grand_receipts,
        prepayment_receipts=round(prepay_to_exclude, 2),
        net_receipts=round(net_receipts, 2),
        ending_balance=grand_balance,
        charges_by_code=charges_by_code,
        _parse_error=None,
    )
