"""
Yardi Receivable Detail Parser
================================
Parses the Yardi Receivable Detail report (.xlsx) for the management fee
cash-received calculation.

Report layout (Report1 sheet):
  Row 1:  Title   — 'Receivable Detail'
  Row 2:  Caption — 'DB Caption: ... Property: revlabpm  Month From: MM/YYYY ...'
  Row 3:  Column headers  — Property | Customer | Tenant | Control # | ...
  Row 4:  Sub-headers     — (blank)  | (blank)  | (blank) | '#'      | ...
  ...
  Per-tenant section:
    Tenant group header  — ('Tenant Name (tXXXXXXX)', ...)
    Balance Forward row  — (..., 'Balance Forward', 0, 0, balance, 'Balance Forward')
    C-XXXX rows         — charge entries (Charges > 0, Receipts = 0)
    R-XXXX rows         — receipt entries (Charges = 0, Receipts < 0)
    Tenant subtotal     — (None, None, 'Tenant Name', None, ..., total_charges, total_receipts, ending_balance, None)
  ...
  Property total row:   ('revlabpm', None, None, None, ..., total, total, total, None)
  Grand Total row:      ('Grand Total', None, ..., total_charges, total_receipts, ending_balance, None)

Management fee basis:
  JLL excludes Prepayment receipts (charge code containing 'prepay' / 'PREPM').
  net_receipts = abs(grand_total_receipts) - prepayment_receipts
  mgmt_fee = net_receipts × 3.00%

Column indices (0-based):
  0  Property
  1  Customer
  2  Tenant
  3  Control #
  4  Transaction Date
  5  Post Month
  6  Charge Code
  7  Charges
  8  Receipts
  9  Balance
  10 Notes
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Charge codes whose receipts are excluded from the management fee basis.
# Prepayments are advance deposits / unapplied credits — not earned receipts.
_PREPAYMENT_KEYWORDS = ('prepay', 'prepm', 'prepayment', 'pre-pay', 'deposit')


@dataclass
class TenantReceivable:
    tenant_name: str
    charges:     float
    receipts:    float          # absolute value (cash in)
    ending_balance: float


@dataclass
class ReceivableDetailResult:
    property_code:        str
    period:               str                    # e.g. '03/2026'
    total_charges:        float                  # Grand Total Charges
    total_receipts:       float                  # Grand Total Receipts (absolute)
    prepayment_receipts:  float                  # Receipts on prepayment-coded rows
    net_receipts:         float                  # total_receipts − prepayment_receipts (mgmt fee basis)
    ending_balance:       float                  # Grand Total ending AR balance
    per_tenant:           List[TenantReceivable] = field(default_factory=list)
    prepayment_detail:    List[Dict[str, Any]]   = field(default_factory=list)
    _parse_error:         Optional[str]          = None


# ── Public entry point ─────────────────────────────────────────────────────────

def parse(filepath: str) -> ReceivableDetailResult:
    """
    Parse a Yardi Receivable Detail .xlsx file.

    Returns a ReceivableDetailResult with net_receipts ready for use as
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
        return ReceivableDetailResult(
            property_code='',
            period='',
            total_charges=0.0,
            total_receipts=0.0,
            prepayment_receipts=0.0,
            net_receipts=0.0,
            ending_balance=0.0,
            _parse_error=str(exc),
        )


# ── Internal parsing ───────────────────────────────────────────────────────────

def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _is_prepayment(charge_code: str) -> bool:
    cc = str(charge_code or '').lower().strip()
    return any(kw in cc for kw in _PREPAYMENT_KEYWORDS)


def _parse_rows(rows: list) -> ReceivableDetailResult:
    # ── Extract period from caption row ───────────────────────────────────────
    period = ''
    property_code = 'revlabpm'
    for row in rows[:5]:
        caption = str(row[0] or '')
        # "Month From: 03/2026  To  03/2026"
        m = re.search(r'Month\s+From[:\s]+(\d{2}/\d{4})', caption, re.IGNORECASE)
        if m:
            period = m.group(1)
        m2 = re.search(r'Property[:\s]+(\w+)', caption, re.IGNORECASE)
        if m2:
            property_code = m2.group(1).strip()

    # ── Find Grand Total row ───────────────────────────────────────────────────
    # Last row whose col-0 is 'Grand Total' or == property_code with col-2 None
    grand_charges  = 0.0
    grand_receipts = 0.0
    grand_balance  = 0.0
    for row in reversed(rows):
        col0 = str(row[0] or '').strip()
        if col0 in ('Grand Total', property_code) and row[2] is None:
            grand_charges  = abs(_safe_float(row[7]))
            grand_receipts = abs(_safe_float(row[8]))
            grand_balance  = _safe_float(row[9])
            break

    # ── Scan all transaction rows for prepayment charge codes ─────────────────
    # C-XXXX rows have a Charge Code in col[6].
    # R-XXXX rows (actual cash) have no charge code but are grouped under their tenant.
    # Strategy:
    #   1. Track per-tenant prepayment charges billed (C-XXXX with prepay code)
    #   2. Track per-tenant total receipts (from tenant subtotal rows)
    #   3. Prepayment receipts for a tenant = receipts if ALL their charges are
    #      prepayment-coded; otherwise = charges on prepayment rows (conservative)
    prepayment_detail: List[Dict[str, Any]] = []
    prepayment_receipts = 0.0

    current_tenant = ''
    tenant_prepay_charges = 0.0
    tenant_total_charges  = 0.0
    per_tenant: List[TenantReceivable] = []

    for row in rows:
        col0 = str(row[0] or '').strip()
        col2 = str(row[2] or '').strip()
        col3 = str(row[3] or '').strip()   # Control #
        col6 = str(row[6] or '').strip()   # Charge Code
        charges  = _safe_float(row[7])
        receipts = _safe_float(row[8])     # negative = cash in

        # Tenant group header: col0 = 'revlabpm', col2 = tenant name, col3 = None
        if col0 == property_code and col2 and not col3 and not row[3]:
            current_tenant = col2
            tenant_prepay_charges = 0.0
            tenant_total_charges  = 0.0
            continue

        # Skip header / caption / title rows — but only when there's no financial data
        has_financials = (row[7] is not None or row[8] is not None)
        if col0 in ('Receivable Detail', 'Property', '') and not col3 and not has_financials:
            continue

        # C-XXXX charge rows — check for prepayment charge code
        if col3.upper().startswith('C-') and col6 and col6 not in ('Balance Forward', 'Charge', 'Code'):
            tenant_total_charges += charges
            if _is_prepayment(col6):
                tenant_prepay_charges += abs(charges)
                prepayment_detail.append({
                    'tenant':      current_tenant,
                    'control':     col3,
                    'charge_code': col6,
                    'charges':     charges,
                    'receipts':    0.0,
                })

        # Tenant subtotal row: (None, None, 'Tenant Name', None, ..., subtotal_charges, subtotal_receipts, ...)
        if row[0] is None and col2 and not col3:
            sub_charges  = abs(_safe_float(row[7]))
            sub_receipts = abs(_safe_float(row[8]))
            sub_balance  = _safe_float(row[9])

            per_tenant.append(TenantReceivable(
                tenant_name=col2,
                charges=sub_charges,
                receipts=sub_receipts,
                ending_balance=sub_balance,
            ))

            # Prepayment receipts: if this tenant had prepay charges, the
            # receipts attributable to prepayments = min(prepay_charges, sub_receipts)
            if tenant_prepay_charges > 0:
                prepay_r = min(tenant_prepay_charges, sub_receipts)
                prepayment_receipts += prepay_r
                # Tag detail rows with the estimated receipt amount
                for d in prepayment_detail:
                    if d['tenant'] == col2 and d['receipts'] == 0.0:
                        d['receipts'] = prepay_r / max(1, sum(
                            1 for x in prepayment_detail if x['tenant'] == col2
                        ))

            tenant_prepay_charges = 0.0
            tenant_total_charges  = 0.0

    net_receipts = max(0.0, grand_receipts - prepayment_receipts)

    return ReceivableDetailResult(
        property_code=property_code,
        period=period,
        total_charges=grand_charges,
        total_receipts=grand_receipts,
        prepayment_receipts=round(prepayment_receipts, 2),
        net_receipts=round(net_receipts, 2),
        ending_balance=grand_balance,
        per_tenant=per_tenant,
        prepayment_detail=prepayment_detail,
        _parse_error=None,
    )
