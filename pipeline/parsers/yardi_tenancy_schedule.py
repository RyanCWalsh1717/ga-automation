"""
Yardi Tenancy Schedule (Rent Roll) Parser
=============================================
Parses a Yardi Tenancy Schedule export (.xlsx) into a list of active tenants,
each carrying Yardi's own stable tenant code — used as the tenant "key" for
Tenant Utility Billing instead of an app-invented slug, and to build the
current tenant list live each period instead of a static config snapshot
that goes stale as leases turn over.

Report layout (Report1 sheet):
  Row 1:    Title — 'Tenancy Schedule II'
  Row 2:    Caption — 'Property: .2540hrt  As of Date: MM/DD/YYYY  By Property ...'
            (the leading-dot code here is the Yardi REPORT SUBSET code, e.g.
            '.2540hrt' for a report combining 25 & 40 Hartwell)
  Rows 3-4: Column headers (wrapped across two rows) — Property | Building |
            Floor | Unit Code | Unit Type | Unit Area | Lease | Customer |
            Lease From | Lease To | Term | Tenancy (Years) | Lease Area |
            Annual Rent | Annual Rent/Area | Lease Type | LOC Amount/Bank
            Guarantee | Charge Code | Desc
  Data rows, one BUILDING GROUP per property, then per building:
    - Group header row:  Property filled (e.g. '25 Hartwell Ave (25hart)'),
                          every other column blank.
    - Tenant row:         Property + Floor + Unit Code + Lease all filled.
                           'Lease' combines customer name and Yardi tenant
                           code: 'National Medical Care, Inc. (t0000017)'.
                           A vacant unit has Lease == 'VACANT' — not a real
                           tenant, excluded from the result.
    - Continuation row(s): everything blank except Charge Code + Desc — an
                           additional recurring charge code on the tenant
                           row immediately above (e.g. EOPXR, ETXRC).

Only real, active (non-vacant) tenants are returned — one record per tenant
row, with its continuation rows' charge codes folded in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TenancyRecord:
    """One active tenant on the Tenancy Schedule."""
    tenant_code:   str                 # Yardi tenant code, e.g. 't0000017' — stable, unique
    tenant_name:   str                 # e.g. 'National Medical Care, Inc.'
    building_name: str                 # e.g. '25 Hartwell Ave'
    building_code: str                 # e.g. '25hart' — this building's own Yardi property code
    floor:         str = ''
    unit_code:     str = ''
    unit_area:     float = 0.0
    lease_from:    Optional[object] = None
    lease_to:      Optional[object] = None
    annual_rent:   float = 0.0
    charge_codes:  List[str] = field(default_factory=list)


@dataclass
class TenancyScheduleResult:
    """Output of parsing a Yardi Tenancy Schedule export."""
    subset_code:  str = ''                          # e.g. '.2540hrt'
    as_of_date:   str = ''                           # e.g. '06/30/2026'
    tenants:      List[TenancyRecord] = field(default_factory=list)
    _parse_error: Optional[str] = None


_BUILDING_CODE_RE = re.compile(r'\(([a-z0-9]+)\)\s*$', re.IGNORECASE)
_TENANT_CODE_RE   = re.compile(r'^(.*?)\s*\((t\d+)\)\s*$', re.IGNORECASE)


def parse(filepath: str) -> TenancyScheduleResult:
    """Parse a Yardi Tenancy Schedule .xlsx export."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        return _parse_rows(rows)
    except Exception as exc:
        return TenancyScheduleResult(_parse_error=str(exc))


def _parse_rows(rows: list) -> TenancyScheduleResult:
    subset_code = ''
    as_of_date  = ''
    for row in rows[:3]:
        caption = str(row[0] or '') if row else ''
        m = re.search(r'Property:\s*(\S+)', caption)
        if m:
            subset_code = m.group(1).strip()
        m2 = re.search(r'As of Date:\s*(\d{1,2}/\d{1,2}/\d{4})', caption)
        if m2:
            as_of_date = m2.group(1)

    tenants: List[TenancyRecord] = []
    current_building_name = ''
    current_building_code = ''
    last_tenant: Optional[TenancyRecord] = None

    # Rows 1-5 (title, caption, two-row header, blank spacer) are fixed —
    # data starts at row 6. Skip them explicitly rather than relying on
    # column-content heuristics, since the header row's own label text
    # ('Lease', 'Unit Area', ...) can otherwise be mistaken for tenant data.
    for row in rows[5:]:
        row = tuple(row) + (None,) * max(0, 19 - len(row))
        prop_col   = str(row[0] or '').strip()
        floor_col  = str(row[2] or '').strip()
        unit_col   = str(row[3] or '').strip()
        area_col   = row[5]
        lease_col  = str(row[6] or '').strip()
        from_col   = row[8]
        to_col     = row[9]
        rent_col   = row[13]
        charge_col = str(row[17] or '').strip()
        desc_col   = str(row[18] or '').strip()

        if not any((prop_col, floor_col, unit_col, lease_col, charge_col)):
            continue   # blank separator row

        # Continuation row: only charge code/desc filled — belongs to the
        # tenant row immediately above.
        if not prop_col and not lease_col and charge_col and last_tenant is not None:
            last_tenant.charge_codes.append(charge_col)
            continue

        # Building group header: Property filled, nothing else — track state
        # for tenant rows that happen not to repeat it (defensive; observed
        # real exports always repeat it on the tenant row itself too).
        if prop_col and not floor_col and not unit_col and not lease_col:
            m = _BUILDING_CODE_RE.search(prop_col)
            current_building_code = m.group(1) if m else ''
            current_building_name = _BUILDING_CODE_RE.sub('', prop_col).strip()
            last_tenant = None
            continue

        # Tenant (or vacant unit) row
        if lease_col:
            if lease_col.upper() == 'VACANT':
                last_tenant = None
                continue
            m = _TENANT_CODE_RE.match(lease_col)
            tenant_name = m.group(1).strip() if m else lease_col
            tenant_code = m.group(2).strip() if m else lease_col

            _bcode_m = _BUILDING_CODE_RE.search(prop_col) if prop_col else None
            building_code = _bcode_m.group(1) if _bcode_m else current_building_code
            building_name = (
                _BUILDING_CODE_RE.sub('', prop_col).strip() if prop_col
                else current_building_name
            )

            rec = TenancyRecord(
                tenant_code=tenant_code,
                tenant_name=tenant_name,
                building_name=building_name,
                building_code=building_code,
                floor=floor_col,
                unit_code=unit_col,
                unit_area=float(area_col or 0),
                lease_from=from_col,
                lease_to=to_col,
                annual_rent=float(rent_col or 0),
                charge_codes=[charge_col] if charge_col else [],
            )
            tenants.append(rec)
            last_tenant = rec

    return TenancyScheduleResult(subset_code=subset_code, as_of_date=as_of_date, tenants=tenants)
