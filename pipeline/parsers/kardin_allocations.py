"""
Kardin Budget System — Allocations Report parser (rptAllocations, PDF).

This is a DIFFERENT Kardin export from kardin_budget.py's annual budget
.xlsx — it lists every named cost-center allocation schedule configured
for a property (e.g. "B1-B5 Split", "Split (Elevators)"), each as a set of
Cost Center ID / Cost Center / Percentage rows summing to 100%. Confirmed
on a real "Lex Labs" export 2026-08-31: Cost Center IDs here (e.g.
'lexlab-1') are the same codes used in Yardi.

Used at onboarding time to pre-fill the Property Setup "Building /
Allocation Splits" table (step 4) from a real Kardin report instead of
typing percentages in by hand. IMPORTANT: Kardin is the budgeting system,
not the AP/accrual system -- confirm these percentages match what Nexus
actually uses for cost allocation before relying on them for real
accruals (see the on-screen note in Property Setup). This parser doesn't
attempt that cross-check; it only reads what Kardin says.

Report layout (per page, repeated header boilerplate then data):
    Prepared For: ...                    Software: Kardin Budget System
    Prepared By: ...                     File: ...
    Property ID: lexlabs - lexlab 5      Revision: ...
    Property RSF: 306,239                Date (EDT): ...
    Page: 1 of 3
    Lex Labs
    Allocations
    Allocation Cost Center ID Cost Center Percentage
    B1 & B2 Water Split
    lexlab-1 lexlab-1 62.1100%
    lexlab-2 lexlab-2 37.8900%
    100.0000%
    B1-B5 Split
    lexlab-1 lexlab-1 31.8800%
    ...
    100.0000%
    lexlab-1                              <- single-cost-center "schedules"
    lexlab-1 lexlab-1 100.0000%              (self-allocation, not a real
    100.0000%                                split) are parsed but filtered
                                              out by get_multi_way_splits()

Cost Center ID and Cost Center are identical in every row observed so far
(Kardin repeats the code as its own display name) -- only the ID is kept.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class AllocationRow:
    cost_center_id: str
    pct: float   # 0-100 scale, matching this app's own "Share %" convention


@dataclass
class AllocationSchedule:
    name: str
    rows: List[AllocationRow] = field(default_factory=list)

    @property
    def total_pct(self) -> float:
        return round(sum(r.pct for r in self.rows), 4)

    @property
    def is_balanced(self) -> bool:
        return abs(self.total_pct - 100.0) <= 0.02


@dataclass
class KardinAllocationsResult:
    property_display_name: str = ''
    property_id: str = ''
    schedules: List[AllocationSchedule] = field(default_factory=list)
    _parse_error: Optional[str] = None

    def get_multi_way_splits(self) -> List[AllocationSchedule]:
        """
        Real multi-building splits only -- excludes single-cost-center
        "schedules" (e.g. 'lexlab-1' alone at 100%), which are Kardin's own
        per-cost-center identity entries, not an allocation across
        buildings. These are the only schedules worth offering for import
        into the Building/Allocation Splits table.
        """
        return [s for s in self.schedules if len(s.rows) >= 2]


_COL_HEADER = 'Allocation Cost Center ID Cost Center Percentage'
_TOTAL_RE = re.compile(r'^[\d,]+\.\d+%$')
_ROW_RE_3TOK = re.compile(r'^([\w\-]+)\s+([\w\-]+)\s+([\d,]+\.\d+)%$')
_ROW_RE_2TOK = re.compile(r'^([\w\-]+)\s+([\d,]+\.\d+)%$')


def parse(filepath: str) -> KardinAllocationsResult:
    """Parse a Kardin rptAllocations PDF export."""
    result = KardinAllocationsResult()

    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            pages_text = [p.extract_text() or '' for p in pdf.pages]
    except Exception as exc:
        result._parse_error = str(exc)
        return result

    current_name: Optional[str] = None
    current_rows: List[AllocationRow] = []

    def _flush():
        nonlocal current_name, current_rows
        if current_name is not None:
            result.schedules.append(AllocationSchedule(name=current_name, rows=current_rows))
        current_name, current_rows = None, []

    for page_text in pages_text:
        lines = [l.strip() for l in page_text.split('\n') if l.strip()]
        try:
            hdr_idx = lines.index(_COL_HEADER)
        except ValueError:
            continue   # not a recognized Allocations report page -- skip

        if not result.property_display_name and hdr_idx >= 2:
            result.property_display_name = lines[hdr_idx - 2]
        if not result.property_id:
            for l in lines[:hdr_idx]:
                # "Property ID: lexlabs - lexlab 5 Revision: 24" -- Property ID
                # and Revision share one physical row in the report's two-
                # column header, so pdfplumber concatenates them into one
                # line. Stop before the next column's own label.
                m = re.match(r'^Property ID:\s*(.+?)(?:\s+Revision:.*)?$', l)
                if m:
                    result.property_id = m.group(1).strip()
                    break

        for line in lines[hdr_idx + 1:]:
            if _TOTAL_RE.match(line):
                _flush()
                continue
            m3 = _ROW_RE_3TOK.match(line)
            if m3:
                current_rows.append(AllocationRow(cost_center_id=m3.group(1),
                                                   pct=float(m3.group(3).replace(',', ''))))
                continue
            m2 = _ROW_RE_2TOK.match(line)
            if m2:
                current_rows.append(AllocationRow(cost_center_id=m2.group(1),
                                                   pct=float(m2.group(2).replace(',', ''))))
                continue
            # Doesn't match a data row or a total -- this is a new
            # schedule's name line. Flush any schedule left open (should
            # only happen if a total line was missing/misparsed).
            _flush()
            current_name = line

    _flush()
    if not result.schedules:
        result._parse_error = 'No allocation schedules found — is this a Kardin rptAllocations export?'
    return result
