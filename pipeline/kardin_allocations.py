"""
Kardin Allocations Report Parser ("rptAllocations")
======================================================
Parses Kardin's own building-allocation export — a list of every named
allocation schedule configured for a property in the Kardin budget system —
so Property Setup's "Building Allocations (Kardin Report)" uploader (app.py)
can pre-fill the Building/Allocation Splits table with the real percentages
instead of Ryan typing them in by hand.

Real layout confirmed 2026-09-16 against 4 real reports (25 & 40 Hartwell,
Lex Labs, 12 & 24 Hartwell, Riverside Labs):

    Prepared For: Software: Kardin Budget System
    Prepared By: File: 2027 Budget
    Property ID: 25hart - 40hart Revision: 21
    Property RSF: 63,739 Date (EDT): 9/16/2026 11:25:41 AM
    Page: 1 of 1
    25 & 40 Hartwell
    Allocations
    Allocation Cost Center ID Cost Center Percentage
    25hart
    25hart 25hart 100.0000%
    100.0000%
    Campus Split (% per GRP)
    25hart 25hart 63.0000%
    40hart 40hart 37.0000%
    100.0000%
    Split (50/50)
    25hart 25hart 50.0000%
    40hart 40hart 50.0000%
    100.0000%

Each named schedule is its own block: a header line (the schedule name,
e.g. 'Campus Split (% per GRP)'), one row per building, and a trailing
total line (just 'NN.NNNN%' with no cost center at all) that's a sum-check,
not a row. A building row is '<Cost Center ID> <Cost Center> <Pct>%' — the
"Cost Center" (human-readable name) column can itself contain spaces (e.g.
'12hart  12 Hartwell Development  100.0000%', confirmed on a real 12 & 24
Hartwell report), so only the FIRST token (the Cost Center ID, always a
single bare code like '25hart' or 'lexlab-1') and the LAST token (the
percentage) are read — whatever sits between them is the display name and
isn't needed here, since the Building/Allocation Splits table keys off the
Yardi code, not Kardin's own label for it.

A schedule with only one row (e.g. '25hart' listing '25hart' at 100%) is a
building naming itself, not an actual split — get_multi_way_splits()
excludes those, since Property Setup's Building/Allocation Splits table
only has any use for a schedule that actually divides cost between 2+
buildings.

Multi-page reports repeat the same header block (Prepared For / Prepared
By / Property ID / Property RSF / Page / display name / "Allocations" /
column header) on every page, confirmed on a real 3-page Lex Labs report —
those lines are skipped explicitly rather than relying on them incidentally
being filtered out by the 2+ row rule later.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class AllocationRow:
    cost_center_id: str
    pct: float   # 0-100 scale (e.g. 63.0 for 63%) -- matches the "Share %"
                 # field in Property Setup's Building/Allocation Splits table


@dataclass
class AllocationSchedule:
    name: str
    rows: List[AllocationRow] = field(default_factory=list)

    @property
    def is_balanced(self) -> bool:
        return abs(sum(r.pct for r in self.rows) - 100.0) < 0.5


@dataclass
class KardinAllocationsResult:
    property_id:           str = ''
    property_display_name: str = ''
    schedules:             List[AllocationSchedule] = field(default_factory=list)
    _parse_error:          Optional[str] = None

    def get_multi_way_splits(self) -> List[AllocationSchedule]:
        """Schedules with 2+ rows -- an actual split between buildings, not
        a single building's own 100%-to-itself 'schedule'."""
        return [s for s in self.schedules if len(s.rows) >= 2]


_PCT_RE = re.compile(r'^([\d,]+\.\d+)%$')
_HEADER_PREFIXES = ('Prepared For', 'Prepared By', 'Property ID', 'Property RSF', 'Page:')


def parse(filepath: str) -> KardinAllocationsResult:
    """Parse a Kardin 'rptAllocations' PDF export."""
    result = KardinAllocationsResult()

    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            full_text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
    except Exception as exc:
        result._parse_error = str(exc)
        return result

    if 'kardin' not in full_text.lower() or 'allocations' not in full_text.lower():
        result._parse_error = 'This does not look like a Kardin Allocations report (rptAllocations).'
        return result

    m = re.search(r'Property ID:\s*(.+?)\s+Revision:', full_text)
    if m:
        result.property_id = m.group(1).strip()

    lines = [ln.strip() for ln in full_text.split('\n') if ln.strip()]

    # Property display name is the line right after "Page: N of M" and
    # before the literal "Allocations" title line, on the report's own
    # first page.
    try:
        _page_idx = next(i for i, ln in enumerate(lines) if ln.startswith('Page:'))
        _alloc_idx = next(i for i, ln in enumerate(lines) if ln == 'Allocations')
        if _alloc_idx > _page_idx:
            result.property_display_name = ' '.join(lines[_page_idx + 1:_alloc_idx])
    except StopIteration:
        pass

    # Column header line marks where the first page's schedule blocks start.
    try:
        _start = next(
            i for i, ln in enumerate(lines)
            if ln.lower().startswith('allocation cost center id cost center percentage')
        ) + 1
    except StopIteration:
        result._parse_error = 'Could not find the Allocation/Cost Center table header on this report.'
        return result

    current: Optional[AllocationSchedule] = None
    for ln in lines[_start:]:
        if ln.startswith(_HEADER_PREFIXES) or ln == 'Allocations' or ln == result.property_display_name:
            continue  # repeated page header/footer on multi-page reports
        if ln.lower() == 'allocation cost center id cost center percentage':
            continue

        # A bare "NN.NNNN%" line with nothing else is the schedule's own
        # total row -- a sum-check, not a building row. Skip it.
        if _PCT_RE.match(ln):
            continue

        parts = ln.split()
        if len(parts) >= 2 and _PCT_RE.match(parts[-1]):
            # "<cost center id> <cost center name...> <pct>%" -- a building row.
            if current is None:
                continue
            cost_center_id = parts[0]
            pct = float(_PCT_RE.match(parts[-1]).group(1).replace(',', ''))
            current.rows.append(AllocationRow(cost_center_id=cost_center_id, pct=pct))
        else:
            # A schedule name header, e.g. '25hart' or 'Campus Split (% per GRP)'.
            current = AllocationSchedule(name=ln)
            result.schedules.append(current)

    if not result.schedules:
        result._parse_error = 'Recognized as a Kardin Allocations report but found no allocation schedules.'

    return result
