"""
building_splits_engine.py — Pro-rata JE expansion for multi-building properties
=================================================================================
Expands a flat list of JE line dicts into per-building lines based on the
allocation schedules configured in PropertyConfig.building_splits.

Usage
-----
    from building_splits_engine import apply_building_splits

    je_lines = build_accrual_entries(...)          # existing pipeline output
    je_lines = apply_building_splits(je_lines, cfg) # expand for multi-building
    generate_etl_csv(je_lines, ...)                 # write CSV as normal

Per-line schedule control
-------------------------
Each JE line dict may carry a private '_split_schedule' key:

    '_split_schedule': None        → use cfg.default_split_schedule
    '_split_schedule': 'No Split'  → pass through unchanged (one Yardi property)
    '_split_schedule': '4-Bldg'    → use the schedule named '4-Bldg'

The '_split_schedule' key is stripped before CSV generation.

Rounding
--------
Dollar amounts are split to 2 decimal places.  The last building in each
group absorbs any rounding remainder so the sum of split lines always equals
the original line amount exactly.

Single-building properties
--------------------------
If cfg.building_splits is empty, all lines are returned unchanged.
"""

from __future__ import annotations

from typing import Dict, List, Optional


_NO_SPLIT = 'No Split'


def apply_building_splits(
    je_lines: List[Dict],
    property_config,
    default_property_code: str = '',
) -> List[Dict]:
    """
    Expand JE lines for multi-building properties.

    Args:
        je_lines:              List of JE line dicts (from build_accrual_entries
                               or supplement / manual entries).
        property_config:       PropertyConfig for the active property.
        default_property_code: Fallback PROPERTY code when a building's
                               yardi_code is blank. Defaults to
                               property_config.property_code.

    Returns:
        Expanded list of JE line dicts.  For single-building properties this
        is identical to the input.  '_split_schedule' keys are removed.
    """
    if not property_config or not property_config.is_multi_building:
        # No splits defined — pass through, just strip metadata key
        return [_strip_meta(line) for line in je_lines]

    parent_code = default_property_code or property_config.property_code
    schedules   = property_config.allocation_schedules   # {name: [BuildingSplitConfig]}
    default_sch = (property_config.default_split_schedule or '').strip()

    result: List[Dict] = []
    for line in je_lines:
        sch_name = (line.get('_split_schedule') or '').strip() or default_sch

        # "No Split" or no schedule configured → pass through unchanged
        if sch_name == _NO_SPLIT or not sch_name or sch_name not in schedules:
            result.append(_strip_meta(line))
            continue

        splits = schedules[sch_name]
        if not splits:
            result.append(_strip_meta(line))
            continue

        result.extend(_expand_line(line, splits, parent_code))

    return result


def _expand_line(
    line: Dict,
    splits: list,
    parent_code: str,
) -> List[Dict]:
    """
    Expand one JE line into N lines — one per building split.

    Rounding: amounts are split to 2dp; the last building absorbs any
    remainder to ensure the sum equals the original amount exactly.
    """
    import copy, decimal

    orig_amount = float(line.get('amount', 0) or 0)
    expanded: List[Dict] = []

    total_allocated = 0.0
    for idx, split in enumerate(splits):
        is_last = (idx == len(splits) - 1)
        bldg_code = split.yardi_code.strip() if split.yardi_code.strip() else parent_code

        if is_last:
            # Absorb rounding remainder
            split_amount = round(orig_amount - total_allocated, 2)
        else:
            split_amount = round(orig_amount * split.share_pct, 2)
            total_allocated += split_amount

        new_line = copy.deepcopy(line)
        new_line['amount']   = split_amount
        new_line['property'] = bldg_code

        # Annotate remark / description with building label for traceability
        _bldg_tag = f' [{split.name}]' if split.name else f' [{bldg_code}]'
        for _field in ('remark', 'description', 'desc'):
            if new_line.get(_field):
                new_line[_field] = str(new_line[_field]) + _bldg_tag
                break

        _strip_meta_inplace(new_line)
        expanded.append(new_line)

    return expanded


def _strip_meta(line: Dict) -> Dict:
    """Return a copy of the line with the _split_schedule key removed."""
    out = dict(line)
    out.pop('_split_schedule', None)
    return out


def _strip_meta_inplace(line: Dict) -> None:
    line.pop('_split_schedule', None)


# ── Convenience: tag a list of lines with a schedule ─────────────────────────

def tag_lines(je_lines: List[Dict], schedule: str) -> List[Dict]:
    """
    Return copies of je_lines with '_split_schedule' set to schedule.
    Useful when building supplement or manual JE entries that need a
    specific schedule (not the property default).
    """
    out = []
    for line in je_lines:
        new = dict(line)
        new['_split_schedule'] = schedule
        out.append(new)
    return out


def tag_no_split(je_lines: List[Dict]) -> List[Dict]:
    """Mark je_lines as excluded from splitting."""
    return tag_lines(je_lines, _NO_SPLIT)
