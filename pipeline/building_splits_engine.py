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
    account_schedules: Optional[Dict[str, str]] = None,
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
        account_schedules:     Optional {account_code: allocation schedule name}
                               — the APPROVED allocation each account is
                               supposed to use, read from the Kardin budget's
                               AllocationName column. The named schedule's own
                               percentages come from the uploaded Kardin
                               Allocations report (rptAllocations), which
                               populates property_config.building_splits.

                               Confirmed with Ryan 2026-09-22: the uploaded
                               allocations are the ONLY ones applied unless a
                               human deliberately specifies otherwise, which
                               should be rare. Splits observed in GL history
                               are deliberately NOT applied here — they'd
                               silently bake a historical miscoding into new
                               JEs. Their role is detection instead: QC
                               CHECK_9 flags where real coding has drifted
                               from the approved allocation, so a human
                               decides.

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
    account_schedules = account_schedules or {}

    from property_config import is_allocation_exempt

    result: List[Dict] = []
    for line in je_lines:
        _acct = str(line.get('account_code', '') or '').strip()

        # Asset-specific accounts are never split by a percentage — rent
        # follows the lease, RE tax follows the parcel, the management fee is
        # calculated, cash just follows whatever really moved (confirmed with
        # Ryan 2026-09-22). Their building assignment comes from the source
        # data, so applying a schedule here would invent a wrong number.
        # An explicit per-line override still wins: that's a human decision.
        if is_allocation_exempt(_acct, property_config) and not (line.get('_split_schedule') or '').strip():
            result.append(_strip_meta(line))
            continue

        # Priority: an explicit per-line override (a human decision) > the
        # account's own approved Kardin allocation > the property default.
        sch_name = (
            (line.get('_split_schedule') or '').strip()
            or (account_schedules.get(_acct) or '').strip()
            or default_sch
        )

        # An allocation naming ONE building (Kardin's AllocationName is the
        # building's own code, e.g. '40hart') means this cost belongs 100% to
        # that building — code the line to it rather than leaving it on the
        # parent property. These never appear in `schedules`: the Kardin
        # Allocations report's single-cost-center entries aren't splits, so
        # get_multi_way_splits() correctly leaves them out of building_splits.
        if sch_name and sch_name != _NO_SPLIT and sch_name not in schedules:
            _single = _match_single_building(sch_name, property_config)
            if _single:
                _line = _strip_meta(line)
                _line['property'] = _single.yardi_code or parent_code
                _tag = f' [{_single.name}]' if _single.name else f' [{_single.yardi_code}]'
                for _field in ('remark', 'description', 'desc'):
                    if _line.get(_field):
                        _line[_field] = str(_line[_field]) + _tag
                        break
                result.append(_line)
                continue

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


def _match_single_building(name: str, property_config):
    """
    Resolve an allocation name that refers to ONE building to that building
    (matched on its Yardi code or its name, case-insensitively). Returns None
    when the name isn't a building on this property.
    """
    _key = str(name or '').strip().lower()
    if not _key:
        return None
    for b in (getattr(property_config, 'consolidated_buildings', None) or []):
        if _key in (str(b.yardi_code or '').strip().lower(),
                    str(b.name or '').strip().lower()):
            return b
    return None


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
    import copy

    # Real JE line dicts throughout this codebase (accrual_entry_generator.py,
    # management_fee.py, app.py's manual JEs) carry 'debit'/'credit' — never
    # 'amount'. Reading/writing 'amount' here always read 0 and left every
    # deep-copied line's real debit/credit unchanged, so a split JE came out
    # as N full-amount copies (multiplying the JE by the building count)
    # instead of N proportional shares. Split whichever side (debit or
    # credit) the line actually carries; the other stays 0 through the same
    # proportional math (0 * share_pct == 0, and the last split's "remainder"
    # of 0 - 0 is still 0).
    orig_debit  = float(line.get('debit', 0) or 0)
    orig_credit = float(line.get('credit', 0) or 0)
    expanded: List[Dict] = []

    total_debit_allocated  = 0.0
    total_credit_allocated = 0.0
    for idx, split in enumerate(splits):
        is_last = (idx == len(splits) - 1)
        bldg_code = split.yardi_code.strip() if split.yardi_code.strip() else parent_code

        if is_last:
            # Absorb rounding remainder
            split_debit  = round(orig_debit - total_debit_allocated, 2)
            split_credit = round(orig_credit - total_credit_allocated, 2)
        else:
            split_debit  = round(orig_debit * split.share_pct, 2)
            split_credit = round(orig_credit * split.share_pct, 2)
            total_debit_allocated  += split_debit
            total_credit_allocated += split_credit

        new_line = copy.deepcopy(line)
        new_line['debit']    = split_debit
        new_line['credit']   = split_credit
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
