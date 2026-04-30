"""
Analysis Tab Builder — Revolution Labs Monthly Close
=====================================================
Handles the "copy-and-extend" logic for analysis tabs that accumulate
multi-year history from the prior workpaper.

Workflow per tab
----------------
1. Locate the prior period's version in the workbook.  After the carry-forward
   rename (e.g., "Loan Analysis" → "Mar-2026 Loan Analysis") we search by base
   name.
2. Copy all cell values into a new current-period worksheet.
3. Find the insertion point — the "Ending Balance" row that holds the SUM formula.
4. Call ws.insert_rows() to shift that row (and everything below) downward, then
   write the new current-period data rows into the newly vacated space.
5. Update the SUM formula to include the new rows.
6. Rebuild the GL / TB tie-out from live TB data (replaces the JLL VLOOKUP refs
   that break after the carry-forward rename).

Tab coverage
------------
  115200 Escrow RET          — GL 115200 + Berkadia payment_re_taxes
  115300 Escrow Insurance    — GL 115300 + Berkadia insurance escrow
  115600 Restricted Cash     — GL 115600 current-period transactions
  RE Tax Analysis            — GL 115200 + Berkadia escrow deposit
  Insurance Analysis         — Prepaid ledger (column-based, monthly amortization)
  Loan Analysis              — GL 213200 / 801110 + Berkadia per-loan interest data
"""

import re
from copy import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from openpyxl.utils import column_index_from_string, get_column_letter


# ─────────────────────────────────────────────────────────────────────────────
# Month / period helpers
# ─────────────────────────────────────────────────────────────────────────────

_MONTH_ABBR = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
}
_MONTH_NUM = {v: k for k, v in _MONTH_ABBR.items()}


def _period_to_dt(period: str) -> Optional[datetime]:
    """'Apr-2026' or 'Apr 2026' → datetime(2026, 4, 1)"""
    if not period:
        return None
    for sep in ('-', ' '):
        parts = period.strip().split(sep, 1)
        if len(parts) == 2:
            mon = _MONTH_NUM.get(parts[0][:3].title())
            if mon:
                try:
                    return datetime(int(parts[1].strip()), mon, 1)
                except ValueError:
                    pass
    return None


def _fmt_mmy(period: str) -> str:
    """'Apr-2026' → '04/26'"""
    d = _period_to_dt(period)
    return d.strftime('%m/%y') if d else ''


def _fmt_long(period: str) -> str:
    """'Apr-2026' → 'Apr 2026'"""
    d = _period_to_dt(period)
    return d.strftime('%b %Y') if d else period or ''


def _prior_long(period: str) -> str:
    """'Apr-2026' → 'Mar 2026'"""
    d = _period_to_dt(period)
    if not d:
        return ''
    prev_month = d.month - 1 if d.month > 1 else 12
    prev_year  = d.year if d.month > 1 else d.year - 1
    return f"{_MONTH_ABBR[prev_month]} {prev_year}"


def _quarter_label(period: str) -> str:
    """'Apr-2026' → 'Q2-2026'"""
    d = _period_to_dt(period)
    if not d:
        return ''
    q = (d.month - 1) // 3 + 1
    return f'Q{q}-{d.year}'


def _safe_float(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Prior-tab discovery
# ─────────────────────────────────────────────────────────────────────────────

def _find_prior_tab(wb, base_names: List[str], current_prefix: str):
    """
    Find the prior-period copy of an analysis tab.

    Searches for any sheet whose lowercase name CONTAINS one of the base_names
    (stripped, lowercase) and does NOT start with current_prefix.

    Returns (worksheet, sheet_name) or (None, None).
    """
    bases = [b.lower().strip() for b in base_names]
    for sname in wb.sheetnames:
        if sname.startswith(current_prefix):
            continue
        sl = sname.lower()
        for b in bases:
            if b in sl:
                return wb[sname], sname
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# Content copying  (values only — avoids broken cross-sheet formula refs)
# ─────────────────────────────────────────────────────────────────────────────

def _copy_tab_values(source_ws, target_ws):
    """
    Copy all cell values, row heights, column widths, and merged ranges
    from source_ws to target_ws.

    Formulas are copied as formula strings (not evaluated values).  After the
    carry-forward rename, cross-sheet refs like ='General '!A5 will show as
    #REF in Excel for historical data — acceptable since those values were
    already finalised.  The current-period tie-out is rebuilt fresh by
    _rebuild_tieout(), which overwrites the stale VLOOKUP cells.
    """
    # Column widths
    for col_letter, cdim in source_ws.column_dimensions.items():
        target_ws.column_dimensions[col_letter].width = cdim.width

    # Row heights
    for row_num, rdim in source_ws.row_dimensions.items():
        target_ws.row_dimensions[row_num].height = rdim.height

    # Cell values and styles
    for row in source_ws.iter_rows():
        for cell in row:
            tc = target_ws.cell(row=cell.row, column=cell.column)
            tc.value = cell.value
            if cell.has_style:
                try:
                    tc.font       = copy(cell.font)
                    tc.fill       = copy(cell.fill)
                    tc.border     = copy(cell.border)
                    tc.alignment  = copy(cell.alignment)
                    tc.number_format = cell.number_format
                except Exception:
                    pass

    # Merged cells
    for merge_range in list(source_ws.merged_cells.ranges):
        try:
            target_ws.merge_cells(str(merge_range))
        except Exception:
            pass

    # Tab colour
    if source_ws.sheet_properties.tabColor:
        target_ws.sheet_properties.tabColor = source_ws.sheet_properties.tabColor


# ─────────────────────────────────────────────────────────────────────────────
# Insertion-point detection
# ─────────────────────────────────────────────────────────────────────────────

def _find_insertion_point(ws, amount_col: int) -> Dict[str, Any]:
    """
    Scan the worksheet to find the structural boundaries.

    Strategy:
      1. Look for a cell in amount_col containing '=SUM(' — that is the
         "Ending Balance" / total row.
      2. As a fallback, look for a cell in any column containing the literal
         string pattern "='General " (the JLL "Ending Balance per GL" label row).

    Handles both simple SUM formulas (=SUM(F8:F37)) and compound ones
    (=SUM(F60:F75)+SUM(F96:F112)).  For compound formulas the insertion
    point is the total row itself; new rows are inserted before it and a
    new SUM component is appended rather than rewriting the entire formula.

    Returns dict with keys:
        insert_before_row   — row to insert new data BEFORE
        sum_start_row       — first row of the LAST SUM range (used for
                              the new appended component)
        sum_end_row         — last row of the LAST SUM range
        total_row           — row holding the SUM formula
        gl_row              — row with 'GL' label (may be None)
        variance_row        — row with 'Variance' label (may be None)
        compound_sum        — True if the formula is a multi-part SUM
        original_formula    — the original formula string (for compound rebuild)
    """
    result: Dict[str, Any] = {
        'insert_before_row': None,
        'sum_start_row':     None,
        'sum_end_row':       None,
        'total_row':         None,
        'gl_row':            None,
        'variance_row':      None,
        'compound_sum':      False,
        'original_formula':  None,
    }

    max_row = ws.max_row or 1

    for r in range(max_row, 0, -1):
        # ── Check adjacent cells for GL / Variance labels ──────────────
        for c in range(1, min(amount_col + 3, 20)):
            adj = ws.cell(row=r, column=c).value
            if isinstance(adj, str):
                al = adj.strip().lower()
                if al == 'variance' and result['variance_row'] is None:
                    result['variance_row'] = r
                elif al in ('gl', 'g/l', 'gl balance') and result['gl_row'] is None:
                    result['gl_row'] = r

        # ── Check amount column for SUM formula ───────────────────────
        cell_val = ws.cell(row=r, column=amount_col).value
        if isinstance(cell_val, str) and 'SUM(' in cell_val.upper():
            result['total_row'] = r
            result['insert_before_row'] = r
            result['original_formula'] = cell_val

            # Find ALL SUM ranges in the formula (handles compound formulas)
            all_ranges = re.findall(
                r'SUM\(\s*[A-Z]+(\d+)\s*:\s*[A-Z]+(\d+)\s*\)',
                cell_val, re.I,
            )
            if all_ranges:
                # Store the LAST range's start/end — new rows go just before
                # the total row, so the new component appends after the last range
                result['sum_start_row'] = int(all_ranges[-1][0])
                result['sum_end_row']   = int(all_ranges[-1][1])
                result['compound_sum']  = len(all_ranges) > 1
            break

        # ── Fallback: JLL "='General '!A10" ending-balance label ───────
        if result['insert_before_row'] is None:
            for c in range(1, 20):
                adj = ws.cell(row=r, column=c).value
                if isinstance(adj, str) and adj.strip().startswith("='General"):
                    result['insert_before_row'] = r
                    break

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Row-insertion + formula update
# ─────────────────────────────────────────────────────────────────────────────

def _insert_rows_and_write(
    ws,
    ip: Dict[str, Any],
    new_rows: List[Dict[str, Any]],
    amount_col: int,
    period: str,
    tb_map: Optional[dict],
    account_code: str,
):
    """
    1. Insert len(new_rows) blank rows at ip['insert_before_row'].
    2. Write new_rows data.
    3. Update the SUM formula at the (now-shifted) total row.
    4. Rebuild the GL / TB tie-out section.

    Each item in new_rows is a dict mapping column letters to values,
    e.g. {'B': 'Description', 'F': 1234.56}.
    """
    n = len(new_rows)
    if n == 0:
        return

    insert_at = ip.get('insert_before_row') or (ws.max_row + 2)

    # ── 1. Insert blank rows ──────────────────────────────────────────
    ws.insert_rows(insert_at, amount=n)

    # ── 2. Write data into the new blank rows ─────────────────────────
    for i, row_data in enumerate(new_rows):
        r = insert_at + i
        for col_letter, val in row_data.items():
            if not col_letter:
                continue
            try:
                col_idx = column_index_from_string(col_letter.strip().upper())
            except Exception:
                continue
            cell = ws.cell(row=r, column=col_idx)
            cell.value = val
            if isinstance(val, (int, float)):
                cell.number_format = '#,##0.00;(#,##0.00);"-"'

    # ── 3. Update SUM formula ─────────────────────────────────────────
    if ip.get('total_row'):
        new_total_row = ip['total_row'] + n
        col_l         = get_column_letter(amount_col)

        if ip.get('compound_sum') and ip.get('original_formula'):
            # Compound formula (e.g. =SUM(F60:F75)+SUM(F96:F112)).
            # Preserve the original ranges exactly — they didn't shift because
            # insertion happened AFTER them (before the total row).
            # Append a new component for the rows we just inserted.
            new_component_start = insert_at          # first new row
            new_component_end   = insert_at + n - 1  # last new row
            new_formula = (
                ip['original_formula'].rstrip()
                + f'+SUM({col_l}{new_component_start}:{col_l}{new_component_end})'
            )
            ws.cell(row=new_total_row, column=amount_col).value = new_formula

        elif ip.get('sum_start_row'):
            # Simple single-range SUM — extend the end row.
            new_sum_end = (ip['sum_end_row'] or ip['total_row'] - 1) + n
            ws.cell(row=new_total_row, column=amount_col).value = (
                f'=SUM({col_l}{ip["sum_start_row"]}:{col_l}{new_sum_end})'
            )

    # ── 4. Rebuild tie-out ────────────────────────────────────────────
    # Locate where the GL / Variance rows ended up after insertion
    gl_row  = (ip.get('gl_row')       or 0) + n if ip.get('gl_row')       else None
    var_row = (ip.get('variance_row') or 0) + n if ip.get('variance_row') else None
    total_row_final = (ip.get('total_row') or 0) + n if ip.get('total_row') else None

    _rebuild_tieout(ws, total_row_final, gl_row, var_row, amount_col, tb_map, account_code)


def _rebuild_tieout(
    ws,
    total_row: Optional[int],
    gl_row: Optional[int],
    var_row: Optional[int],
    amount_col: int,
    tb_map: Optional[dict],
    account_code: str,
):
    """
    Overwrite the GL and Variance cells with fresh values from tb_map.
    This replaces the stale JLL VLOOKUP references that break after the
    carry-forward rename.

    Also updates the account-code cell (used by the VLOOKUP key) to a
    plain integer so it reads cleanly even without the General tab.
    """
    tb_acct   = (tb_map or {}).get(account_code)
    tb_ending = tb_acct.ending_balance if tb_acct else None
    col_l     = get_column_letter(amount_col)

    # Update GL cell
    if gl_row:
        gl_cell = ws.cell(row=gl_row, column=amount_col)
        if tb_ending is not None:
            gl_cell.value = tb_ending
            gl_cell.number_format = '#,##0.00;(#,##0.00);"-"'
        else:
            gl_cell.value = 'Not in TB'

    # Update Variance cell
    if var_row and total_row and gl_row:
        ws.cell(row=var_row, column=amount_col).value = (
            f'={col_l}{gl_row}-{col_l}{total_row}'
        )
        ws.cell(row=var_row, column=amount_col).number_format = '#,##0.00;(#,##0.00);"-"'

    # Fix account-code key cell (row just above GL label row)
    if gl_row and gl_row >= 3:
        # The account-code cell is typically 2 rows above GL in JLL layout
        key_row = gl_row - 2
        for c in range(1, amount_col + 2):
            v = ws.cell(row=key_row, column=c).value
            # If cell holds a formula referencing General tab, overwrite it
            if isinstance(v, str) and ("='General" in v or account_code in str(v)):
                ws.cell(row=key_row, column=amount_col).value = (
                    int(account_code) if account_code.isdigit() else account_code
                )
                break
            elif str(v) == account_code:
                break  # already a plain value


# ─────────────────────────────────────────────────────────────────────────────
# GL helper
# ─────────────────────────────────────────────────────────────────────────────

def _get_txns(gl_result, account_code: str) -> list:
    if not gl_result:
        return []
    for acct in (gl_result.accounts or []):
        if acct.account_code == account_code:
            return acct.transactions or []
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Stub writer (no prior workpaper)
# ─────────────────────────────────────────────────────────────────────────────

def _write_stub(ws, display_name: str, period: str, new_rows: list,
                amount_col: int, tb_map: Optional[dict], account_code: str):
    """
    Minimal fallback when no prior workpaper was uploaded.
    Writes headers, current-period data rows, a SUM total, and a tie-out.
    """
    col_l = get_column_letter(amount_col)

    ws.cell(row=1, column=2).value = display_name
    ws.cell(row=2, column=2).value = f'Period: {period}'
    ws.cell(row=3, column=2).value = (
        'NOTE: Upload prior workpaper to carry forward historical data.'
    )
    ws.cell(row=5, column=2).value = 'Description'
    ws.cell(row=5, column=4).value = 'Date'
    ws.cell(row=5, column=amount_col).value = 'Amount'

    data_start = 7
    for i, row_data in enumerate(new_rows):
        r = data_start + i
        for col_letter, val in row_data.items():
            if not col_letter:
                continue
            try:
                c = column_index_from_string(col_letter.strip().upper())
            except Exception:
                continue
            ws.cell(row=r, column=c).value = val
            if isinstance(val, (int, float)):
                ws.cell(row=r, column=c).number_format = '#,##0.00;(#,##0.00);"-"'

    total_row = data_start + len(new_rows) + 1
    ws.cell(row=total_row, column=2).value = 'Ending Balance'
    ws.cell(row=total_row, column=amount_col).value = (
        f'=SUM({col_l}{data_start}:{col_l}{total_row - 1})'
    )
    ws.cell(row=total_row, column=amount_col).number_format = '#,##0.00;(#,##0.00);"-"'

    # Account code key
    ws.cell(row=total_row + 2, column=amount_col).value = (
        int(account_code) if account_code.isdigit() else account_code
    )

    tb_acct   = (tb_map or {}).get(account_code)
    tb_ending = tb_acct.ending_balance if tb_acct else None

    ws.cell(row=total_row + 4, column=amount_col - 1).value = 'GL'
    ws.cell(row=total_row + 4, column=amount_col).value = tb_ending
    if tb_ending is not None:
        ws.cell(row=total_row + 4, column=amount_col).number_format = '#,##0.00;(#,##0.00);"-"'

    ws.cell(row=total_row + 5, column=amount_col - 1).value = 'Variance'
    ws.cell(row=total_row + 5, column=amount_col).value = (
        f'={col_l}{total_row + 4}-{col_l}{total_row}'
    )


# ─────────────────────────────────────────────────────────────────────────────
# ── 115200  Escrow RET
# ─────────────────────────────────────────────────────────────────────────────

def build_ret_escrow_tab(
    wb, berkadia_loans, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    115200 Escrow RET: copy prior tab + append current-period escrow deposit row.

    Data sources (in priority order):
      1. GL account 115200 debit transactions (actual Yardi entries)
      2. Berkadia payment_re_taxes (sum across all loans)

    Column layout (RCW):  B = Description | D = Date | F = Per Stmt amount
    Amount column: F (col 6)
    """
    prior_ws, _ = _find_prior_tab(
        wb,
        ['115200 Escrow RET', '115200', 'Escrow RET', 'Escrow Real Estate Tax'],
        current_prefix,
    )
    tab_name = (tab_prefix + '115200 Escrow RET')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)
    if prior_ws:
        _copy_tab_values(prior_ws, ws)

    # Build new rows from GL first
    txns = _get_txns(gl_result, '115200')
    new_rows = []
    for txn in txns:
        date_str = txn.date.strftime('%m/%d/%Y') if getattr(txn, 'date', None) else ''
        net = _safe_float(getattr(txn, 'debit', 0)) - _safe_float(getattr(txn, 'credit', 0))
        new_rows.append({
            'B': (txn.description or '')[:60],
            'D': date_str,
            'F': net,
        })

    # Fallback: Berkadia escrow deposit sum
    if not new_rows and berkadia_loans:
        total = sum(_safe_float(l.get('payment_re_taxes', 0)) for l in berkadia_loans)
        if total:
            new_rows.append({
                'B': f'RET ESCROW Payment {_fmt_long(period)}',
                'D': '',
                'F': total,
            })

    if prior_ws and new_rows:
        ip = _find_insertion_point(ws, 6)
        _insert_rows_and_write(ws, ip, new_rows, 6, period, tb_map, '115200')
    elif not prior_ws:
        _write_stub(ws, '115200 Escrow RET', period, new_rows, 6, tb_map, '115200')


# ─────────────────────────────────────────────────────────────────────────────
# ── 115300  Escrow Insurance
# ─────────────────────────────────────────────────────────────────────────────

def build_insurance_escrow_tab(
    wb, berkadia_loans, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    115300 Escrow Insurance: copy prior tab + append current-period entry.

    Data sources:
      1. GL account 115300 transactions
      2. Berkadia insurance_escrow_balance change (ending - prior ending)
         If unavailable, use payment_reserves as proxy.

    Amount column: F (col 6)
    """
    prior_ws, _ = _find_prior_tab(
        wb,
        ['115300 Escrow Insurance', '115300', 'Escrow Insurance'],
        current_prefix,
    )
    tab_name = (tab_prefix + '115300 Escrow Insurance')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)
    if prior_ws:
        _copy_tab_values(prior_ws, ws)

    txns = _get_txns(gl_result, '115300')
    new_rows = []
    for txn in txns:
        date_str = txn.date.strftime('%m/%d/%Y') if getattr(txn, 'date', None) else ''
        net = _safe_float(getattr(txn, 'debit', 0)) - _safe_float(getattr(txn, 'credit', 0))
        new_rows.append({
            'B': (txn.description or f'Property Insurance per {_fmt_long(period)} stmt due')[:60],
            'D': date_str,
            'F': net,
        })

    if not new_rows and berkadia_loans:
        total = sum(
            _safe_float(l.get('payment_insurance', 0)
                        or l.get('payment_reserves', 0))
            for l in berkadia_loans
        )
        if total:
            new_rows.append({
                'B': f'Property Insurance per {_fmt_long(period)} stmt due',
                'D': '',
                'F': total,
            })

    if prior_ws and new_rows:
        ip = _find_insertion_point(ws, 6)
        _insert_rows_and_write(ws, ip, new_rows, 6, period, tb_map, '115300')
    elif not prior_ws:
        _write_stub(ws, '115300 Escrow Insurance', period, new_rows, 6, tb_map, '115300')


# ─────────────────────────────────────────────────────────────────────────────
# ── 115600  Restricted Cash – Other
# ─────────────────────────────────────────────────────────────────────────────

def build_restricted_cash_tab(
    wb, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    115600 Restricted Cash: copy prior tab + append current-period GL entries.
    Typical activity: monthly interest income deposits.

    Amount column: F (col 6)
    """
    prior_ws, _ = _find_prior_tab(
        wb,
        ['115600', 'Restricted Cash - Other', 'Restricted Cash'],
        current_prefix,
    )
    tab_name = (tab_prefix + '115600 Restricted Cash')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)
    if prior_ws:
        _copy_tab_values(prior_ws, ws)

    txns = _get_txns(gl_result, '115600')
    new_rows = []
    for txn in txns:
        date_str = txn.date.strftime('%m/%d/%Y') if getattr(txn, 'date', None) else ''
        net = _safe_float(getattr(txn, 'debit', 0)) - _safe_float(getattr(txn, 'credit', 0))
        new_rows.append({
            'B': f'Rcd: {_fmt_long(period)} {txn.description or "Interest Income"}'[:60],
            'D': date_str,
            'F': net,
        })

    if prior_ws and new_rows:
        ip = _find_insertion_point(ws, 6)
        _insert_rows_and_write(ws, ip, new_rows, 6, period, tb_map, '115600')
    elif not prior_ws:
        _write_stub(ws, '115600 Restricted Cash - Other', period, new_rows, 6, tb_map, '115600')


# ─────────────────────────────────────────────────────────────────────────────
# ── RE Tax Analysis
# ─────────────────────────────────────────────────────────────────────────────

def build_ret_analysis_tab(
    wb, berkadia_loans, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    RE Tax Analysis: copy prior tab + append the monthly escrow deposit row.

    RCW column layout:
      B = Description | D = Date
      F = A/C 135120  Prepaid RE Tax (escrow asset)
      H = A/C 641110  RE Tax Expense (amortization — usually blank on deposit months)

    Data source: GL 115200 debit transactions (escrow deposit to Berkadia).
    Fallback: Berkadia payment_re_taxes.

    Amount column for tie-out: F (col 6)
    """
    prior_ws, _ = _find_prior_tab(
        wb,
        ['RE Tax Analysis', 'Real Estate Tax Analysis', 'RE Tax'],
        current_prefix,
    )
    tab_name = (tab_prefix + 'RE Tax Analysis')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)
    if prior_ws:
        _copy_tab_values(prior_ws, ws)

    txns_115200 = _get_txns(gl_result, '115200')
    txns_641110 = _get_txns(gl_result, '641110')

    new_rows = []

    # Each GL debit to 115200 is an escrow deposit
    for txn in txns_115200:
        debit = _safe_float(getattr(txn, 'debit', 0))
        if debit <= 0:
            continue
        date_str = txn.date.strftime('%m/%d/%Y') if getattr(txn, 'date', None) else ''
        desc     = txn.description or f'RET ESCROW Payment {_fmt_mmy(period)}'
        new_rows.append({'B': desc[:60], 'D': date_str, 'F': debit})

    # GL 641110 credits → payments/disbursements from escrow
    for txn in txns_641110:
        credit = _safe_float(getattr(txn, 'credit', 0))
        if credit <= 0:
            continue
        date_str = txn.date.strftime('%m/%d/%Y') if getattr(txn, 'date', None) else ''
        desc     = txn.description or f'Reclass {_fmt_long(period)} Tax Exp fr PPD'
        new_rows.append({'B': desc[:60], 'D': date_str, 'F': -credit, 'H': credit})

    # Berkadia fallback
    if not new_rows and berkadia_loans:
        total = sum(_safe_float(l.get('payment_re_taxes', 0)) for l in berkadia_loans)
        if total:
            new_rows.append({
                'B': f'RET ESCROW Payment {_fmt_mmy(period)}- {_quarter_label(period)}',
                'D': '',
                'F': total,
            })

    if prior_ws and new_rows:
        ip = _find_insertion_point(ws, 6)
        _insert_rows_and_write(ws, ip, new_rows, 6, period, tb_map, '115200')
    elif not prior_ws:
        _write_stub(ws, 'RE Tax Analysis', period, new_rows, 6, tb_map, '115200')


# ─────────────────────────────────────────────────────────────────────────────
# ── Insurance Analysis  (column-based — one column per month)
# ─────────────────────────────────────────────────────────────────────────────

def build_insurance_analysis_tab(
    wb, prepaid_active, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    Insurance Analysis: copy prior tab, then find which column corresponds to
    the current period and fill in the monthly amortization amounts.

    Structure (RCW):
      Row 7: date headers starting from a base date, each subsequent column
             is the prior column + 31 days (=I7+31, =J7+31, …).
      Rows 8+: one row per policy, amounts in the matching date column.
      Cols U–X: TOTAL, TOTAL, PREPAID INSURANCE, ACCRUED INSURANCE  (summary cols)

    Strategy:
      1. Copy prior tab.
      2. Scan row 7 for a datetime whose month/year matches the current period.
         If found, that is the target column.
         If not found, find the last datetime column and use the next column,
         writing the date formula continuation (=prev_col+31).
      3. For each data row (8+), match to a prepaid_active item by description
         and write the monthly_amount into the target column.
      4. Update the TOTAL/SUM columns (U onward) if they don't already include
         the new column in their ranges.

    No tie-out rebuild needed — the Insurance Analysis does its own GL lookups
    in the summary columns (U–X), which are rebuilt by the VLOOKUP formulas;
    we update those cells with direct TB values instead.
    """
    prior_ws, _ = _find_prior_tab(
        wb, ['Insurance Analysis'], current_prefix,
    )
    tab_name = (tab_prefix + 'Insurance Analysis')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)

    if not prior_ws:
        ws.cell(row=1, column=2).value = 'Insurance Analysis'
        ws.cell(row=2, column=2).value = f'Period: {period}'
        ws.cell(row=4, column=2).value = (
            'NOTE: Upload prior workpaper to carry forward insurance analysis.'
        )
        return

    _copy_tab_values(prior_ws, ws)

    if not prepaid_active:
        return

    period_dt = _period_to_dt(period)
    if not period_dt:
        return

    # ── Find the target column ────────────────────────────────────────
    target_col = None
    last_date_col = None

    # Row 7 has the date headers — search up to col 40
    for col in range(1, 41):
        val = ws.cell(row=7, column=col).value
        if isinstance(val, datetime):
            if val.year == period_dt.year and val.month == period_dt.month:
                target_col = col
                break
            last_date_col = col

    if target_col is None and last_date_col is not None:
        # Append the current period as the next column
        target_col = last_date_col + 1
        prev_letter = get_column_letter(last_date_col)
        ws.cell(row=7, column=target_col).value = f'={prev_letter}7+31'

    if target_col is None:
        return

    target_letter = get_column_letter(target_col)

    # ── Insurance prepaid items ───────────────────────────────────────
    ins_gl_accounts = {'639110', '639120', '135110', '213300'}
    ins_items = [
        i for i in prepaid_active
        if str(i.get('gl_account_number', '')).strip() in ins_gl_accounts
    ]

    # Build a lookup: description keywords → monthly_amount
    item_lookup = {}
    for item in ins_items:
        desc = (
            item.get('description') or item.get('vendor') or ''
        ).lower().strip()
        monthly = float(item.get('monthly_amount') or 0)
        if desc and monthly:
            item_lookup[desc] = monthly

    # ── Fill monthly amounts into matching data rows ──────────────────
    skip_keywords = ('date', 'paid', 'premium', 'per month', 'prepaid', 'term',
                     'accrual', 'total', 'expense', 'insurance', 'account')

    for row_num in range(8, min(ws.max_row, 30)):
        # Find the description for this row (cols B–E area)
        row_desc = ''
        for c in range(2, 8):
            v = ws.cell(row=row_num, column=c).value
            if isinstance(v, str) and v.strip():
                row_desc = v.strip().lower()
                if not any(k in row_desc for k in skip_keywords):
                    break
                row_desc = ''

        if not row_desc:
            continue
        if any(k in row_desc for k in skip_keywords):
            continue

        # Match to a prepaid item
        monthly = 0.0
        for item_desc, amt in item_lookup.items():
            # Check if at least one meaningful word from the item description
            # appears in the row description (ignore short words)
            words = [w for w in item_desc.split() if len(w) > 3]
            if words and any(w in row_desc for w in words[:3]):
                monthly = amt
                break

        if monthly:
            ws.cell(row=row_num, column=target_col).value = round(monthly, 2)
            ws.cell(row=row_num, column=target_col).number_format = '#,##0.00'

    # ── Update summary TOTAL column SUM ranges if needed ─────────────
    # Summary cols U (21), V (22), W (23), X (24) hold =SUM(I:T row) style
    for sum_col in range(21, 26):
        for row_num in range(8, 25):
            cell = ws.cell(row=row_num, column=sum_col)
            val  = cell.value
            if not isinstance(val, str) or not val.upper().startswith('=SUM('):
                continue
            # Check if target_letter is already in the range
            m = re.search(
                r'=SUM\(\s*([A-Z]+)(\d+)\s*:\s*([A-Z]+)(\d+)\s*\)',
                val, re.I,
            )
            if not m:
                continue
            start_letter = m.group(1).upper()
            end_letter   = m.group(3).upper()
            row_ref      = m.group(2)
            start_idx    = column_index_from_string(start_letter)
            end_idx      = column_index_from_string(end_letter)
            if target_col < start_idx or target_col > end_idx + 1:
                continue  # not adjacent, skip
            if target_col > end_idx:
                # Extend the range
                new_end = get_column_letter(target_col)
                cell.value = f'=SUM({start_letter}{row_ref}:{new_end}{row_ref})'

    # ── Rebuild GL tie-out cells (cols U–X, rows ~22–25) ─────────────
    for account_code, sum_col in [('639110', 21), ('639120', 22),
                                   ('135110', 23), ('213300', 24)]:
        tb_acct = (tb_map or {}).get(account_code)
        if tb_acct is None:
            continue
        # Find GL row for this column (scan for 'GL' label nearby)
        for gl_row in range(20, min(ws.max_row, 32)):
            lbl_cell = ws.cell(row=gl_row, column=sum_col - 1).value
            if isinstance(lbl_cell, str) and lbl_cell.strip().lower() in ('gl', 'g/l'):
                ws.cell(row=gl_row, column=sum_col).value = tb_acct.ending_balance
                ws.cell(row=gl_row, column=sum_col).number_format = '#,##0.00;(#,##0.00);"-"'
                # Variance: next row
                vs_cell = ws.cell(row=gl_row + 1, column=sum_col)
                col_l   = get_column_letter(sum_col)
                # Find the total row for this column
                for tot_r in range(gl_row - 1, gl_row - 5, -1):
                    tv = ws.cell(row=tot_r, column=sum_col).value
                    if isinstance(tv, (int, float)) or (isinstance(tv, str) and tv.startswith('=SUM')):
                        vs_cell.value = f'={col_l}{gl_row}-{col_l}{tot_r}'
                        vs_cell.number_format = '#,##0.00;(#,##0.00);"-"'
                        break
                break


# ─────────────────────────────────────────────────────────────────────────────
# ── Loan Analysis
# ─────────────────────────────────────────────────────────────────────────────

def build_loan_analysis_tab(
    wb, berkadia_loans, gl_result, period,
    current_prefix, tab_prefix, tb_map=None,
):
    """
    Loan Analysis: copy prior tab + append current-period interest cycle rows.

    Per loan, per period, the standard three-row pattern is:
      1. {prior_month} Accrual Reversal   → 213200 debit (+), 801110 credit (-)
      2. {loan_id} {prior_month} Interest Payment → 801110 debit (+)
      3. {loan_id} Accr {curr_month} Interest Due → 213200 credit (-), 801110 debit (+)

    Data sources (preference order):
      1. GL account 213200 transactions  — matched to loan by description suffix
         (e.g., description contains "1159010" or "159010")
      2. GL account 801110 transactions  — interest expense counterpart
      3. Berkadia payment_interest       — confirms the payment amount per loan

    RCW column layout:
      A  = Loan number
      B  = Description
      D  = Period string (MM/YY)
      F  = 231100 Revlab     (mortgage payable — blank for interest entries)
      G  = 231100 Revlabpm   (mortgage payable — blank for interest entries)
      I  = 213200 Accrued Interest Payable
      K  = 801110 Interest Expense

    Amount column for SUM / tie-out: I (col 9).
    """
    prior_ws, _ = _find_prior_tab(wb, ['Loan Analysis'], current_prefix)
    tab_name = (tab_prefix + 'Loan Analysis')[:31]
    if tab_name in wb.sheetnames:
        return

    ws = wb.create_sheet(tab_name)
    if prior_ws:
        _copy_tab_values(prior_ws, ws)

    txns_213200 = _get_txns(gl_result, '213200')
    txns_801110 = _get_txns(gl_result, '801110')
    period_str  = _fmt_mmy(period)
    prior_str   = _prior_long(period)

    # ── Identify loan IDs ─────────────────────────────────────────────
    loan_ids = _extract_loan_ids(txns_213200, berkadia_loans or [])

    new_rows = []

    if loan_ids:
        for lid in loan_ids:
            _add_loan_rows(
                new_rows, lid, txns_213200, txns_801110,
                berkadia_loans or [], period_str, prior_str,
            )
    else:
        # Fallback: dump all 213200/801110 transactions without loan grouping
        for txn in txns_213200:
            net = _safe_float(getattr(txn, 'debit', 0)) - _safe_float(getattr(txn, 'credit', 0))
            new_rows.append({
                'B': (txn.description or '')[:50],
                'D': period_str,
                'I': net,
            })
        for txn in txns_801110:
            net = _safe_float(getattr(txn, 'debit', 0)) - _safe_float(getattr(txn, 'credit', 0))
            new_rows.append({
                'B': (txn.description or '')[:50],
                'D': period_str,
                'K': net,
            })

    if prior_ws and new_rows:
        ip = _find_insertion_point(ws, 9)   # col I = 9
        _insert_rows_and_write(ws, ip, new_rows, 9, period, tb_map, '213200')
    elif not prior_ws:
        _write_stub(ws, 'Loan Analysis', period, new_rows, 9, tb_map, '213200')


def _extract_loan_ids(txns_213200: list, berkadia_loans: list) -> List[str]:
    """
    Derive the set of loan IDs to process, in order.

    Priority:
      1. Berkadia loan_number fields  (e.g., "1159010")
      2. IDs parsed from GL 213200 transaction descriptions
    """
    seen: set = set()
    ids: List[str] = []

    # From Berkadia
    for loan in berkadia_loans:
        lid = str(loan.get('loan_number') or '').strip()
        if lid and lid not in seen:
            seen.add(lid)
            ids.append(lid)

    # From GL descriptions (pattern: 6-7 digit number starting with 1159)
    if not ids:
        for txn in txns_213200:
            desc = txn.description or ''
            for m in re.finditer(r'\b1?1590\d\d\b', desc):
                raw = m.group()
                # Normalise to 7 digits
                lid = raw if len(raw) == 7 else ('1' + raw if len(raw) == 6 else raw)
                if lid not in seen:
                    seen.add(lid)
                    ids.append(lid)

    return ids


def _add_loan_rows(
    new_rows: list,
    loan_id: str,
    txns_213200: list,
    txns_801110: list,
    berkadia_loans: list,
    period_str: str,
    prior_str: str,
):
    """
    Append the three standard rows for one loan to new_rows.
    """
    suffix = loan_id[-6:]   # e.g., "159010" from "1159010"

    def _matches(txn):
        d = (txn.description or '').lower()
        return suffix in d or loan_id.lower() in d

    loan_213 = [t for t in txns_213200 if _matches(t)]
    loan_811 = [t for t in txns_801110 if _matches(t)]

    berk = next(
        (l for l in berkadia_loans
         if str(l.get('loan_number', '')).endswith(suffix)),
        None,
    )

    # ── Row 1: Prior-month accrual reversal ──────────────────────────
    reversal = next(
        (t for t in loan_213
         if 'reversal' in (t.description or '').lower()
         or 'reversal' in (t.description or '').lower()),
        None,
    )
    if reversal:
        rev_213 = _safe_float(getattr(reversal, 'debit', 0)) - _safe_float(getattr(reversal, 'credit', 0))
        rev_811 = -rev_213
    else:
        rev_213 = rev_811 = 0.0

    new_rows.append({
        'B': f'{prior_str} Accrual Reversal',
        'D': period_str,
        'I': rev_213 if rev_213 != 0 else None,
        'K': rev_811 if rev_811 != 0 else None,
    })

    # ── Row 2: Interest payment ───────────────────────────────────────
    payment_txn = next(
        (t for t in loan_213
         if any(k in (t.description or '').lower() for k in ('payment', 'pytm'))
         and 'reversal' not in (t.description or '').lower()),
        None,
    )
    if payment_txn:
        pay_amt = (
            _safe_float(getattr(payment_txn, 'debit', 0))
            - _safe_float(getattr(payment_txn, 'credit', 0))
        )
    else:
        pay_amt = _safe_float((berk or {}).get('payment_interest', 0))

    new_rows.append({
        'A': loan_id,
        'B': f'{prior_str} Interest Payment',
        'D': period_str,
        'K': pay_amt if pay_amt != 0 else None,
    })

    # ── Row 3: New accrual ────────────────────────────────────────────
    accrual_txn = next(
        (t for t in loan_213
         if any(k in (t.description or '').lower() for k in ('accr', 'accrual'))
         and 'reversal' not in (t.description or '').lower()),
        None,
    )
    if accrual_txn:
        acc_213 = (
            _safe_float(getattr(accrual_txn, 'debit', 0))
            - _safe_float(getattr(accrual_txn, 'credit', 0))
        )
        acc_811 = -acc_213
    else:
        acc_213 = acc_811 = 0.0

    new_rows.append({
        'A': loan_id,
        'B': f'Accr {period_str} Interest Due -{loan_id}',
        'D': period_str,
        'I': acc_213 if acc_213 != 0 else None,
        'K': acc_811 if acc_811 != 0 else None,
    })


# ─────────────────────────────────────────────────────────────────────────────
# ── Main entry point  (called from bs_workpaper_generator)
# ─────────────────────────────────────────────────────────────────────────────

def build_all_analysis_tabs(
    wb,
    period: str,
    current_prefix: str,
    tab_prefix: str,
    gl_result=None,
    tb_map: Optional[dict] = None,
    berkadia_loans: Optional[list] = None,
    prepaid_active: Optional[list] = None,
):
    """
    Build all analysis tabs for the current period.

    Called from bs_workpaper_generator.generate_bs_workpaper() after the
    standard BS account tabs have been written.

    Order matters: simpler / self-contained tabs first, complex last.
    Each builder checks whether its tab already exists and returns early
    if so (idempotent).
    """
    build_ret_escrow_tab(
        wb, berkadia_loans, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
    build_insurance_escrow_tab(
        wb, berkadia_loans, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
    build_restricted_cash_tab(
        wb, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
    build_ret_analysis_tab(
        wb, berkadia_loans, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
    build_insurance_analysis_tab(
        wb, prepaid_active, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
    build_loan_analysis_tab(
        wb, berkadia_loans, gl_result, period,
        current_prefix, tab_prefix, tb_map,
    )
