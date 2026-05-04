"""
Workpaper Generator — Revolution Labs Monthly Close
====================================================
Generates the monthly close workpaper with:
  - Summary tab:      all BS accounts, GL ending vs TB ending, variance status
  - Trial Balance tab: direct from Yardi TB export
  - One tab per balance sheet account: transactions + GL ending + TB tie-out
  - Prepaid Schedule tab (if ledger data available)
  - Bank Rec tabs (PNC Operating + DACA)

Historical carry-forward
------------------------
If ``prior_workpaper_path`` is supplied the generator loads the prior
month's workpaper and renames every existing sheet with the
``prior_period`` label (e.g. "Feb-2026 Summary").  The current-period
sheets are then appended with the current ``period`` label (e.g.
"Mar-2026 Summary").  Over time the file accumulates a full history:

    Feb-2026 Summary
    Feb-2026 Trial Balance
    Feb-2026 111100
    …
    Mar-2026 Summary   ← current period (most recent tabs at end)
    Mar-2026 Trial Balance
    Mar-2026 111100
    …

Structure mirrors the Hartwell workpaper pattern:
  [transactions / rollforward]
  ─────────────────────────────
  Ending Balance per GL:   $X    ← computed from GL transactions
  TB Ending Balance:       $X    ← from Yardi TB export
  Variance:                $0    ← must equal zero (flags accrual gaps if not)

The Variance will be non-zero for accounts where accrual JEs are in the TB
but not yet in the GL — surfacing exactly what still needs to be posted.
"""

import os
import re
from datetime import datetime, date
from typing import List, Dict, Optional
from openpyxl import Workbook, load_workbook as _load_workbook
from property_config import is_balance_sheet_account
from openpyxl.styles import (
    Font, PatternFill, Alignment, Border, Side
)
from openpyxl.utils import get_column_letter

try:
    from analysis_tab_builder import build_all_analysis_tabs as _build_analysis_tabs
except ImportError:
    _build_analysis_tabs = None

# Regex to detect already-prefixed sheet names like "Mar-2026 Summary"
_PERIOD_PREFIX_RE = re.compile(
    r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4} '
)

# Tab names (lowercase) that are always carried forward even though they
# don't start with an account code digit.
_PRIOR_TAB_WHITELIST = {
    'xxxxxxx', 'general ', 'general',
    'mgmt fee', 'tb', 'ts',
    'rent roll rec', 'loan analysis',
    're tax analysis', 'insurance analysis',
    '135150 ppd other', 'accrued insurance',
    'bank rec - operating', 'bank rec - daca', 'bank rec - development',
    'prepaid schedule', 'summary', 'trial balance',
}

# Tab names (lowercase) that are never carried forward — JLL working/utility sheets
_PRIOR_TAB_BLOCKLIST = {
    'sheet1', 'sheet2', 'sheet3',
    'instructions', 'upload', 'input',
    'mgmt fee back up', 'rs', 'deposit register',
    'insu calc', 'accrual calc support',
    'stx', 'stx gl', 'electric bb_recon',
    'sq footage', 'sales tax rec',
}


def _should_carry_forward_tab(tab_name: str) -> bool:
    """
    Return True if a prior-workpaper tab should be renamed and kept.

    Keeps:
      • Account-code tabs — name (stripped) starts with a digit, e.g. '111100 PNC Cash'
      • Known analysis / summary tab names (whitelist)
      • Any tab whose name contains a 6-digit account code

    Drops:
      • Explicitly blocked JLL working / utility tabs
      • Any other text-named tab not in the whitelist
    """
    stripped = tab_name.strip()
    lower    = stripped.lower()

    if lower in _PRIOR_TAB_BLOCKLIST:
        return False

    # Account-code tabs (may have a leading space in JLL files)
    if stripped and stripped[0].isdigit():
        return True

    if lower in _PRIOR_TAB_WHITELIST:
        return True

    # Tab name contains a 6-digit account code anywhere (e.g. ' 2220-010')
    if re.search(r'\b\d{6}\b', stripped):
        return True

    return False


# ── Constants ────────────────────────────────────────────────

# Balance sheet account range (assets + liabilities + equity)
BS_ACCOUNT_RANGE = ('100000', '399999')

# Excel sheet names cannot contain: \ / * ? : [ ]
# Any of these characters in account names or period labels will be replaced with '-'.
_EXCEL_INVALID_CHARS = '\\/*?:[]'


def _safe_sheet_name(name: str, max_len: int = 31) -> str:
    """Return *name* with Excel-illegal characters replaced by '-', truncated to max_len."""
    for ch in _EXCEL_INVALID_CHARS:
        name = name.replace(ch, '-')
    return name[:max_len]

# Tab colors
COLOR_SUMMARY    = '1F4E78'   # dark blue  — summary
COLOR_TB         = '2E75B6'   # medium blue — trial balance
COLOR_BS_STD     = '70AD47'   # green       — standard BS tabs
COLOR_BS_COMPLEX = 'FF0000'   # red         — complex tabs (accrued exp, prepaids)

COMPLEX_ACCOUNTS = {'213100', '135110', '135150', '213200', '221100'}

# Accounts that use a JLL-style accrual schedule instead of raw GL transaction detail.
# Each accrual line shows: Expense Acct # | Description | Vendor | FROM | TO | Amount | Notes
_ACCRUAL_SCHEDULE_ACCOUNTS = {'211200', '211300', '213100', '201000'}

# Styling helpers
DARK_BLUE  = '1F4E78'
MED_BLUE   = '2E75B6'
LIGHT_BLUE = 'D6E4F0'
LIGHT_GRAY = 'F2F2F2'
GREEN_FILL = 'E2EFDA'
RED_FILL   = 'FFCCCC'
AMBER_FILL = 'FFF2CC'
WHITE      = 'FFFFFF'

# Column layout — col A is always blank; all data starts in col B
_A  = 1   # always blank — never write here
_B  = 2   # first data column (Date / Description / first label)
_C  = 3
_D  = 4
_E  = 5
_F  = 6
_G  = 7
_H  = 8
_I  = 9   # last standard data column (Balance / Total)
_NCOLS = 9  # total columns including blank col A

THIN = Border(
    left=Side(style='thin'), right=Side(style='thin'),
    top=Side(style='thin'), bottom=Side(style='thin'),
)
THICK_BOTTOM = Border(bottom=Side(style='medium'))
DOUBLE_BTM   = Border(bottom=Side(style='double'))

def _fill(hex_color):
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type='solid')

def _font(bold=False, italic=False, size=11, color='000000', name='Calibri'):
    return Font(name=name, size=size, bold=bold, italic=italic, color=color)

def _hdr_font():
    return Font(name='Calibri', size=11, bold=True, color='FFFFFF')

def _apply(cell, font=None, fill=None, fmt=None, border=None, align=None):
    if font:   cell.font   = font
    if fill:   cell.fill   = fill
    if fmt:    cell.number_format = fmt
    if border: cell.border = border
    if align:  cell.alignment = align


# ── Main entry point ─────────────────────────────────────────

def generate_bs_workpaper(gl_result, tb_result, output_path: str,
                           period: str = '', property_name: str = '',
                           prepaid_ledger_active: list = None,
                           bank_rec_data: dict = None,
                           gl_cash_balance: float = None,
                           daca_bank_data: dict = None,
                           daca_gl_balance: float = None,
                           je_adjustments: Optional[Dict[str, float]] = None,
                           prior_workpaper_path: str = None,
                           prior_period: str = None,
                           berkadia_loans: list = None,
                           dev_bank_rec_data: dict = None) -> str:
    """
    Generate the monthly close workpaper (GL vs TB tie-out + bank recs).

    Args:
        gl_result:             GLParseResult from parsers.yardi_gl.parse_gl()
        tb_result:             TBResult from parsers.yardi_trial_balance.parse()
        output_path:           Where to write the .xlsx file
        period:                Period label e.g. 'Mar-2026'
        property_name:         Property display name
        prepaid_ledger_active: Active prepaid items from prepaid_ledger.py (optional)
        bank_rec_data:         Parsed Yardi Bank Rec dict
        gl_cash_balance:       GL ending balance for account 111100 (PNC Operating)
        daca_bank_data:        Parsed KeyBank DACA statement dict
        daca_gl_balance:       GL ending balance for account 115100 (DACA)
        prior_workpaper_path:  Path to the prior month's workpaper .xlsx for
                               historical carry-forward.  All existing sheets are
                               renamed with the prior_period prefix so current-period
                               sheets can be appended without name collisions.
        prior_period:          Period label of the prior workpaper, e.g. 'Feb-2026'.
                               Used to prefix the copied sheets.
        berkadia_loans:        List of loan dicts from parsers.berkadia_loan — used to
                               populate Loan Analysis, RE Tax, and Insurance Escrow tabs.

    Returns:
        output_path
    """
    # Pass 2 safety guard — GL is already final; je_adjustments must not be used.
    if je_adjustments is not None:
        raise ValueError(
            "je_adjustments must not be passed to generate_bs_workpaper() in Pass 2. "
            "The GL is already final after the close — read actuals directly from GL."
        )

    # ── Load prior workpaper (if provided) or start fresh ─────────────────────
    # Strategy:
    #   1. Extract historical per-period summary rows from prior account tabs.
    #   2. Keep only analysis tabs (Loan, RE Tax, Insurance) — renamed with the
    #      prior period prefix so analysis_tab_builder can copy-and-extend them.
    #   3. Delete all other prior tabs (account tabs, Summary, TB, Bank Rec) —
    #      they will be regenerated fresh below.
    # Account tabs are rebuilt as rolling tables (one row per period) so the
    # full balance history lives in a single tab per account.
    _account_history: dict = {}   # {account_code: [sorted period row dicts]}

    if prior_workpaper_path and os.path.exists(prior_workpaper_path):
        try:
            _wb_prior = _load_workbook(prior_workpaper_path)

            # Auto-detect prior period label from prefixed tab names.
            if not prior_period:
                for _n in _wb_prior.sheetnames:
                    _m = _PERIOD_PREFIX_RE.match(_n)
                    if _m:
                        prior_period = _m.group(0).strip()
                        break

            # Extract historical balance data from all prior account tabs.
            _account_history = _extract_account_history(_wb_prior)

            # Determine which analysis tab names to carry forward (copy-and-extend).
            # These are the only sheets we keep in the working wb.
            _ANALYSIS_NAMES = {
                'loan analysis', 're tax analysis', 'insurance analysis',
                '135150 ppd other', 'accrued insurance',
                'bank rec - operating', 'bank rec - daca', 'bank rec - development',
            }

            # Build wb from analysis tabs only — start fresh then copy them in.
            wb = Workbook()
            _pfx = (prior_period or 'Prior') + ' '
            for _name in _wb_prior.sheetnames:
                _stripped_lower = _name.strip().lower()
                # Already-prefixed analysis tabs carry straight through.
                _already_pfx = _PERIOD_PREFIX_RE.match(_name)
                _bare_lower  = _PERIOD_PREFIX_RE.sub('', _name).strip().lower()

                is_analysis = (
                    _bare_lower in _ANALYSIS_NAMES
                    or _stripped_lower in _ANALYSIS_NAMES
                )
                if not is_analysis:
                    continue  # skip — will be regenerated

                # Copy sheet from prior wb into our working wb
                from openpyxl import copy as _xl_copy
                try:
                    import copy as _copy
                    _src = _wb_prior[_name]
                    _dst = wb.copy_worksheet(_src) if hasattr(wb, 'copy_worksheet') else None
                    if _dst is None:
                        # openpyxl < 2.5 fallback — skip analysis copy
                        continue
                    # Rename with prior period prefix if not already prefixed
                    if _already_pfx:
                        _dst.title = _name[:31]
                    else:
                        _new_name = (_pfx + _name)[:31]
                        _ctr = 1
                        while _new_name in wb.sheetnames and wb[_new_name] is not _dst:
                            _new_name = (_pfx + _name)[:28] + f'_{_ctr}'
                            _ctr += 1
                        _dst.title = _new_name
                except Exception:
                    pass  # if copy fails, analysis tab is skipped — non-fatal

        except Exception:
            wb = Workbook()
    else:
        wb = Workbook()

    # Tab prefix for all current-period sheets — sanitized for Excel.
    _tab_pfx = (_safe_sheet_name(period) + ' ') if period else ''

    # Build TB lookup: account_code -> TBAccount
    tb_map = {}
    if tb_result and hasattr(tb_result, 'accounts'):
        tb_map = {a.account_code: a for a in tb_result.accounts}

    # Identify balance sheet accounts from GL
    bs_accounts = [
        a for a in (gl_result.accounts if gl_result else [])
        if BS_ACCOUNT_RANGE[0] <= a.account_code <= BS_ACCOUNT_RANGE[1]
    ]

    # Pre-compute: journal control → (expense_code, expense_name) for accrual schedules.
    # For each accrual JE, the credit side (211200/211300/213100) and the debit side
    # (a P&L expense account) share the same journal control number.  We scan all
    # expense-range GL accounts to build this lookup so the accrual schedule tab can
    # show which expense account each accrual line offsets.
    _control_to_expense: dict = {}
    if gl_result:
        for _ea in (gl_result.accounts or []):
            _ec = _ea.account_code
            # P&L accounts are 4xxxxx (revenue) through 8xxxxx (expense)
            if _ec and '4' <= _ec[0] <= '8':
                for _et in (_ea.transactions or []):
                    _ctrl = str(getattr(_et, 'control', '') or '').strip()
                    if _ctrl and _ctrl not in _control_to_expense:
                        _control_to_expense[_ctrl] = (_ec, _ea.account_name)

    # ── Identify TB accounts with no current-period GL activity ──────────────
    # These appear in the Trial Balance (balance carried from prior period) but
    # have zero transactions in this period's GL export.  They still need a tab
    # so the workpaper shows the balance that makes up the G/L.
    _gl_bs_codes = {a.account_code for a in bs_accounts}
    _zero_activity_tb = []
    if tb_result and hasattr(tb_result, 'accounts'):
        for _tba in sorted(tb_result.accounts, key=lambda a: a.account_code):
            if (BS_ACCOUNT_RANGE[0] <= _tba.account_code <= BS_ACCOUNT_RANGE[1]
                    and _tba.account_code not in _gl_bs_codes
                    and abs(_tba.ending_balance) > 0.01):
                _zero_activity_tb.append(_tba)

    # ── Build workpaper tabs ──────────────────────────────────
    # Summary and Trial Balance keep the period prefix (current-period snapshots).
    # Account tabs have NO period prefix — they grow as rolling history tables.
    _write_summary_tab(wb, bs_accounts, tb_map, period, property_name,
                       je_adjustments, tab_prefix=_tab_pfx,
                       zero_activity_tb_accounts=_zero_activity_tb)
    _write_tb_tab(wb, tb_result, period, property_name, tab_prefix=_tab_pfx)
    for acct in bs_accounts:
        _hist = _account_history.get(acct.account_code, [])
        if acct.account_code in _ACCRUAL_SCHEDULE_ACCOUNTS:
            _write_accrual_schedule_tab(
                wb, acct, tb_map.get(acct.account_code), period, property_name,
                _control_to_expense,
                tab_prefix='',        # no period prefix — rolling table
                history_rows=_hist)
        else:
            _write_account_tab(wb, acct, tb_map.get(acct.account_code), period,
                               property_name, je_adjustments,
                               tab_prefix='',   # no period prefix
                               history_rows=_hist)

    # ── Stub tabs for TB accounts with no current-period GL activity ──────────
    for _tba in _zero_activity_tb:
        _hist = _account_history.get(_tba.account_code, [])
        _write_stub_tab(wb, _tba, period, property_name,
                        tab_prefix='', history_rows=_hist)

    # ── Prepaid amortization schedule tab (if ledger data available) ──
    if prepaid_ledger_active:
        _write_prepaid_schedule_tab(wb, prepaid_ledger_active, period,
                                    property_name, tab_prefix=_tab_pfx)

    # ── Bank Rec tab (PNC Operating — account 111100) ──────────────────────────
    if bank_rec_data:
        # If gl_cash_balance not passed in, try to pull it from the GL accounts
        _gl_cash = gl_cash_balance
        if _gl_cash is None and gl_result:
            for _acct in (gl_result.accounts or []):
                if _acct.account_code == '111100':
                    _gl_cash = _acct.ending_balance
                    break
        _gl_cash = _gl_cash or 0.0
        _write_bank_rec_tab(
            wb, bank_rec_data, _gl_cash, period, property_name,
            account_label='PNC Operating (x3993)',
            gl_account_code='111100',
            tab_prefix=_tab_pfx,
        )

    # ── DACA Bank Rec tab (KeyBank x5132 — account 115100) ────────────────────
    if daca_bank_data is not None:
        _gl_daca = daca_gl_balance
        if _gl_daca is None and gl_result:
            for _acct in (gl_result.accounts or []):
                if _acct.account_code == '115100':
                    _gl_daca = _acct.ending_balance
                    break
        _gl_daca = _gl_daca or 0.0
        _write_daca_bank_rec_tab(
            wb, daca_bank_data, _gl_daca, period, property_name,
            tab_prefix=_tab_pfx,
        )

    # ── Development Bank Rec tab (revlabs entity — BofA x3132) ───────────────
    if dev_bank_rec_data is not None:
        # GL balance defaults to 0.0 — revlabs has no activity in the revlabpm
        # GL export; the tab shows the BofA statement balance for reference.
        _gl_dev = float(dev_bank_rec_data.get('gl_balance') or 0)
        _write_bank_rec_tab(
            wb, dev_bank_rec_data, _gl_dev, period, 'Rev Labs (revlabs)',
            account_label='Development Account (revlabs)',
            gl_account_code='',
            tab_prefix=_tab_pfx,
            tab_name_override='Bank Rec - Development',
        )

    # ── Analysis tabs (Loan, RE Tax, Insurance, Escrow) ──────────────────────
    # Copy-and-extend: copies the prior period's renamed tab, inserts new rows
    # for current-period data, and rebuilds the GL/TB tie-out from live data.
    if _build_analysis_tabs is not None:
        try:
            _build_analysis_tabs(
                wb,
                period=period,
                current_prefix=_tab_pfx,
                tab_prefix=_tab_pfx,
                gl_result=gl_result,
                tb_map=tb_map,
                berkadia_loans=berkadia_loans or [],
                prepaid_active=prepaid_ledger_active or [],
            )
        except Exception as _atb_exc:
            import traceback
            print(f"[bs_workpaper_generator] Analysis tab build warning: {_atb_exc}")
            traceback.print_exc()

    # Remove the blank default sheet openpyxl creates for new workbooks
    for _default in ('Sheet', 'Sheet1'):
        if _default in wb.sheetnames:
            del wb[_default]

    wb.save(output_path)
    return output_path


# ── Summary tab ───────────────────────────────────────────────

def _write_summary_tab(wb, bs_accounts, tb_map, period, property_name,
                       je_adjustments=None, tab_prefix: str = '',
                       zero_activity_tb_accounts: list = None):
    _tab_name = (tab_prefix + 'Summary')[:31]
    ws = wb.create_sheet(_tab_name)
    ws.sheet_properties.tabColor = COLOR_SUMMARY

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    # Title block
    c = ws.cell(row=row, column=_B, value=f'{property_name or "Revolution Labs"} — Workpaper')
    c.font = _font(bold=True, size=14, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    c = ws.cell(row=row, column=_B, value=f'Period: {period}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, size=11, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # Column headers — show projected label when JE adjustments are applied
    gl_col_label = 'GL Projected Balance' if je_adjustments else 'GL Ending Balance'
    headers = ['Account', 'Account Name', gl_col_label, 'TB Ending Balance',
               'Variance', 'Status']
    widths  = [12, 40, 22, 20, 16, 10]
    for ci, (h, w) in enumerate(zip(headers, widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', vertical='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 28
    row += 1

    # Asset / Liability / Equity groupings
    groups = [
        ('ASSETS',      lambda c: '100000' <= c <= '199999'),
        ('LIABILITIES', lambda c: '200000' <= c <= '299999'),
        ('EQUITY',      lambda c: '300000' <= c <= '399999'),
    ]

    all_pass = True
    total_gl_end = 0.0
    total_tb_end = 0.0

    # Zero-activity TB accounts keyed by code for quick lookup within groups
    _zero_map = {}
    for _z in (zero_activity_tb_accounts or []):
        _zero_map[_z.account_code] = _z

    for group_name, group_test in groups:
        group_accts = [a for a in bs_accounts if group_test(a.account_code)]
        # Zero-activity TB accounts that fall in this group (not already in bs_accounts)
        group_zero = [a for a in (zero_activity_tb_accounts or []) if group_test(a.account_code)]

        if not group_accts and not group_zero:
            continue

        # Group header
        c = ws.cell(row=row, column=_B, value=group_name)
        c.font = _font(bold=True, size=11, color=DARK_BLUE)
        c.fill = _fill(LIGHT_BLUE)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        for acct in group_accts:
            tb_acct = tb_map.get(acct.account_code)
            gl_end  = acct.ending_balance + (je_adjustments or {}).get(acct.account_code, 0.0)
            tb_end  = tb_acct.ending_balance if tb_acct else None
            variance = (gl_end - tb_end) if tb_end is not None else None
            status   = '✓' if (variance is not None and abs(variance) < 0.02) else ('⚠' if tb_end is None else '✗')
            if status != '✓':
                all_pass = False

            alt = (row % 2 == 0)
            row_fill = _fill(LIGHT_GRAY) if alt else None

            ws.cell(row=row, column=_B, value=acct.account_code).border = THIN
            ws.cell(row=row, column=_C, value=acct.account_name).border = THIN
            if row_fill:
                ws.cell(row=row, column=_B).fill = row_fill
                ws.cell(row=row, column=_C).fill = row_fill

            c_gl = ws.cell(row=row, column=_D, value=gl_end)
            _apply(c_gl, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
            if row_fill: c_gl.fill = row_fill

            if tb_end is not None:
                c_tb = ws.cell(row=row, column=_E, value=tb_end)
                _apply(c_tb, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
                if row_fill: c_tb.fill = row_fill
            else:
                c_na = ws.cell(row=row, column=_E, value='N/A in TB')
                c_na.font = _font(italic=True, color='888888')
                c_na.border = THIN

            if variance is not None:
                var_fill = _fill(GREEN_FILL) if abs(variance) < 0.02 else _fill(RED_FILL)
                c_var = ws.cell(row=row, column=_F, value=variance)
                _apply(c_var, fmt='#,##0.00;(#,##0.00);"-"', border=THIN, fill=var_fill)
                c_var.font = _font(bold=(abs(variance) >= 0.02))
            else:
                ws.cell(row=row, column=_F, value='').border = THIN

            stat_fill = _fill(GREEN_FILL) if status == '✓' else _fill(RED_FILL)
            c_stat = ws.cell(row=row, column=_G, value=status)
            _apply(c_stat, fill=stat_fill, border=THIN,
                   align=Alignment(horizontal='center'))
            c_stat.font = _font(bold=True, color='006100' if status == '✓' else '9C0006')

            total_gl_end += gl_end
            if tb_end is not None:
                total_tb_end += tb_end
            row += 1

        # ── Zero-activity TB accounts in this group ─────────────────────────────
        # GL ending balance = TB ending balance (no current-period activity).
        # Variance is always $0; status is ✓ with a lighter italic style to indicate
        # "no activity" rather than active reconciliation.
        for tb_acct in sorted(group_zero, key=lambda a: a.account_code):
            tb_end  = tb_acct.ending_balance
            gl_end  = tb_end   # no GL activity — balance unchanged from prior period
            variance = 0.0

            alt = (row % 2 == 0)
            row_fill = _fill(LIGHT_GRAY) if alt else None

            for _col, _val in [(_B, tb_acct.account_code), (_C, tb_acct.account_name)]:
                c = ws.cell(row=row, column=_col, value=_val)
                c.font = _font(italic=True, color='595959')
                c.border = THIN
                if row_fill:
                    c.fill = row_fill

            c_gl = ws.cell(row=row, column=_D, value=gl_end)
            _apply(c_gl, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
            c_gl.font = _font(italic=True, color='595959')
            if row_fill: c_gl.fill = row_fill

            c_tb = ws.cell(row=row, column=_E, value=tb_end)
            _apply(c_tb, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
            c_tb.font = _font(italic=True, color='595959')
            if row_fill: c_tb.fill = row_fill

            c_var = ws.cell(row=row, column=_F, value=variance)
            _apply(c_var, fmt='#,##0.00;(#,##0.00);"-"', border=THIN, fill=_fill(GREEN_FILL))
            c_var.font = _font(italic=True, color='006100')

            c_stat = ws.cell(row=row, column=_G, value='✓')
            _apply(c_stat, fill=_fill(GREEN_FILL), border=THIN,
                   align=Alignment(horizontal='center'))
            c_stat.font = _font(italic=True, color='006100')

            total_gl_end += gl_end
            total_tb_end += tb_end
            row += 1

        row += 1  # spacer between groups

    # Overall status banner
    status_text  = 'ALL ACCOUNTS TIE — WORKPAPER COMPLETE' if all_pass else 'VARIANCES FOUND — REVIEW REQUIRED'
    status_color = '006100' if all_pass else '9C0006'
    banner_fill  = GREEN_FILL if all_pass else RED_FILL
    c = ws.cell(row=row, column=_B, value=status_text)
    c.font = _font(bold=True, size=12, color=status_color)
    c.fill = _fill(banner_fill)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    c.alignment = Alignment(horizontal='center')
    row += 2

    # Note about variances
    if je_adjustments:
        note = ('Note: GL Projected Balance = GL ending balance + pipeline JE adjustments (accruals, '
                'management fee, prepaid amortization). Non-zero variances vs TB indicate JEs not yet '
                'posted to Yardi — expected at pre-close. Post all JEs and re-run for final tie-out.')
    else:
        note = ('Note: Non-zero variances indicate accrual journal entries posted in Yardi (visible in TB) '
                'but not yet reflected in the GL detail file. These are expected for period-end accruals.')
    c = ws.cell(row=row, column=_B, value=note)
    c.font = _font(italic=True, size=10, color='595959')
    c.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    ws.row_dimensions[row].height = 30

    ws.freeze_panes = 'B4'


# ── Trial Balance tab ─────────────────────────────────────────

def _write_tb_tab(wb, tb_result, period, property_name, tab_prefix: str = ''):
    _tab_name = (tab_prefix + 'Trial Balance')[:31]
    ws = wb.create_sheet(_tab_name)
    ws.sheet_properties.tabColor = COLOR_TB

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    c = ws.cell(row=row, column=_B, value=f'{property_name or "Revolution Labs"} — Trial Balance')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    meta_text = period
    if tb_result and tb_result.metadata:
        meta_text = f'Period: {tb_result.metadata.period}  |  Book: {tb_result.metadata.book}'
    c = ws.cell(row=row, column=_B, value=meta_text)
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # Column headers
    headers = ['Account', 'Account Name', 'Forward Balance', 'Debit', 'Credit', 'Ending Balance']
    widths  = [12, 42, 18, 18, 18, 18]
    for ci, (h, w) in enumerate(zip(headers, widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 28
    row += 1

    if not tb_result:
        ws.cell(row=row, column=_B, value='No TB data available')
        return

    # Section groupings
    sections = [
        ('ASSETS',             '100000', '199999'),
        ('LIABILITIES',        '200000', '299999'),
        ('EQUITY',             '300000', '399999'),
        ('REVENUE',            '400000', '499999'),
        ('OPERATING EXPENSES', '500000', '799999'),
        ('DEBT SERVICE',       '800000', '999999'),
    ]

    section_totals = {}
    for section_name, lo, hi in sections:
        accts = [a for a in tb_result.accounts if lo <= a.account_code <= hi]
        if not accts:
            continue

        # Section header
        c = ws.cell(row=row, column=_B, value=section_name)
        c.font = _font(bold=True, color=DARK_BLUE)
        c.fill = _fill(LIGHT_BLUE)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        sec_fwd = sec_dr = sec_cr = sec_end = 0.0
        for i, acct in enumerate(accts):
            alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
            ws.cell(row=row, column=_B, value=acct.account_code).border = THIN
            ws.cell(row=row, column=_C, value=acct.account_name).border = THIN
            if alt_fill:
                ws.cell(row=row, column=_B).fill = alt_fill
                ws.cell(row=row, column=_C).fill = alt_fill

            for ci, val in enumerate([acct.forward_balance, acct.debit,
                                       acct.credit, acct.ending_balance]):
                c = ws.cell(row=row, column=_D + ci, value=val)
                _apply(c, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
                if alt_fill:
                    c.fill = alt_fill

            sec_fwd += acct.forward_balance
            sec_dr  += acct.debit
            sec_cr  += acct.credit
            sec_end += acct.ending_balance
            row += 1

        # Section subtotal
        ws.cell(row=row, column=_C, value=f'{section_name} TOTAL').font = _font(bold=True, color=DARK_BLUE)
        ws.cell(row=row, column=_C).border = THIN
        ws.cell(row=row, column=_B).border = THIN
        for ci, val in enumerate([sec_fwd, sec_dr, sec_cr, sec_end]):
            c = ws.cell(row=row, column=_D + ci, value=val)
            _apply(c, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
                   border=THIN, fill=_fill(LIGHT_BLUE))
        row += 2

    # Grand total
    all_accts = tb_result.accounts
    ws.cell(row=row, column=_C, value='GRAND TOTAL').font = _font(bold=True, size=12)
    ws.cell(row=row, column=_C).border = DOUBLE_BTM
    ws.cell(row=row, column=_B).border = DOUBLE_BTM
    for ci, val in enumerate([
        sum(a.forward_balance for a in all_accts),
        sum(a.debit for a in all_accts),
        sum(a.credit for a in all_accts),
        sum(a.ending_balance for a in all_accts),
    ]):
        c = ws.cell(row=row, column=_D + ci, value=val)
        _apply(c, font=_font(bold=True, size=12),
               fmt='#,##0.00;(#,##0.00);"-"', border=DOUBLE_BTM)

    ws.freeze_panes = 'B5'


# ── History-extraction helpers ───────────────────────────────

def _safe_float(v):
    """Return float(v) or None if v is None/non-numeric."""
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _extract_old_format_row(ws, period_label: str):
    """
    Extract a single-period summary dict from an old-format account tab
    (transaction detail with GL/TB tie-out rows at the bottom).

    Scans for rows whose text contains:
      "ending balance per gl" → GL ending value (col _I)
      "tb balance"            → TB ending value
      "beginning balance"     → beginning balance value
    """
    gl_end   = None
    tb_end   = None
    beg_bal  = None

    for row_vals in ws.iter_rows(values_only=True, max_row=ws.max_row):
        row_str = ' '.join(str(c or '').lower() for c in row_vals)
        if ('ending balance per gl' in row_str
                or ('ending balance' in row_str and 'gl' in row_str
                    and 'tb' not in row_str and 'projected' not in row_str)):
            for c in row_vals:
                v = _safe_float(c)
                if v is not None:
                    gl_end = v
                    break
        elif 'tb balance' in row_str and gl_end is not None:
            for c in row_vals:
                v = _safe_float(c)
                if v is not None:
                    tb_end = v
                    break
        elif 'beginning balance' in row_str and beg_bal is None:
            for c in row_vals:
                v = _safe_float(c)
                if v is not None:
                    beg_bal = v
                    break

    if gl_end is None:
        return None

    tb_val     = tb_end if tb_end is not None else gl_end
    net_change = gl_end - (beg_bal or 0.0)
    return {
        'period':     period_label,
        'beg_bal':    beg_bal or 0.0,
        'net_change': round(net_change, 2),
        'gl_end':     gl_end,
        'tb_end':     tb_val,
        'variance':   round(gl_end - tb_val, 2),
    }


def _extract_new_format_history(ws) -> list:
    """
    Extract all history rows from a new-format (rolling-table) account tab.

    Looks for a header row containing "Period" in column _B; reads subsequent
    rows until a blank period cell is found.

    Columns (B..G): Period | Beg Balance | Net Activity | GL Ending | TB Ending | Variance
    """
    rows = []
    header_row = None
    for r in range(1, min(15, ws.max_row + 1)):
        val = str(ws.cell(r, _B).value or '').strip().lower()
        if val == 'period':
            header_row = r
            break
    if not header_row:
        return rows

    for r in range(header_row + 1, ws.max_row + 1):
        period_val = str(ws.cell(r, _B).value or '').strip()
        if not period_val or not re.match(
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}',
                period_val):
            break
        beg  = _safe_float(ws.cell(r, _C).value)
        net  = _safe_float(ws.cell(r, _D).value)
        gl_e = _safe_float(ws.cell(r, _E).value)
        tb_e = _safe_float(ws.cell(r, _F).value)
        var  = _safe_float(ws.cell(r, _G).value)
        if gl_e is not None:
            rows.append({
                'period':     period_val,
                'beg_bal':    beg    if beg  is not None else 0.0,
                'net_change': net    if net  is not None else 0.0,
                'gl_end':     gl_e,
                'tb_end':     tb_e   if tb_e is not None else gl_e,
                'variance':   var    if var  is not None else 0.0,
            })
    return rows


_MONTH_ORDER = dict(Jan=1, Feb=2, Mar=3, Apr=4, May=5, Jun=6,
                    Jul=7, Aug=8, Sep=9, Oct=10, Nov=11, Dec=12)


def _period_sort_key(row: dict):
    parts = str(row.get('period', '')).split('-')
    if len(parts) == 2:
        mon = _MONTH_ORDER.get(parts[0], 0)
        yr  = int(parts[1]) if parts[1].isdigit() else 0
        return (yr, mon)
    return (0, 0)


def _extract_account_history(wb_prior) -> dict:
    """
    Extract per-period summary rows from any workpaper (old or new format).

    Old format: tabs named "Jan-2026 111100" — one tab per period per account.
    New format: tabs named "111100 PNC Cash" — one rolling-table tab per account.

    Returns {account_code: [sorted list of period row dicts]}.
    """
    history: dict = {}

    for sheet_name in (wb_prior.sheetnames if wb_prior else []):
        stripped = sheet_name.strip()

        # New format: tab starts with 6-digit account code, no period prefix
        if re.match(r'^\d{6}', stripped) and not _PERIOD_PREFIX_RE.match(stripped):
            acct_code = stripped[:6]
            ws    = wb_prior[sheet_name]
            rows  = _extract_new_format_history(ws)
            if rows:
                existing = history.get(acct_code, [])
                existing_periods = {r['period'] for r in existing}
                history[acct_code] = existing + [
                    r for r in rows if r['period'] not in existing_periods
                ]
            continue

        # Old format: "Period ACCTCODE [name]", e.g. "Jan-2026 111100"
        pfx_m = _PERIOD_PREFIX_RE.match(stripped)
        if pfx_m:
            period_label = pfx_m.group(0).strip()   # "Jan-2026"
            remainder    = stripped[pfx_m.end():].strip()
            code_m       = re.match(r'^(\d{6})', remainder)
            if code_m:
                acct_code = code_m.group(1)
                ws  = wb_prior[sheet_name]
                row = _extract_old_format_row(ws, period_label)
                if row:
                    existing_periods = {r['period'] for r in history.get(acct_code, [])}
                    if period_label not in existing_periods:
                        history.setdefault(acct_code, []).append(row)

    # Sort each account's history chronologically
    for acct_code in history:
        history[acct_code] = sorted(history[acct_code], key=_period_sort_key)

    return history


# ── Account reconciliation tab ────────────────────────────────

def _write_account_tab(wb, gl_acct, tb_acct, period, property_name,
                       je_adjustments=None, tab_prefix: str = '',
                       history_rows: list = None):
    """
    One tab per balance sheet account — rolling multi-period GL vs TB tie-out.

    Tab name: no period prefix; just account code + truncated name.
    Layout: header → rolling history table (one row per prior period, then
    current period highlighted in blue).  Each row ties GL Ending to TB Ending.
    The Variance column is green for $0, red for non-zero.
    """
    # Tab name: no period prefix — account stays in one place through all months
    acct_label = _safe_sheet_name(f'{gl_acct.account_code} {gl_acct.account_name}')
    ws = wb.create_sheet(acct_label)

    is_complex = gl_acct.account_code in COMPLEX_ACCOUNTS
    ws.sheet_properties.tabColor = COLOR_BS_COMPLEX if is_complex else COLOR_BS_STD
    ws.column_dimensions['A'].width = 2

    # ── Header block ─────────────────────────────────────────────────────────
    row = 1
    c = ws.cell(row=row, column=_B,
                value=f'{gl_acct.account_code} — {gl_acct.account_name}')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=f'{property_name or "Revolution Labs"}  |  '
                      f'Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # ── Rolling-table column headers ────────────────────────────────────────
    _COL_PERIOD   = _B
    _COL_BEG      = _B + 1   # C
    _COL_NET      = _B + 2   # D
    _COL_GL_END   = _B + 3   # E
    _COL_TB_END   = _B + 4   # F
    _COL_VARIANCE = _B + 5   # G

    tbl_headers = ['Period', 'Beg Balance', 'Net Activity', 'GL Ending', 'TB Ending', 'Variance']
    tbl_widths  = [12, 18, 16, 18, 18, 14]
    for ci, (h, w) in enumerate(zip(tbl_headers, tbl_widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 24
    row += 1

    # ── Historical rows (prior periods) ──────────────────────────────────────
    for i, hist in enumerate(history_rows or []):
        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
        _var     = hist.get('variance', 0.0) or 0.0
        _vzero   = abs(_var) < 0.02

        row_data = [
            (_COL_PERIOD,   hist.get('period', ''),           None),
            (_COL_BEG,      hist.get('beg_bal', 0.0),         '#,##0.00;(#,##0.00);"-"'),
            (_COL_NET,      hist.get('net_change', 0.0),      '#,##0.00;(#,##0.00);"-"'),
            (_COL_GL_END,   hist.get('gl_end', 0.0),          '#,##0.00;(#,##0.00);"-"'),
            (_COL_TB_END,   hist.get('tb_end', hist.get('gl_end', 0.0)),
                                                               '#,##0.00;(#,##0.00);"-"'),
            (_COL_VARIANCE, _var,                              '#,##0.00;(#,##0.00);"-"'),
        ]
        for col, val, fmt in row_data:
            c = ws.cell(row=row, column=col, value=val)
            if fmt:
                c.number_format = fmt
            if alt_fill:
                c.fill = alt_fill
            c.border = THIN

        # Variance cell: green or red
        vc = ws.cell(row=row, column=_COL_VARIANCE)
        vc.fill  = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
        vc.font  = _font(color='006100' if _vzero else '9C0006')
        vc.border = THIN
        row += 1

    # ── Current period row (always last, highlighted blue) ───────────────────
    _je_delta = (je_adjustments or {}).get(gl_acct.account_code, 0.0)
    gl_end   = gl_acct.ending_balance + _je_delta
    tb_end   = tb_acct.ending_balance if tb_acct else None
    variance = round(gl_end - tb_end, 2) if tb_end is not None else None
    _vzero   = variance is not None and abs(variance) < 0.02

    cur_row_data = [
        (_COL_PERIOD,   period,                     None),
        (_COL_BEG,      gl_acct.beginning_balance,  '#,##0.00;(#,##0.00);"-"'),
        (_COL_NET,      gl_acct.net_change,          '#,##0.00;(#,##0.00);"-"'),
        (_COL_GL_END,   gl_end,                      '#,##0.00;(#,##0.00);"-"'),
        (_COL_TB_END,   tb_end if tb_end is not None else '',
                                                     '#,##0.00;(#,##0.00);"-"' if tb_end is not None else None),
        (_COL_VARIANCE, variance if variance is not None else '',
                                                     '#,##0.00;(#,##0.00);"-"' if variance is not None else None),
    ]
    for col, val, fmt in cur_row_data:
        c = ws.cell(row=row, column=col, value=val)
        if fmt:
            c.number_format = fmt
        c.fill   = _fill(LIGHT_BLUE)
        c.font   = _font(bold=True)
        c.border = THIN

    if variance is not None:
        vc = ws.cell(row=row, column=_COL_VARIANCE)
        vc.fill  = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
        vc.font  = _font(bold=True, color='006100' if _vzero else '9C0006')
        vc.border = THIN
        if not _vzero:
            note_row = row + 2
            note = ws.cell(row=note_row, column=_B,
                           value=f'Variance ${abs(variance):,.2f} — accrual JEs in TB '
                                 f'not yet in GL. Review accrual entries for {period}.')
            note.font = _font(italic=True, color='9C0006', size=10)
            note.alignment = Alignment(wrap_text=True)
            ws.merge_cells(start_row=note_row, start_column=_B,
                           end_row=note_row, end_column=_B + 5)
            ws.row_dimensions[note_row].height = 28

    row += 3  # gap before transaction detail

    # ── Transaction detail for current period ────────────────────────────────
    # Shows every GL line that makes up net activity, so the workpaper is
    # self-supporting without needing to open the full GL export.
    txns = [t for t in (gl_acct.transactions or []) if t.period == period or not t.period]
    # Fallback: if period filter returns nothing (period format mismatch), show all transactions
    if not txns:
        txns = list(gl_acct.transactions or [])

    if txns:
        # Section header
        hdr = ws.cell(row=row, column=_B, value=f'GL Transaction Detail — {period}')
        hdr.font = _font(bold=True, size=10, color='FFFFFF')
        hdr.fill = _fill(MED_BLUE)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 6)
        row += 1

        # Column headers
        _DT  = _B       # B  Date
        _DSC = _B + 1   # C  Description
        _CTL = _B + 2   # D  Control / JE#
        _REF = _B + 3   # E  Reference
        _DR  = _B + 4   # F  Debit
        _CR  = _B + 5   # G  Credit
        _BAL = _B + 6   # H  Balance

        txn_hdrs  = ['Date', 'Description', 'Control', 'Reference', 'Debit', 'Credit', 'Balance']
        txn_widths = [12,     38,             14,         18,          14,      14,       16]
        for ci, (h, w) in enumerate(zip(txn_hdrs, txn_widths)):
            col = _B + ci
            c = ws.cell(row=row, column=col, value=h)
            _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
                   align=Alignment(horizontal='center'))
            ws.column_dimensions[get_column_letter(col)].width = w
        row += 1

        # Beginning balance row
        bb_row_vals = [
            (_DT,  ''),
            (_DSC, 'Beginning Balance'),
            (_CTL, ''),
            (_REF, ''),
            (_DR,  ''),
            (_CR,  ''),
            (_BAL, gl_acct.beginning_balance),
        ]
        for col, val in bb_row_vals:
            c = ws.cell(row=row, column=col, value=val)
            c.font   = _font(bold=True, italic=True, size=9)
            c.fill   = _fill(LIGHT_GRAY)
            c.border = THIN
            if isinstance(val, float):
                c.number_format = '#,##0.00;(#,##0.00);"-"'
        row += 1

        # Transaction rows
        for ti, t in enumerate(txns):
            alt = _fill(LIGHT_GRAY) if ti % 2 == 1 else None
            t_date = t.date.strftime('%m/%d/%Y') if t.date else ''
            debit  = t.debit  if (t.debit  or 0) > 0.005 else ''
            credit = t.credit if (t.credit or 0) > 0.005 else ''
            row_vals = [
                (_DT,  t_date),
                (_DSC, (t.description or '') + (' — ' + t.remarks if t.remarks else '')),
                (_CTL, t.control or ''),
                (_REF, t.reference or ''),
                (_DR,  debit),
                (_CR,  credit),
                (_BAL, t.balance),
            ]
            for col, val in row_vals:
                c = ws.cell(row=row, column=col, value=val)
                c.font   = _font(size=9)
                c.border = THIN
                if alt:
                    c.fill = alt
                if col in (_DR, _CR, _BAL) and isinstance(val, float):
                    c.number_format = '#,##0.00;(#,##0.00);"-"'
            row += 1

        # Ending balance row
        eb_row_vals = [
            (_DT,  ''),
            (_DSC, 'Ending Balance'),
            (_CTL, ''),
            (_REF, ''),
            (_DR,  ''),
            (_CR,  ''),
            (_BAL, gl_acct.ending_balance),
        ]
        for col, val in eb_row_vals:
            c = ws.cell(row=row, column=col, value=val)
            c.font   = _font(bold=True, size=9)
            c.fill   = _fill(LIGHT_BLUE)
            c.border = THIN
            if isinstance(val, float):
                c.number_format = '#,##0.00;(#,##0.00);"-"'

    ws.freeze_panes = 'B5'


def _write_tieout(ws, row, gl_acct, tb_acct, period, je_delta: float = 0.0):
    """Write the GL ending / TB balance / Variance tie-out block (Hartwell inline style)."""

    # Separator line
    for col in range(_B, _I + 1):
        ws.cell(row=row, column=col).border = THICK_BOTTOM
    row += 1

    gl_ending = gl_acct.ending_balance + je_delta   # projected post-close if je_delta != 0
    tb_ending = tb_acct.ending_balance if tb_acct else None
    variance  = (gl_ending - tb_ending) if tb_ending is not None else None

    # Blank separator row (already advanced past separator line above)
    row += 1

    # GL ending balance — Hartwell inline style
    # Label in _D (description col), value in _I (balance col), bold, light blue fill across data cols
    _gl_label = (f'Projected Balance per GL as of {period} (incl. pipeline JEs)'
                 if je_delta != 0.0 else f'Ending Balance per GL as of {period}')
    label_gl = ws.cell(row=row, column=_D, value=_gl_label)
    label_gl.font = _font(bold=True)
    c_gl = ws.cell(row=row, column=_I, value=gl_ending)
    _apply(c_gl, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THICK_BOTTOM)
    for col in range(_B, _I + 1):
        cell = ws.cell(row=row, column=col)
        if not cell.fill or cell.fill.fill_type == 'none':
            cell.fill = _fill(LIGHT_BLUE)
    row += 1

    # TB balance — label in _H, value in _I
    label_tb = ws.cell(row=row, column=_H, value='TB Balance')
    label_tb.font = _font(bold=True)
    if tb_ending is not None:
        c_tb = ws.cell(row=row, column=_I, value=tb_ending)
        _apply(c_tb, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
               fill=_fill(LIGHT_BLUE), border=THIN)
    else:
        c_tb = ws.cell(row=row, column=_I, value='Not in TB')
        c_tb.font = _font(italic=True, color='888888')
    row += 1

    # Variance — label in _H, value in _I; green if zero, red if non-zero
    label_var = ws.cell(row=row, column=_H, value='Variance')
    label_var.font = _font(bold=True)
    if variance is not None:
        is_zero = abs(variance) < 0.02
        var_fill = _fill(GREEN_FILL) if is_zero else _fill(RED_FILL)
        var_color = '006100' if is_zero else '9C0006'
        c_var = ws.cell(row=row, column=_I, value=variance)
        _apply(c_var, font=_font(bold=True, color=var_color),
               fmt='#,##0.00;(#,##0.00);"-"', fill=var_fill, border=DOUBLE_BTM)

        if not is_zero:
            note_row = row + 2
            note = ws.cell(row=note_row, column=_B,
                           value=f'Variance of ${abs(variance):,.2f} — likely accrual entries in TB not yet in GL. '
                                 f'Review accrual JEs posted for this account.')
            note.font = _font(italic=True, color='9C0006', size=10)
            note.alignment = Alignment(wrap_text=True)
            ws.merge_cells(start_row=note_row, start_column=_B,
                           end_row=note_row, end_column=_I)
            ws.row_dimensions[note_row].height = 28
    else:
        ws.cell(row=row, column=_I, value='').border = DOUBLE_BTM


# ── Accrual schedule helpers ─────────────────────────────────

def _parse_accrual_txn(desc: str, expense_name: str = '') -> dict:
    """
    Parse a pipeline-generated accrual description into structured fields for
    the JLL-style accrual schedule tab.

    Returns dict with keys:
        acct_desc   — expense account name (from description or expense_name arg)
        vendor      — vendor name if identifiable
        period_from — billing/service period start (string)
        period_to   — billing/service period end (string)
        notes       — short description line (matches JLL "Acc …" style)
    """
    import re as _re
    result = {
        'acct_desc': expense_name or '',
        'vendor': '',
        'period_from': '',
        'period_to': '',
        'notes': (desc or '').strip(),
    }
    if not desc:
        return result

    # "Invoice proration — Account Name: last invoice MM/DD/YY-MM/DD/YY vendor..."
    m = _re.match(
        r'Invoice proration\s*[—\-]+\s*(.+?):\s*last invoice\s+([\d/][\d/\- ]+?)'
        r'(?:\s+(.+?))?$', desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        dates_str = m.group(2).strip()
        date_parts = _re.split(r'\s*[-–]\s*', dates_str)
        if len(date_parts) >= 2:
            result['period_from'] = date_parts[0].strip()
            result['period_to']   = date_parts[1].strip()
        vendor_extra = (m.group(3) or '').strip()
        if vendor_extra:
            result['vendor'] = vendor_extra[:40]
        result['notes'] = (
            f"Acc {result['period_from']} - {result['period_to']} "
            f"{result['acct_desc']}"
        ).strip()
        return result

    # "Payroll accrual — Account Name: last run MM/DD/YY (…)"
    m = _re.match(
        r'Payroll accrual\s*[—\-]+\s*(.+?):\s*last run\s+(\d{1,2}/\d{1,2}/\d{2,4})',
        desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        last_run = m.group(2).strip()
        result['period_from'] = last_run
        result['vendor'] = 'Payroll'
        result['notes'] = f"Acc payroll last run {last_run} {result['acct_desc']}"
        return result

    # "Monthly bonus accrual — Account Name: Kardin annual…"
    m = _re.match(r'Monthly bonus accrual\s*[—\-]+\s*(.+?):', desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        result['vendor'] = 'Bonus accrual'
        result['notes'] = f"Acc bonus per Kardin {result['acct_desc']}"
        return result

    # "Recurring monthly accrual — Account Name: VENDOR"
    m = _re.match(
        r'Recurring monthly accrual\s*[—\-]+\s*(.+?):\s*(.+?)$', desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        result['vendor'] = m.group(2).strip()[:40]
        result['notes'] = f"Acc {result['vendor']}"
        return result

    # "Budget gap accrual — Account Name: …"
    m = _re.match(r'Budget gap accrual\s*[—\-]+\s*(.+?):\s*(.+?)$', desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        result['notes'] = m.group(2).strip()[:60]
        return result

    # "REVIEW REQUIRED — Account Name: …" / "REVIEW — …"
    m = _re.match(r'REVIEW(?:\s+REQUIRED)?\s*[—\-]+\s*(.+?):', desc, _re.I)
    if m:
        if not result['acct_desc']:
            result['acct_desc'] = m.group(1).strip()
        result['vendor'] = '⚠ REVIEW'
        result['notes'] = desc.strip()
        return result

    return result


def _write_accrual_schedule_tab(wb, gl_acct, tb_acct, period, property_name,
                                 control_to_expense: dict, tab_prefix: str = '',
                                 history_rows: list = None):
    """
    Write a JLL-style accrual schedule tab for 211200 / 211300 / 213100.

    Layout matches '213100-Accrued Exp' in the JLL workpaper:
      Col B  Account #         (expense account code from GL debit side)
      Col C  Account Desc      (expense account name)
      Col D  Vendor            (parsed from description)
      Col E  FROM              (billing/service period start)
      Col F  TO                (billing/service period end)
      Col G  Accrual           (negative — credit to this liability account)
      Col H  Description       (short note matching "Acc MM/YY Vendor" style)

    Footer: total → GL balance → variance (should be ≤ $0.02 rounding)
    TB tie-out row appended after the GL section.
    """
    acct_label = _safe_sheet_name(f'{gl_acct.account_code} {gl_acct.account_name}')
    ws = wb.create_sheet(acct_label)
    ws.sheet_properties.tabColor = COLOR_BS_COMPLEX  # red — complex account
    ws.column_dimensions['A'].width = 2

    row = 1
    # ── Header ───────────────────────────────────────────────────
    c = ws.cell(row=row, column=_B,
                value=f'{gl_acct.account_code} — {gl_acct.account_name}')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_I)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=(f'Period: {period}  |  '
                       f'{property_name or "Revolution Labs"}  |  '
                       f'Prepared: {datetime.now().strftime("%m/%d/%Y")}'))
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_I)
    row += 3

    # ── Column headers ────────────────────────────────────────────
    col_specs = [
        ('Account #',        10),
        ('Account Description', 28),
        ('Vendor',           24),
        ('FROM',             14),
        ('TO',               14),
        ('Accrual',          14),
        ('Description',      48),
        ('',                  4),
    ]
    for ci, (h, w) in enumerate(col_specs):
        col = _B + ci
        ws.column_dimensions[get_column_letter(col)].width = w
        if not h:
            continue
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
    ws.row_dimensions[row].height = 24
    row += 1

    # ── Data rows ─────────────────────────────────────────────────
    total_accrual = 0.0
    txns = gl_acct.transactions or []

    for i, txn in enumerate(txns):
        credit = float(txn.credit or 0)
        debit  = float(txn.debit or 0)
        # Net credit = how much is accrued into this liability account
        net_credit = credit - debit

        ctrl = str(getattr(txn, 'control', '') or '').strip()
        expense_info = control_to_expense.get(ctrl, ('', ''))
        expense_code = expense_info[0] if expense_info else ''
        expense_name = expense_info[1] if expense_info else ''

        parsed = _parse_accrual_txn(txn.description or '', expense_name)

        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
        is_review = parsed['vendor'] == '⚠ REVIEW'

        # Accrual amount stored as negative (matching JLL's sign convention for credits)
        accrual_amount = -net_credit if net_credit != 0 else None

        row_data = [
            (expense_code,                   'left',   False),
            (parsed['acct_desc'] or expense_name or (txn.description or '')[:40],
                                              'left',   False),
            (parsed['vendor'],               'left',   False),
            (parsed['period_from'],          'center',  False),
            (parsed['period_to'],            'center',  False),
            (accrual_amount,                 'right',   True),   # number format
            (parsed['notes'][:65],           'left',   False),
        ]

        for ci, (val, align_h, is_num) in enumerate(row_data):
            col = _B + ci
            c = ws.cell(row=row, column=col, value=val)
            c.alignment = Alignment(horizontal=align_h,
                                    wrap_text=(ci == 6))
            c.border = THIN
            if alt_fill:
                c.fill = alt_fill
            if is_num and val is not None:
                c.number_format = '#,##0.00;(#,##0.00);"-"'
            if is_review:
                c.font = _font(bold=True, color='9C0006')

        if accrual_amount is not None:
            total_accrual += accrual_amount
        row += 1

    # ── Total row ─────────────────────────────────────────────────
    row += 1
    ws.cell(row=row, column=_D, value='Rounding').font = _font(italic=True, color='888888')
    row += 1
    ws.cell(row=row, column=_D, value='Total').font = _font(bold=True)
    c_tot = ws.cell(row=row, column=_G, value=total_accrual)
    _apply(c_tot, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
    row += 2

    # ── GL balance + variance ─────────────────────────────────────
    gl_ending = gl_acct.ending_balance
    ws.cell(row=row, column=_D, value=str(gl_acct.account_code)).font = _font(bold=True, color=DARK_BLUE)
    row += 2

    ws.cell(row=row, column=_E, value='GL').font = _font(bold=True)
    c_gl = ws.cell(row=row, column=_G, value=gl_ending)
    _apply(c_gl, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THIN)
    row += 1

    # Variance between schedule total and GL ending balance
    sched_variance = (total_accrual + abs(gl_ending)) if gl_ending is not None else None
    ws.cell(row=row, column=_E, value='Variance').font = _font(bold=True)
    if sched_variance is not None:
        is_zero = abs(sched_variance) < 0.02
        c_sv = ws.cell(row=row, column=_G, value=sched_variance if not is_zero else 0)
        _apply(c_sv,
               font=_font(bold=True, color='006100' if is_zero else '9C0006'),
               fmt='#,##0.00;(#,##0.00);"-"',
               fill=_fill(GREEN_FILL if is_zero else RED_FILL),
               border=DOUBLE_BTM)
    row += 2

    # ── TB tie-out (below GL section) ─────────────────────────────
    tb_ending = tb_acct.ending_balance if tb_acct else None
    variance  = (gl_ending - tb_ending) if tb_ending is not None else None

    ws.cell(row=row, column=_H, value='TB Balance').font = _font(bold=True)
    if tb_ending is not None:
        c_tb = ws.cell(row=row, column=_I, value=tb_ending)
        _apply(c_tb, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
               fill=_fill(LIGHT_BLUE), border=THIN)
    else:
        c_tb = ws.cell(row=row, column=_I, value='Not in TB')
        c_tb.font = _font(italic=True, color='888888')
    row += 1

    ws.cell(row=row, column=_H, value='Variance').font = _font(bold=True)
    if variance is not None:
        is_zero = abs(variance) < 0.02
        c_var = ws.cell(row=row, column=_I, value=variance)
        _apply(c_var,
               font=_font(bold=True, color='006100' if is_zero else '9C0006'),
               fmt='#,##0.00;(#,##0.00);"-"',
               fill=_fill(GREEN_FILL if is_zero else RED_FILL),
               border=DOUBLE_BTM)
    else:
        ws.cell(row=row, column=_I, value='').border = DOUBLE_BTM

    # ── Historical rollforward (below tie-out) ────────────────────────────────
    if history_rows:
        row += 3
        c = ws.cell(row=row, column=_B, value='Historical GL vs TB Rollforward')
        c.font = _font(bold=True, color='FFFFFF')
        c.fill = _fill(DARK_BLUE)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        hist_hdrs = ['Period', 'Beg Balance', 'Net Activity', 'GL Ending', 'TB Ending', 'Variance']
        hist_wids = [12, 18, 16, 18, 18, 14]
        for ci, (h, w) in enumerate(zip(hist_hdrs, hist_wids)):
            col = _B + ci
            c = ws.cell(row=row, column=col, value=h)
            _apply(c, font=_hdr_font(), fill=_fill(MED_BLUE), border=THIN,
                   align=Alignment(horizontal='center', wrap_text=True))
        row += 1

        for i, hist in enumerate(history_rows):
            alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
            _var   = hist.get('variance', 0.0) or 0.0
            _vzero = abs(_var) < 0.02
            row_data = [
                (_B,     hist.get('period', ''),      None),
                (_B + 1, hist.get('beg_bal', 0.0),    '#,##0.00;(#,##0.00);"-"'),
                (_B + 2, hist.get('net_change', 0.0), '#,##0.00;(#,##0.00);"-"'),
                (_B + 3, hist.get('gl_end', 0.0),     '#,##0.00;(#,##0.00);"-"'),
                (_B + 4, hist.get('tb_end', 0.0),     '#,##0.00;(#,##0.00);"-"'),
                (_B + 5, _var,                         '#,##0.00;(#,##0.00);"-"'),
            ]
            for col, val, fmt in row_data:
                c = ws.cell(row=row, column=col, value=val)
                if fmt:
                    c.number_format = fmt
                if alt_fill:
                    c.fill = alt_fill
                c.border = THIN
            vc = ws.cell(row=row, column=_B + 5)
            vc.fill = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
            vc.font = _font(color='006100' if _vzero else '9C0006')
            row += 1

        # Current period row in blue
        cur_data = [
            (_B,     period,            None),
            (_B + 1, gl_acct.beginning_balance, '#,##0.00;(#,##0.00);"-"'),
            (_B + 2, gl_acct.net_change,         '#,##0.00;(#,##0.00);"-"'),
            (_B + 3, gl_ending,                  '#,##0.00;(#,##0.00);"-"'),
            (_B + 4, tb_ending if tb_ending is not None else '', '#,##0.00;(#,##0.00);"-"' if tb_ending is not None else None),
            (_B + 5, variance  if variance  is not None else '', '#,##0.00;(#,##0.00);"-"' if variance  is not None else None),
        ]
        for col, val, fmt in cur_data:
            c = ws.cell(row=row, column=col, value=val)
            if fmt:
                c.number_format = fmt
            c.fill = _fill(LIGHT_BLUE)
            c.font = _font(bold=True)
            c.border = THIN
        if variance is not None:
            _vzero = abs(variance) < 0.02
            vc = ws.cell(row=row, column=_B + 5)
            vc.fill = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
            vc.font = _font(bold=True, color='006100' if _vzero else '9C0006')

    ws.freeze_panes = 'B5'


# ── Stub tab for zero-activity BS accounts ───────────────────

def _write_stub_tab(wb, tb_acct, period: str, property_name: str,
                    tab_prefix: str = '',
                    history_rows: list = None):
    """
    Stub tab for a BS account in the TB with no current-period GL transactions.
    Uses the same rolling-table format as _write_account_tab.
    Current-period row: net activity = 0, GL ending = TB forward balance.
    """
    acct_label = _safe_sheet_name(f'{tb_acct.account_code} {tb_acct.account_name}')
    ws = wb.create_sheet(acct_label)
    ws.sheet_properties.tabColor = COLOR_BS_STD
    ws.column_dimensions['A'].width = 2

    row = 1
    c = ws.cell(row=row, column=_B,
                value=f'{tb_acct.account_code} — {tb_acct.account_name}')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=f'{property_name or "Revolution Labs"}  |  '
                      f'Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    tbl_headers = ['Period', 'Beg Balance', 'Net Activity', 'GL Ending', 'TB Ending', 'Variance']
    tbl_widths  = [12, 18, 16, 18, 18, 14]
    for ci, (h, w) in enumerate(zip(tbl_headers, tbl_widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 24
    row += 1

    for i, hist in enumerate(history_rows or []):
        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
        _var   = hist.get('variance', 0.0) or 0.0
        _vzero = abs(_var) < 0.02
        row_data = [
            (_B,     hist.get('period', ''),      None),
            (_B + 1, hist.get('beg_bal', 0.0),    '#,##0.00;(#,##0.00);"-"'),
            (_B + 2, hist.get('net_change', 0.0), '#,##0.00;(#,##0.00);"-"'),
            (_B + 3, hist.get('gl_end', 0.0),     '#,##0.00;(#,##0.00);"-"'),
            (_B + 4, hist.get('tb_end', 0.0),     '#,##0.00;(#,##0.00);"-"'),
            (_B + 5, _var,                         '#,##0.00;(#,##0.00);"-"'),
        ]
        for col, val, fmt in row_data:
            c = ws.cell(row=row, column=col, value=val)
            if fmt:
                c.number_format = fmt
            if alt_fill:
                c.fill = alt_fill
            c.border = THIN
        vc = ws.cell(row=row, column=_B + 5)
        vc.fill = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
        vc.font = _font(color='006100' if _vzero else '9C0006')
        row += 1

    # Current period: no GL activity → ending = forward balance from TB
    fwd      = getattr(tb_acct, 'forward_balance', None) or tb_acct.ending_balance
    gl_end   = fwd
    tb_end   = tb_acct.ending_balance
    variance = round(gl_end - tb_end, 2)
    _vzero   = abs(variance) < 0.02

    cur_data = [
        (_B,     period,  None),
        (_B + 1, fwd,     '#,##0.00;(#,##0.00);"-"'),
        (_B + 2, 0.0,     '#,##0.00;(#,##0.00);"-"'),
        (_B + 3, gl_end,  '#,##0.00;(#,##0.00);"-"'),
        (_B + 4, tb_end,  '#,##0.00;(#,##0.00);"-"'),
        (_B + 5, variance,'#,##0.00;(#,##0.00);"-"'),
    ]
    for col, val, fmt in cur_data:
        c = ws.cell(row=row, column=col, value=val)
        if fmt:
            c.number_format = fmt
        c.fill = _fill(LIGHT_BLUE)
        c.font = _font(bold=True)
        c.border = THIN
    vc = ws.cell(row=row, column=_B + 5)
    vc.fill = _fill(GREEN_FILL) if _vzero else _fill(RED_FILL)
    vc.font = _font(bold=True, color='006100' if _vzero else '9C0006')

    ws.freeze_panes = 'B5'


# ── Prepaid amortization schedule tab ────────────────────────

def _write_prepaid_schedule_tab(wb, active_items: list, period: str, property_name: str,
                                 tab_prefix: str = ''):
    """
    Adds a 'Prepaid Schedule' tab using the Hartwell 13-column format.
    Tied to accounts 135xxx.

    Columns _B.._N:
      _B  Description
      _C  G/L Account
      _D  Payment Date
      _E  Payment Amount
      _F  Period Covered
      _G  Start Date
      _H  End Date
      _I  # of Mos. Covered
      _J  Exp per Month
      _K  Months Elapsed
      _L  # of Mos Prepaid
      _M  Prepaid Balance
      _N  Expense Balance
    """
    COLOR_PREPAID = 'ED7D31'   # orange tab — matches prepaid ledger convention

    # Extended column constants for this tab
    _J = 10
    _K = 11
    _L = 12
    _M = 13
    _N = 14

    _tab_name = (tab_prefix + 'Prepaid Schedule')[:31]
    ws = wb.create_sheet(_tab_name)
    ws.sheet_properties.tabColor = COLOR_PREPAID

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    c = ws.cell(row=row, column=_B,
                value=f'{property_name or "Revolution Labs"} — Prepaid Expense Schedule')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(COLOR_PREPAID)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_N)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=f'Period: {period}  |  Active items as of close  |  Account 135xxx — Prepaid Expenses')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(COLOR_PREPAID)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_N)
    row += 2

    # Column headers
    headers = [
        'Description',       # _B
        'G/L Account',       # _C
        'Payment Date',      # _D
        'Payment Amount',    # _E
        'Period Covered',    # _F
        'Start Date',        # _G
        'End Date',          # _H
        '# of Mos. Covered', # _I
        'Exp per Month',     # _J
        'Months Elapsed',    # _K
        '# of Mos Prepaid',  # _L
        'Prepaid Balance',   # _M
        'Expense Balance',   # _N
    ]
    widths = [32, 13, 13, 16, 20, 13, 13, 15, 15, 15, 15, 16, 16]
    for ci, (h, w) in enumerate(zip(headers, widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 28
    row += 1

    total_prepaid = 0.0
    total_expense = 0.0

    for i, item in enumerate(active_items):
        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None

        months_elapsed  = int(item.get('months_amortized', 0) or 0)
        months_prepaid  = int(item.get('remaining_months', 0) or 0)
        exp_per_month   = float(item.get('monthly_amount', 0) or 0)
        prepaid_balance = exp_per_month * months_prepaid
        expense_balance = exp_per_month * months_elapsed
        total_prepaid  += prepaid_balance
        total_expense  += expense_balance

        total_months = int(item.get('total_months', 0) or 0)
        total_amount = float(item.get('total_amount', 0) or 0)

        # Payment date
        svc_start = item.get('service_start', '')
        if svc_start and hasattr(svc_start, 'strftime'):
            pay_date_str = svc_start.strftime('%m/%d/%Y')
        else:
            pay_date_str = str(svc_start) if svc_start else ''

        # Start / End date strings
        svc_end = item.get('service_end', '')
        if svc_end and hasattr(svc_end, 'strftime'):
            end_date_str = svc_end.strftime('%m/%d/%Y')
        else:
            end_date_str = str(svc_end) if svc_end else ''

        if svc_start and hasattr(svc_start, 'strftime'):
            start_date_str = svc_start.strftime('%m/%d/%Y')
        else:
            start_date_str = str(svc_start) if svc_start else ''

        # Period covered — use field if set, else format from start/end
        period_covered = item.get('period_covered', '')
        if not period_covered and svc_start and svc_end:
            try:
                if hasattr(svc_start, 'strftime') and hasattr(svc_end, 'strftime'):
                    period_covered = (f'{svc_start.strftime("%m.%d.%y")} - '
                                      f'{svc_end.strftime("%m.%d.%y")}')
            except Exception:
                period_covered = ''

        description = item.get('description', '') or item.get('vendor', '')

        row_vals = [
            description,                         # _B
            item.get('gl_account_number', ''),   # _C
            pay_date_str,                         # _D  Payment Date
            total_amount,                         # _E  Payment Amount
            period_covered,                       # _F  Period Covered
            start_date_str,                       # _G  Start Date
            end_date_str,                         # _H  End Date
            total_months,                         # _I  # of Mos. Covered
            exp_per_month,                        # _J  Exp per Month
            months_elapsed,                       # _K  Months Elapsed
            months_prepaid,                       # _L  # of Mos Prepaid
            prepaid_balance,                      # _M  Prepaid Balance
            expense_balance,                      # _N  Expense Balance
        ]

        for ci, val in enumerate(row_vals):
            col = _B + ci
            c = ws.cell(row=row, column=col, value=val)
            c.border = THIN
            if alt_fill:
                c.fill = alt_fill
            # Number formats
            if col == _E:   # Payment Amount
                c.number_format = '#,##0.00;(#,##0.00);"-"'
            elif col in (_J, _M, _N):  # Exp per Month, Prepaid Balance, Expense Balance
                c.number_format = '#,##0.00;(#,##0.00);"-"'
            elif col in (_I, _K, _L):  # integer month counts
                c.alignment = Alignment(horizontal='center')
                # Color-code months prepaid (_L)
                if col == _L:
                    if months_prepaid == 0:
                        c.font = _font(color='FF0000', bold=True)
                    elif months_prepaid == 1:
                        c.font = _font(color='C55A11', bold=True)
        row += 1

    # ── Footer tie-out rows ────────────────────────────────────────────────────
    row += 1  # blank separator

    # "Ending Balance per GL as of [period]" — label in _D, prepaid total in _M, expense total in _N
    c_lbl = ws.cell(row=row, column=_D,
                    value=f'Ending Balance per GL as of {period}')
    c_lbl.font = _font(bold=True)
    c_lbl.fill = _fill(LIGHT_BLUE)
    c_prepaid_tot = ws.cell(row=row, column=_M, value=total_prepaid)
    _apply(c_prepaid_tot, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THICK_BOTTOM)
    c_exp_tot = ws.cell(row=row, column=_N, value=total_expense)
    _apply(c_exp_tot, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THICK_BOTTOM)
    # Fill data cols with light blue
    for col in range(_B, _N + 1):
        cell = ws.cell(row=row, column=col)
        if not cell.fill or cell.fill.fill_type == 'none':
            cell.fill = _fill(LIGHT_BLUE)
    row += 1

    # "TB Balance" — label in _L, value in _M
    c_tb_lbl = ws.cell(row=row, column=_L, value='TB Balance')
    c_tb_lbl.font = _font(bold=True)
    c_tb_val = ws.cell(row=row, column=_M, value=total_prepaid)
    _apply(c_tb_val, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THIN)
    row += 1

    # "Variance" — label in _L, value in _M; green/red
    c_var_lbl = ws.cell(row=row, column=_L, value='Variance')
    c_var_lbl.font = _font(bold=True)
    variance = 0.0  # prepaid balance ties to itself; placeholder for manual TB entry
    is_zero = abs(variance) < 0.02
    var_fill_cell = _fill(GREEN_FILL) if is_zero else _fill(RED_FILL)
    var_color = '006100' if is_zero else '9C0006'
    c_var_val = ws.cell(row=row, column=_M, value=variance)
    _apply(c_var_val, font=_font(bold=True, color=var_color),
           fmt='#,##0.00;(#,##0.00);"-"', fill=var_fill_cell, border=DOUBLE_BTM)
    row += 2

    # "[Add Row]" placeholder for manual additions
    c_add = ws.cell(row=row, column=_B, value='[Add Row]')
    c_add.font = _font(italic=True, color='888888')
    c_add.border = THIN
    for col in range(_C, _N + 1):
        ws.cell(row=row, column=col).border = THIN
    row += 2

    note = ws.cell(row=row, column=_B,
                   value='Prepaid Balance = Exp per Month × # of Mos Prepaid. '
                         'Expense Balance = Exp per Month × Months Elapsed. '
                         'These balances should agree to accounts 135xxx in the TB.')
    note.font = _font(italic=True, size=10, color='595959')
    note.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_N)
    ws.row_dimensions[row].height = 28

    ws.freeze_panes = 'B5'


# ── Bank Rec tab ─────────────────────────────────────────────

COLOR_BANK_REC = '375623'   # dark green tab

def _write_bank_rec_tab(wb, bank_rec_data: dict, gl_acct_balance: float,
                        period: str, property_name: str,
                        account_label: str = 'PNC Operating (x3993)',
                        gl_account_code: str = '111100',
                        tab_prefix: str = '',
                        tab_name_override: str = None):
    """
    Writes one Bank Rec tab showing:
      Balance per Bank Statement
      Less: Outstanding Checks
      = Reconciled Bank Balance  →  must equal GL cash account
    Then lists outstanding checks and cleared checks for reference.
    """
    if tab_name_override:
        _base_name = tab_name_override
    else:
        _base_name = f'Bank Rec - {account_label.split("(")[0].strip()[:20]}'
    tab_name = (tab_prefix + _base_name)[:31]
    ws = wb.create_sheet(tab_name)
    ws.sheet_properties.tabColor = COLOR_BANK_REC

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    # Header
    c = ws.cell(row=row, column=_B,
                value=f'{property_name or "Revolution Labs"} — Bank Reconciliation')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(COLOR_BANK_REC)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=f'Account: {account_label}  |  Period: {period}  |  '
                      f'Prepared by: GRP  |  {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(COLOR_BANK_REC)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # Column widths
    for ci, w in enumerate([18, 15, 45, 18, 6, 6]):
        ws.column_dimensions[get_column_letter(_B + ci)].width = w

    # ── Reconciliation Summary ────────────────────────────────
    bank_bal    = float(bank_rec_data.get('bank_statement_balance', 0) or 0)
    out_total   = float(bank_rec_data.get('total_outstanding_checks', 0) or 0)
    rec_bal     = float(bank_rec_data.get('reconciled_bank_balance', 0) or 0)
    difference  = rec_bal - gl_acct_balance

    def _rec_row(label, value, bold=False, fill_hex=None, border=THIN, fmt='#,##0.00;(#,##0.00);"-"'):
        nonlocal row
        c_lbl = ws.cell(row=row, column=_B, value=label)
        c_lbl.font = _font(bold=bold)
        c_lbl.alignment = Alignment(horizontal='right')
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 3)
        c_val = ws.cell(row=row, column=_B + 4, value=value)
        _apply(c_val, font=_font(bold=bold), fmt=fmt, border=border)
        ws.merge_cells(start_row=row, start_column=_B + 4, end_row=row, end_column=_B + 5)
        if fill_hex:
            c_val.fill = _fill(fill_hex)
        row += 1

    _rec_row('Balance Per Bank Statement:', bank_bal)
    _rec_row(f'  Less: Outstanding Checks:', -out_total)
    ws.cell(row=row - 1, column=_B + 4).border = THICK_BOTTOM
    _rec_row('Reconciled Bank Balance:', rec_bal, bold=True, fill_hex=LIGHT_BLUE, border=DOUBLE_BTM)
    row += 1
    _rec_row(f'Balance per GL — {gl_account_code}:', gl_acct_balance, bold=True, fill_hex=LIGHT_BLUE)

    # Variance row
    is_clean = abs(difference) < 0.02
    var_fill  = GREEN_FILL if is_clean else RED_FILL
    var_color = '006100' if is_clean else '9C0006'
    c_lbl = ws.cell(row=row, column=_B, value='Difference:')
    c_lbl.font = _font(bold=True, color=var_color)
    c_lbl.alignment = Alignment(horizontal='right')
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 3)
    c_val = ws.cell(row=row, column=_B + 4, value=difference)
    _apply(c_val, font=_font(bold=True, color=var_color),
           fmt='#,##0.00;(#,##0.00);"-"', fill=_fill(var_fill), border=DOUBLE_BTM)
    ws.merge_cells(start_row=row, start_column=_B + 4, end_row=row, end_column=_B + 5)
    row += 2

    if not is_clean:
        note = ws.cell(row=row, column=_B,
                       value=f'Reconciling difference of ${abs(difference):,.2f} — investigate before close.')
        note.font = _font(italic=True, color='9C0006', size=10)
        note.alignment = Alignment(wrap_text=True)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 2

    # ── Outstanding Checks ────────────────────────────────────
    outstanding = bank_rec_data.get('outstanding_checks', [])
    if outstanding:
        c = ws.cell(row=row, column=_B, value='Outstanding Checks')
        c.font = _font(bold=True, size=12, color='FFFFFF')
        c.fill = _fill(COLOR_BANK_REC)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        hdrs = ['Check Date', 'Check #', 'Payee', 'Amount', '', '']
        for ci, h in enumerate(hdrs[:4]):
            c = ws.cell(row=row, column=_B + ci, value=h)
            _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
                   align=Alignment(horizontal='center'))
        row += 1

        for i, chk in enumerate(outstanding):
            payee = str(chk.get('payee', '')).split(' - ', 1)[-1]  # strip vendor code prefix
            alt   = _fill(LIGHT_GRAY) if i % 2 == 1 else None
            ws.cell(row=row, column=_B, value=chk.get('date', '')).border = THIN
            ws.cell(row=row, column=_C, value=str(chk.get('check_number', ''))).border = THIN
            ws.cell(row=row, column=_D, value=payee).border = THIN
            c_amt = ws.cell(row=row, column=_E, value=float(chk.get('amount', 0)))
            _apply(c_amt, fmt='#,##0.00', border=THIN)
            if alt:
                for col in range(_B, _B + 4):
                    ws.cell(row=row, column=col).fill = alt
            row += 1

        # Outstanding total
        ws.cell(row=row, column=_D, value='Total Outstanding Checks').font = _font(bold=True)
        c_tot = ws.cell(row=row, column=_E, value=out_total)
        _apply(c_tot, font=_font(bold=True), fmt='#,##0.00', fill=_fill(LIGHT_BLUE), border=DOUBLE_BTM)
        row += 2

    # ── Cleared Checks (reference) ────────────────────────────
    cleared = bank_rec_data.get('cleared_checks', [])
    if cleared:
        c = ws.cell(row=row, column=_B, value='Cleared Checks — Reference')
        c.font = _font(bold=True, size=11, color='595959')
        c.fill = _fill(LIGHT_GRAY)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        hdrs = ['Date', 'Tran #', 'Payee / Notes', 'Amount', 'Date Cleared', '']
        for ci, h in enumerate(hdrs[:5]):
            c = ws.cell(row=row, column=_B + ci, value=h)
            _apply(c, font=_font(bold=True, color='595959'), fill=_fill(LIGHT_GRAY),
                   border=THIN, align=Alignment(horizontal='center'))
        row += 1

        cleared_total = 0.0
        for i, chk in enumerate(cleared):
            payee = str(chk.get('notes', chk.get('payee', ''))).split(' - ', 1)[-1]
            amt   = float(chk.get('amount', 0))
            cleared_total += amt
            alt   = _fill('F9F9F9') if i % 2 == 1 else None
            ws.cell(row=row, column=_B, value=chk.get('date', '')).border = THIN
            ws.cell(row=row, column=_C, value=str(chk.get('tran_number', chk.get('check_number', '')))).border = THIN
            ws.cell(row=row, column=_D, value=payee).border = THIN
            c_amt = ws.cell(row=row, column=_E, value=amt)
            _apply(c_amt, fmt='#,##0.00', border=THIN)
            ws.cell(row=row, column=_F, value=chk.get('date_cleared', '')).border = THIN
            if alt:
                for col in range(_B, _B + 5):
                    ws.cell(row=row, column=col).fill = alt
            row += 1

        ws.cell(row=row, column=_D, value='Total Cleared Checks').font = _font(bold=True, color='595959')
        c_tot = ws.cell(row=row, column=_E, value=cleared_total)
        _apply(c_tot, font=_font(bold=True, color='595959'), fmt='#,##0.00',
               fill=_fill(LIGHT_GRAY), border=DOUBLE_BTM)

    ws.freeze_panes = 'B4'


# ── DACA Bank Rec tab ────────────────────────────────────────

def _write_daca_bank_rec_tab(wb, daca_bank_data: dict, gl_daca_balance: float,
                              period: str, property_name: str,
                              tab_prefix: str = ''):
    """
    Writes the DACA Bank Rec tab for KeyBank x5132 (GL account 115100).

    DACA accounts are sweep accounts — deposits are collected here and swept
    daily to PNC Operating.  There are typically no outstanding checks;
    the reconciliation is simply:

        Bank Statement Ending Balance
        = GL Account 115100 Ending Balance
        Difference (should be $0.00)

    The tab also shows:
      - Statement period and account info
      - Beginning → Ending balance from bank statement
      - Full transaction detail if available (sweeps, deposits)
    """
    COLOR_DACA = '375623'   # same dark green family as Operating rec

    _tab_name = (tab_prefix + 'Bank Rec - DACA')[:31]
    ws = wb.create_sheet(_tab_name)
    ws.sheet_properties.tabColor = COLOR_DACA

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    ending_bal = float(daca_bank_data.get('ending_balance') or 0)
    beginning_bal = float(daca_bank_data.get('beginning_balance') or 0)
    acct_num = daca_bank_data.get('account_number') or 'x5132'
    period_info = daca_bank_data.get('statement_period') or {}
    parse_error = daca_bank_data.get('_parse_error')

    row = 1
    # Header
    c = ws.cell(row=row, column=_B,
                value=f'{property_name or "Revolution Labs"} — Bank Reconciliation (DACA)')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(COLOR_DACA)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    period_str = f'{period_info.get("start", "")} — {period_info.get("end", "")}' if period_info else period
    c = ws.cell(row=row, column=_B,
                value=f'Account: KeyBank DACA (x{acct_num.lstrip("x")})  |  '
                      f'Period: {period_str}  |  GL Account: 115100  |  '
                      f'Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(COLOR_DACA)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # Column widths
    for ci, w in enumerate([22, 15, 42, 18, 6, 6]):
        ws.column_dimensions[get_column_letter(_B + ci)].width = w

    # Parse error warning
    if parse_error:
        c = ws.cell(row=row, column=_B,
                    value=f'⚠  Parser note: {parse_error} — verify balances below manually')
        c.font = _font(italic=True, color='9C0006', size=10)
        c.fill = _fill(AMBER_FILL)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 2

    # ── Reconciliation block ──────────────────────────────────
    def _daca_row(label, value, bold=False, fill_hex=None, border=THIN,
                  fmt='#,##0.00;(#,##0.00);"-"'):
        nonlocal row
        c_lbl = ws.cell(row=row, column=_B, value=label)
        c_lbl.font = _font(bold=bold)
        c_lbl.alignment = Alignment(horizontal='right')
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 3)
        c_val = ws.cell(row=row, column=_B + 4, value=value)
        _apply(c_val, font=_font(bold=bold), fmt=fmt, border=border)
        ws.merge_cells(start_row=row, start_column=_B + 4, end_row=row, end_column=_B + 5)
        if fill_hex:
            c_val.fill = _fill(fill_hex)
        row += 1

    _daca_row('Beginning Balance per Bank Statement:', beginning_bal)
    _daca_row('Ending Balance per Bank Statement:', ending_bal, bold=True,
              fill_hex=LIGHT_BLUE, border=DOUBLE_BTM)
    row += 1
    _daca_row('Balance per GL — Account 115100:', gl_daca_balance, bold=True,
              fill_hex=LIGHT_BLUE)

    # Difference
    difference = ending_bal - gl_daca_balance
    is_clean   = abs(difference) < 0.02
    var_fill   = GREEN_FILL if is_clean else RED_FILL
    var_color  = '006100' if is_clean else '9C0006'

    c_lbl = ws.cell(row=row, column=_B, value='Difference:')
    c_lbl.font = _font(bold=True, color=var_color)
    c_lbl.alignment = Alignment(horizontal='right')
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 3)
    c_val = ws.cell(row=row, column=_B + 4, value=difference)
    _apply(c_val, font=_font(bold=True, color=var_color),
           fmt='#,##0.00;(#,##0.00);"-"', fill=_fill(var_fill), border=DOUBLE_BTM)
    ws.merge_cells(start_row=row, start_column=_B + 4, end_row=row, end_column=_B + 5)
    row += 2

    if not is_clean:
        note = ws.cell(row=row, column=_B,
                       value=f'Reconciling difference of ${abs(difference):,.2f} — '
                             f'investigate before close. DACA account should sweep to zero daily.')
        note.font = _font(italic=True, color='9C0006', size=10)
        note.alignment = Alignment(wrap_text=True)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        ws.row_dimensions[row].height = 28
        row += 2

    # ── Account Note ──────────────────────────────────────────
    note2 = ws.cell(row=row, column=_B,
                    value='Note: This is a Deposit Account Control Agreement (DACA) — a sweep account. '
                          'Tenant rent deposits collect here and are swept daily to PNC Operating (x3993). '
                          'No outstanding checks are expected. Month-end balance should be minimal.')
    note2.font = _font(italic=True, size=10, color='595959')
    note2.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    ws.row_dimensions[row].height = 40
    row += 3

    # ── Raw text preview (first 1500 chars) for auditor reference ──
    raw_text = (daca_bank_data.get('_raw_text') or '').strip()
    if raw_text:
        c_hdr = ws.cell(row=row, column=_B, value='Bank Statement — Extracted Text (Reference)')
        c_hdr.font = _font(bold=True, size=11, color='595959')
        c_hdr.fill = _fill(LIGHT_GRAY)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        # Split into chunks of ~120 chars per cell so it's readable
        preview = raw_text[:3000]
        for chunk_line in preview.split('\n'):
            if not chunk_line.strip():
                continue
            c = ws.cell(row=row, column=_B, value=chunk_line)
            c.font = _font(size=9, name='Courier New')
            c.alignment = Alignment(wrap_text=False)
            ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
            row += 1
            if row > 200:   # cap to avoid massive sheets
                ws.cell(row=row, column=_B, value='... (truncated) ...').font = _font(italic=True, size=9)
                break

    ws.freeze_panes = 'B4'


# ── Convenience function for app.py ──────────────────────────

def generate(gl_result, tb_result, output_path: str,
             period: str = '', property_name: str = '',
             prepaid_ledger_active: list = None,
             bank_rec_data: dict = None,
             gl_cash_balance: float = None,
             daca_bank_data: dict = None,
             daca_gl_balance: float = None,
             je_adjustments: Optional[Dict[str, float]] = None,
             prior_workpaper_path: str = None,
             prior_period: str = None,
             berkadia_loans: list = None,
             dev_bank_rec_data: dict = None) -> str:
    """Alias for generate_bs_workpaper — called from app.py."""
    return generate_bs_workpaper(gl_result, tb_result, output_path, period,
                                  property_name, prepaid_ledger_active,
                                  bank_rec_data, gl_cash_balance,
                                  daca_bank_data, daca_gl_balance,
                                  je_adjustments,
                                  prior_workpaper_path=prior_workpaper_path,
                                  prior_period=prior_period,
                                  berkadia_loans=berkadia_loans,
                                  dev_bank_rec_data=dev_bank_rec_data)
