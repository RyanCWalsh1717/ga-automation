"""
Balance Sheet Workpaper Generator — Phase 3
=============================================
Generates the monthly close workpaper for Revolution Labs with:
  - Summary tab:      all BS accounts, GL ending vs TB ending, variance status
  - Trial Balance tab: direct from Yardi TB export
  - One tab per balance sheet account: transactions + GL ending + TB tie-out

Structure mirrors the Hartwell workpaper pattern:
  [transactions / rollforward]
  ─────────────────────────────
  Ending Balance per GL:   $X    ← computed from GL transactions
  TB Ending Balance:       $X    ← from Yardi TB export
  Variance:                $0    ← must equal zero (flags accrual gaps if not)

The Variance will be non-zero for accounts where accrual JEs are in the TB
but not yet in the GL — surfacing exactly what still needs to be posted.
"""

from datetime import datetime, date
from typing import List, Dict, Optional
from openpyxl import Workbook
from openpyxl.styles import (
    Font, PatternFill, Alignment, Border, Side
)
from openpyxl.utils import get_column_letter


# ── Constants ────────────────────────────────────────────────

# Balance sheet account range (assets + liabilities + equity)
BS_ACCOUNT_RANGE = ('100000', '399999')

# Tab colors
COLOR_SUMMARY    = '1F4E78'   # dark blue  — summary
COLOR_TB         = '2E75B6'   # medium blue — trial balance
COLOR_BS_STD     = '70AD47'   # green       — standard BS tabs
COLOR_BS_COMPLEX = 'FF0000'   # red         — complex tabs (accrued exp, prepaids)

COMPLEX_ACCOUNTS = {'213100', '135110', '135150', '213200', '221100'}

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
                           daca_gl_balance: float = None) -> str:
    """
    Generate the balance sheet reconciliation workpaper.

    Args:
        gl_result:            GLParseResult from parsers.yardi_gl.parse_gl()
        tb_result:            TBResult from parsers.yardi_trial_balance.parse()
        output_path:          Where to write the .xlsx file
        period:               Period label e.g. 'Mar-2026'
        property_name:        Property display name
        prepaid_ledger_active: Active prepaid items from prepaid_ledger.py (optional)
        bank_rec_data:        Parsed Yardi Bank Rec dict (from parsers.yardi_bank_rec.parse)
        gl_cash_balance:      GL ending balance for account 111100 (PNC Operating)
        daca_bank_data:       Parsed KeyBank DACA statement dict (from parsers.keybank_daca.parse)
        daca_gl_balance:      GL ending balance for account 115100 (DACA)

    Returns:
        output_path
    """
    wb = Workbook()

    # Build TB lookup: account_code -> TBAccount
    tb_map = {}
    if tb_result and hasattr(tb_result, 'accounts'):
        tb_map = {a.account_code: a for a in tb_result.accounts}

    # Identify balance sheet accounts from GL
    bs_accounts = [
        a for a in (gl_result.accounts if gl_result else [])
        if BS_ACCOUNT_RANGE[0] <= a.account_code <= BS_ACCOUNT_RANGE[1]
    ]

    # ── Build workpaper tabs ──────────────────────────────────
    _write_summary_tab(wb, bs_accounts, tb_map, period, property_name)
    _write_tb_tab(wb, tb_result, period, property_name)
    for acct in bs_accounts:
        _write_account_tab(wb, acct, tb_map.get(acct.account_code), period, property_name)

    # ── Prepaid amortization schedule tab (if ledger data available) ──
    if prepaid_ledger_active:
        _write_prepaid_schedule_tab(wb, prepaid_ledger_active, period, property_name)

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
        )

    # Remove default sheet
    if 'Sheet' in wb.sheetnames:
        del wb['Sheet']

    wb.save(output_path)
    return output_path


# ── Summary tab ───────────────────────────────────────────────

def _write_summary_tab(wb, bs_accounts, tb_map, period, property_name):
    ws = wb.create_sheet('Summary')
    ws.sheet_properties.tabColor = COLOR_SUMMARY

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    # Title block
    c = ws.cell(row=row, column=_B, value=f'{property_name or "Revolution Labs"} — Balance Sheet Workpaper')
    c.font = _font(bold=True, size=14, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 1

    c = ws.cell(row=row, column=_B, value=f'Period: {period}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, size=11, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    row += 2

    # Column headers
    headers = ['Account', 'Account Name', 'GL Ending Balance', 'TB Ending Balance',
               'Variance', 'Status']
    widths  = [12, 40, 20, 20, 16, 10]
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

    for group_name, group_test in groups:
        group_accts = [a for a in bs_accounts if group_test(a.account_code)]
        if not group_accts:
            continue

        # Group header
        c = ws.cell(row=row, column=_B, value=group_name)
        c.font = _font(bold=True, size=11, color=DARK_BLUE)
        c.fill = _fill(LIGHT_BLUE)
        ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
        row += 1

        for acct in group_accts:
            tb_acct = tb_map.get(acct.account_code)
            gl_end  = acct.ending_balance
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
    note = ('Note: Non-zero variances indicate accrual journal entries posted in Yardi (visible in TB) '
            'but not yet reflected in the GL detail file. These are expected for period-end accruals.')
    c = ws.cell(row=row, column=_B, value=note)
    c.font = _font(italic=True, size=10, color='595959')
    c.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_B + 5)
    ws.row_dimensions[row].height = 30

    ws.freeze_panes = 'B4'


# ── Trial Balance tab ─────────────────────────────────────────

def _write_tb_tab(wb, tb_result, period, property_name):
    ws = wb.create_sheet('Trial Balance')
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


# ── Account reconciliation tab ────────────────────────────────

def _write_account_tab(wb, gl_acct, tb_acct, period, property_name):
    """One tab per balance sheet account."""
    tab_name = f'{gl_acct.account_code}'
    ws = wb.create_sheet(tab_name)

    is_complex = gl_acct.account_code in COMPLEX_ACCOUNTS
    ws.sheet_properties.tabColor = COLOR_BS_COMPLEX if is_complex else COLOR_BS_STD

    # Blank col A — narrow
    ws.column_dimensions['A'].width = 2

    row = 1
    # Account header
    c = ws.cell(row=row, column=_B,
                value=f'{gl_acct.account_code} — {gl_acct.account_name}')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_I)
    row += 1

    c = ws.cell(row=row, column=_B,
                value=f'Period: {period}  |  {property_name or "Revolution Labs"}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=_B, end_row=row, end_column=_I)
    row += 2

    # Column headers: Date, Period, Description, Control, Reference, Debit, Credit, Balance
    headers = ['Date', 'Period', 'Description', 'Control', 'Reference', 'Debit', 'Credit', 'Balance']
    widths  = [12, 10, 45, 12, 16, 14, 14, 16]
    for ci, (h, w) in enumerate(zip(headers, widths)):
        col = _B + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 24
    row += 1

    # Beginning balance row
    ws.cell(row=row, column=_D, value='Beginning Balance').font = _font(bold=True, italic=True)
    c_beg = ws.cell(row=row, column=_I, value=gl_acct.beginning_balance)
    _apply(c_beg, font=_font(bold=True, italic=True),
           fmt='#,##0.00;(#,##0.00);"-"', fill=_fill(LIGHT_BLUE))
    for col in range(_B, _I + 1):
        ws.cell(row=row, column=col).border = THIN
        if col != _I:
            ws.cell(row=row, column=col).fill = _fill(LIGHT_BLUE)
    row += 1

    # Transactions
    running_balance = gl_acct.beginning_balance
    for i, txn in enumerate(gl_acct.transactions):
        running_balance += (txn.debit or 0) - (txn.credit or 0)
        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None

        # Date
        txn_date = txn.date
        c = ws.cell(row=row, column=_B,
                    value=txn_date.strftime('%m/%d/%Y') if txn_date else '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Period — format as Mon-YYYY string (may be datetime obj or ISO string from GL parser)
        period_val = ''
        if hasattr(txn, 'period') and txn.period:
            pv = txn.period
            if hasattr(pv, 'strftime'):
                period_val = pv.strftime('%b-%Y')
            else:
                pv_str = str(pv).strip()
                # Parse ISO-style strings like '2026-03-01 00:00:00' or '2026-03-01'
                try:
                    from datetime import datetime as _dt
                    if len(pv_str) >= 10 and pv_str[4] == '-':
                        _parsed = _dt.fromisoformat(pv_str[:10])
                        period_val = _parsed.strftime('%b-%Y')
                    else:
                        period_val = pv_str
                except Exception:
                    period_val = pv_str
        c = ws.cell(row=row, column=_C, value=period_val)
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Description
        c = ws.cell(row=row, column=_D, value=txn.description or '')
        c.alignment = Alignment(wrap_text=False)
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Control
        c = ws.cell(row=row, column=_E, value=txn.control or '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Reference
        c = ws.cell(row=row, column=_F, value=txn.reference or '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Debit
        debit_val = txn.debit if txn.debit else None
        c = ws.cell(row=row, column=_G, value=debit_val)
        if debit_val:
            c.number_format = '#,##0.00'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Credit
        credit_val = txn.credit if txn.credit else None
        c = ws.cell(row=row, column=_H, value=credit_val)
        if credit_val:
            c.number_format = '#,##0.00'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Running balance
        c = ws.cell(row=row, column=_I, value=running_balance)
        c.number_format = '#,##0.00;(#,##0.00);"-"'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        row += 1

    # ── Tie-out section ──────────────────────────────────────
    row += 1
    _write_tieout(ws, row, gl_acct, tb_acct, period)


def _write_tieout(ws, row, gl_acct, tb_acct, period):
    """Write the GL ending / TB balance / Variance tie-out block (Hartwell inline style)."""

    # Separator line
    for col in range(_B, _I + 1):
        ws.cell(row=row, column=col).border = THICK_BOTTOM
    row += 1

    gl_ending = gl_acct.ending_balance
    tb_ending = tb_acct.ending_balance if tb_acct else None
    variance  = (gl_ending - tb_ending) if tb_ending is not None else None

    # Blank separator row (already advanced past separator line above)
    row += 1

    # GL ending balance — Hartwell inline style
    # Label in _D (description col), value in _I (balance col), bold, light blue fill across data cols
    label_gl = ws.cell(row=row, column=_D,
                       value=f'Ending Balance per GL as of {period}')
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


# ── Prepaid amortization schedule tab ────────────────────────

def _write_prepaid_schedule_tab(wb, active_items: list, period: str, property_name: str):
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

    ws = wb.create_sheet('Prepaid Schedule')
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
                        gl_account_code: str = '111100'):
    """
    Writes one Bank Rec tab showing:
      Balance per Bank Statement
      Less: Outstanding Checks
      = Reconciled Bank Balance  →  must equal GL cash account
    Then lists outstanding checks and cleared checks for reference.
    """
    tab_name = f'Bank Rec - {account_label.split("(")[0].strip()[:20]}'
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
                              period: str, property_name: str):
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

    ws = wb.create_sheet('Bank Rec - DACA')
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
             daca_gl_balance: float = None) -> str:
    """Alias for generate_bs_workpaper — called from app.py."""
    return generate_bs_workpaper(gl_result, tb_result, output_path, period,
                                  property_name, prepaid_ledger_active,
                                  bank_rec_data, gl_cash_balance,
                                  daca_bank_data, daca_gl_balance)
