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
                           period: str = '', property_name: str = '') -> str:
    """
    Generate the balance sheet reconciliation workpaper.

    Args:
        gl_result:     GLParseResult from parsers.yardi_gl.parse_gl()
        tb_result:     TBResult from parsers.yardi_trial_balance.parse()
        output_path:   Where to write the .xlsx file
        period:        Period label e.g. 'Mar-2026'
        property_name: Property display name

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

    # Remove default sheet
    if 'Sheet' in wb.sheetnames:
        del wb['Sheet']

    wb.save(output_path)
    return output_path


# ── Summary tab ───────────────────────────────────────────────

def _write_summary_tab(wb, bs_accounts, tb_map, period, property_name):
    ws = wb.create_sheet('Summary')
    ws.sheet_properties.tabColor = COLOR_SUMMARY

    row = 1
    # Title block
    c = ws.cell(row=row, column=1, value=f'{property_name or "Revolution Labs"} — Balance Sheet Workpaper')
    c.font = _font(bold=True, size=14, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    row += 1

    c = ws.cell(row=row, column=1, value=f'Period: {period}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, size=11, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    row += 2

    # Column headers
    headers = ['Account', 'Account Name', 'GL Ending Balance', 'TB Ending Balance',
               'Variance', 'Status']
    widths  = [12, 40, 20, 20, 16, 10]
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', vertical='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(ci)].width = widths[ci - 1]
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
        c = ws.cell(row=row, column=1, value=group_name)
        c.font = _font(bold=True, size=11, color=DARK_BLUE)
        c.fill = _fill(LIGHT_BLUE)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
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

            ws.cell(row=row, column=1, value=acct.account_code).border = THIN
            ws.cell(row=row, column=2, value=acct.account_name).border = THIN
            if row_fill:
                ws.cell(row=row, column=1).fill = row_fill
                ws.cell(row=row, column=2).fill = row_fill

            c_gl = ws.cell(row=row, column=3, value=gl_end)
            _apply(c_gl, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
            if row_fill: c_gl.fill = row_fill

            if tb_end is not None:
                c_tb = ws.cell(row=row, column=4, value=tb_end)
                _apply(c_tb, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
                if row_fill: c_tb.fill = row_fill
            else:
                c_na = ws.cell(row=row, column=4, value='N/A in TB')
                c_na.font = _font(italic=True, color='888888')
                c_na.border = THIN

            if variance is not None:
                var_fill = _fill(GREEN_FILL) if abs(variance) < 0.02 else _fill(RED_FILL)
                c_var = ws.cell(row=row, column=5, value=variance)
                _apply(c_var, fmt='#,##0.00;(#,##0.00);"-"', border=THIN, fill=var_fill)
                c_var.font = _font(bold=(abs(variance) >= 0.02))
            else:
                ws.cell(row=row, column=5, value='').border = THIN

            stat_fill = _fill(GREEN_FILL) if status == '✓' else _fill(RED_FILL)
            c_stat = ws.cell(row=row, column=6, value=status)
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
    c = ws.cell(row=row, column=1, value=status_text)
    c.font = _font(bold=True, size=12, color=status_color)
    c.fill = _fill(banner_fill)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    c.alignment = Alignment(horizontal='center')
    row += 2

    # Note about variances
    note = ('Note: Non-zero variances indicate accrual journal entries posted in Yardi (visible in TB) '
            'but not yet reflected in the GL detail file. These are expected for period-end accruals.')
    c = ws.cell(row=row, column=1, value=note)
    c.font = _font(italic=True, size=10, color='595959')
    c.alignment = Alignment(wrap_text=True)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    ws.row_dimensions[row].height = 30

    ws.freeze_panes = 'A4'


# ── Trial Balance tab ─────────────────────────────────────────

def _write_tb_tab(wb, tb_result, period, property_name):
    ws = wb.create_sheet('Trial Balance')
    ws.sheet_properties.tabColor = COLOR_TB

    row = 1
    c = ws.cell(row=row, column=1, value=f'{property_name or "Revolution Labs"} — Trial Balance')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    row += 1

    meta_text = period
    if tb_result and tb_result.metadata:
        meta_text = f'Period: {tb_result.metadata.period}  |  Book: {tb_result.metadata.book}'
    c = ws.cell(row=row, column=1, value=meta_text)
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    row += 2

    # Column headers
    headers = ['Account', 'Account Name', 'Forward Balance', 'Debit', 'Credit', 'Ending Balance']
    widths  = [12, 42, 18, 18, 18, 18]
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(ci)].width = widths[ci - 1]
    ws.row_dimensions[row].height = 28
    row += 1

    if not tb_result:
        ws.cell(row=row, column=1, value='No TB data available')
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
        c = ws.cell(row=row, column=1, value=section_name)
        c.font = _font(bold=True, color=DARK_BLUE)
        c.fill = _fill(LIGHT_BLUE)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
        row += 1

        sec_fwd = sec_dr = sec_cr = sec_end = 0.0
        for i, acct in enumerate(accts):
            alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None
            ws.cell(row=row, column=1, value=acct.account_code).border = THIN
            ws.cell(row=row, column=2, value=acct.account_name).border = THIN
            if alt_fill:
                ws.cell(row=row, column=1).fill = alt_fill
                ws.cell(row=row, column=2).fill = alt_fill

            for ci, val in enumerate([acct.forward_balance, acct.debit,
                                       acct.credit, acct.ending_balance], 3):
                c = ws.cell(row=row, column=ci, value=val)
                _apply(c, fmt='#,##0.00;(#,##0.00);"-"', border=THIN)
                if alt_fill:
                    c.fill = alt_fill

            sec_fwd += acct.forward_balance
            sec_dr  += acct.debit
            sec_cr  += acct.credit
            sec_end += acct.ending_balance
            row += 1

        # Section subtotal
        ws.cell(row=row, column=2, value=f'{section_name} TOTAL').font = _font(bold=True, color=DARK_BLUE)
        ws.cell(row=row, column=2).border = THIN
        ws.cell(row=row, column=1).border = THIN
        for ci, val in enumerate([sec_fwd, sec_dr, sec_cr, sec_end], 3):
            c = ws.cell(row=row, column=ci, value=val)
            _apply(c, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
                   border=THIN, fill=_fill(LIGHT_BLUE))
        row += 2

    # Grand total
    all_accts = tb_result.accounts
    ws.cell(row=row, column=2, value='GRAND TOTAL').font = _font(bold=True, size=12)
    ws.cell(row=row, column=2).border = DOUBLE_BTM
    ws.cell(row=row, column=1).border = DOUBLE_BTM
    for ci, val in enumerate([
        sum(a.forward_balance for a in all_accts),
        sum(a.debit for a in all_accts),
        sum(a.credit for a in all_accts),
        sum(a.ending_balance for a in all_accts),
    ], 3):
        c = ws.cell(row=row, column=ci, value=val)
        _apply(c, font=_font(bold=True, size=12),
               fmt='#,##0.00;(#,##0.00);"-"', border=DOUBLE_BTM)

    ws.freeze_panes = 'A5'


# ── Account reconciliation tab ────────────────────────────────

def _write_account_tab(wb, gl_acct, tb_acct, period, property_name):
    """One tab per balance sheet account."""
    tab_name = f'{gl_acct.account_code}'
    ws = wb.create_sheet(tab_name)

    is_complex = gl_acct.account_code in COMPLEX_ACCOUNTS
    ws.sheet_properties.tabColor = COLOR_BS_COMPLEX if is_complex else COLOR_BS_STD

    row = 1
    # Account header
    c = ws.cell(row=row, column=1,
                value=f'{gl_acct.account_code} — {gl_acct.account_name}')
    c.font = _font(bold=True, size=13, color='FFFFFF')
    c.fill = _fill(DARK_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
    row += 1

    c = ws.cell(row=row, column=1,
                value=f'Period: {period}  |  {property_name or "Revolution Labs"}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    c.font = _font(italic=True, color='FFFFFF')
    c.fill = _fill(MED_BLUE)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
    row += 2

    # Column headers
    headers = ['Date', 'Period', 'Description', 'Control', 'Reference', 'Debit', 'Credit', 'Balance']
    widths  = [12, 10, 45, 12, 16, 14, 14, 16]
    for ci, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=ci, value=h)
        _apply(c, font=_hdr_font(), fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', wrap_text=True))
        ws.column_dimensions[get_column_letter(ci)].width = widths[ci - 1]
    ws.row_dimensions[row].height = 24
    row += 1

    # Beginning balance row
    ws.cell(row=row, column=3, value='Beginning Balance').font = _font(bold=True, italic=True)
    c_beg = ws.cell(row=row, column=8, value=gl_acct.beginning_balance)
    _apply(c_beg, font=_font(bold=True, italic=True),
           fmt='#,##0.00;(#,##0.00);"-"', fill=_fill(LIGHT_BLUE))
    for ci in range(1, 9):
        ws.cell(row=row, column=ci).border = THIN
        if ci != 8:
            ws.cell(row=row, column=ci).fill = _fill(LIGHT_BLUE)
    row += 1

    # Transactions
    running_balance = gl_acct.beginning_balance
    for i, txn in enumerate(gl_acct.transactions):
        running_balance += (txn.debit or 0) - (txn.credit or 0)
        alt_fill = _fill(LIGHT_GRAY) if i % 2 == 1 else None

        # Date
        txn_date = txn.date
        c = ws.cell(row=row, column=1,
                    value=txn_date.strftime('%m/%d/%Y') if txn_date else '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Period
        period_val = ''
        if hasattr(txn, 'period') and txn.period:
            period_val = txn.period
        c = ws.cell(row=row, column=2, value=period_val)
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Description
        c = ws.cell(row=row, column=3, value=txn.description or '')
        c.alignment = Alignment(wrap_text=False)
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Control
        c = ws.cell(row=row, column=4, value=txn.control or '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Reference
        c = ws.cell(row=row, column=5, value=txn.reference or '')
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Debit
        debit_val = txn.debit if txn.debit else None
        c = ws.cell(row=row, column=6, value=debit_val)
        if debit_val:
            c.number_format = '#,##0.00'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Credit
        credit_val = txn.credit if txn.credit else None
        c = ws.cell(row=row, column=7, value=credit_val)
        if credit_val:
            c.number_format = '#,##0.00'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        # Running balance
        c = ws.cell(row=row, column=8, value=running_balance)
        c.number_format = '#,##0.00;(#,##0.00);"-"'
        if alt_fill: c.fill = alt_fill
        c.border = THIN

        row += 1

    # ── Tie-out section ──────────────────────────────────────
    row += 1
    _write_tieout(ws, row, gl_acct, tb_acct)


def _write_tieout(ws, row, gl_acct, tb_acct):
    """Write the GL ending / TB balance / Variance tie-out block."""

    # Separator line
    for ci in range(1, 9):
        ws.cell(row=row, column=ci).border = THICK_BOTTOM
    row += 1

    gl_ending = gl_acct.ending_balance
    tb_ending = tb_acct.ending_balance if tb_acct else None
    variance  = (gl_ending - tb_ending) if tb_ending is not None else None

    # GL ending balance
    label = ws.cell(row=row, column=6, value='Ending Balance per GL:')
    label.font = _font(bold=True)
    label.alignment = Alignment(horizontal='right')
    c = ws.cell(row=row, column=8, value=gl_ending)
    _apply(c, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
           fill=_fill(LIGHT_BLUE), border=THICK_BOTTOM)
    ws.merge_cells(start_row=row, start_column=6, end_row=row, end_column=7)
    row += 1

    # TB ending balance
    label = ws.cell(row=row, column=6, value='TB Ending Balance:')
    label.font = _font(bold=True)
    label.alignment = Alignment(horizontal='right')
    ws.merge_cells(start_row=row, start_column=6, end_row=row, end_column=7)

    if tb_ending is not None:
        c = ws.cell(row=row, column=8, value=tb_ending)
        _apply(c, font=_font(bold=True), fmt='#,##0.00;(#,##0.00);"-"',
               fill=_fill(LIGHT_BLUE), border=THIN)
    else:
        c = ws.cell(row=row, column=8, value='Not in TB')
        c.font = _font(italic=True, color='888888')
    row += 1

    # Variance
    label = ws.cell(row=row, column=6, value='Variance:')
    label.font = _font(bold=True)
    label.alignment = Alignment(horizontal='right')
    ws.merge_cells(start_row=row, start_column=6, end_row=row, end_column=7)

    if variance is not None:
        is_zero = abs(variance) < 0.02
        var_fill = _fill(GREEN_FILL) if is_zero else _fill(RED_FILL)
        var_color = '006100' if is_zero else '9C0006'
        c = ws.cell(row=row, column=8, value=variance)
        _apply(c, font=_font(bold=True, color=var_color),
               fmt='#,##0.00;(#,##0.00);"-"', fill=var_fill, border=DOUBLE_BTM)

        if not is_zero:
            note_row = row + 2
            note = ws.cell(row=note_row, column=1,
                           value=f'Variance of ${abs(variance):,.2f} — likely accrual entries in TB not yet in GL. '
                                 f'Review accrual JEs posted for this account.')
            note.font = _font(italic=True, color='9C0006', size=10)
            note.alignment = Alignment(wrap_text=True)
            ws.merge_cells(start_row=note_row, start_column=1,
                           end_row=note_row, end_column=8)
            ws.row_dimensions[note_row].height = 28
    else:
        ws.cell(row=row, column=8, value='').border = DOUBLE_BTM


# ── Convenience function for app.py ──────────────────────────

def generate(gl_result, tb_result, output_path: str,
             period: str = '', property_name: str = '') -> str:
    """Alias for generate_bs_workpaper — called from app.py."""
    return generate_bs_workpaper(gl_result, tb_result, output_path, period, property_name)
