"""
Custom workpaper tab builders — account-by-account logic
=========================================================
Each function builds a single Excel worksheet with the standard layout:

  Standard tabs  : Date | Description | Entity | Amount
                   (TB tie-out block at bottom)

  115100 DACA    : Date | Tenant/Description | Deposits | Disbursements |
                   Adjustments | Ending Balance (running formula)

  115200/115300  : Date | Description | Entity | Amount | Running Balance

Called from bs_workpaper_generator.generate_bs_workpaper() via the
CUSTOM_BUILDERS dispatch dict.
"""
from __future__ import annotations

import re
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Shared palette (matches bs_workpaper_generator) ──────────────────────────
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

def _apply(cell, font=None, fill=None, fmt=None, border=None, align=None):
    if font:   cell.font   = font
    if fill:   cell.fill   = fill
    if fmt:    cell.number_format = fmt
    if border: cell.border = border
    if align:  cell.alignment = align


# ── Seed data ─────────────────────────────────────────────────────────────────

# 115200 — RET Escrow seed ledger
# Tuple: (date_str, description, amount)
_RET_SEED: List[tuple] = [
    ('3/25/2024',  'RET Escrow - Per statement due 4.10.24',    232339.00),
    ('5/24/2024',  'RET Escrow - Per statement due 5.10.24',    232339.00),
    ('6/25/2024',  'RET Escrow - Per statement due 6.10.24',    232339.00),
    ('7/25/2024',  'RET Escrow - Per statement due 7.10.24',    232339.00),
    ('7/25/2024',  'RET ESCROW Payment 8.1.24-2024',           -680420.84),
    ('8/25/2024',  'RET Escrow - Per statement due 8.10.24',    232339.00),
    ('9/9/2024',   'RET Escrow - Per statement due 09.09.24',   216069.98),
    ('10/7/2024',  'RET Escrow - Per statement due 10.07.24',   216069.98),
    ('10/17/2024', 'RET ESCROW Payment 10.1.24-2024',          -680420.84),
    ('11/7/2024',  'RET Escrow - Per statement due 11.07.24',   216069.98),
    ('12/9/2024',  'RET Escrow - Per statement due 12.09.24',   216069.98),
    ('1/7/2025',   'RET Escrow - Per statement due 01.07.25',   216069.98),
    ('1/16/2025',  'RET ESCROW Payment 01.1.25-Q3-2025',       -651630.69),
    ('2/7/2025',   'RET Escrow - Per statement due 02.07.25',   216069.98),
    ('3/7/2025',   'RET Escrow - Per statement due 03.07.25',   216069.98),
    ('4/7/2025',   'RET Escrow - Per statement due 04.07.25',   216069.98),
    ('4/16/2025',  'RET ESCROW Payment 01.1.25-Q4-2025',       -651630.69),
    ('5/9/2025',   'RET Escrow - Per statement due 05.09.25',   216069.98),
    ('6/9/2025',   'RET Escrow - Per statement due 06.09.25',   216069.98),
    ('7/17/2025',  'RET ESCROW Payment 01.1.25-Q1-2025',       -682671.08),
    ('7/9/2025',   'RET Escrow - Per statement due 07.09.25',   216069.98),
    ('8/7/2025',   'RET Escrow - Per statement due 08.07.25',   216069.98),
    ('9/8/2025',   'RET Escrow - Per statement due 09.07.25',   216069.98),
    ('10/7/2025',  'RET Escrow - Per statement due 10.07.25',   216069.98),
    ('10/17/2025', 'RET ESCROW Payment 10.1.25-Q2-2025',       -682671.07),
    ('11/7/2025',  'RET Escrow - Per statement due 11.07.25',   216069.98),
    ('12/8/2025',  'RET Escrow - Per statement due 12.08.25',   203295.19),
    ('1/7/2026',   'RET Escrow - Per statement due 01.07.26',   203295.19),
    ('1/16/2026',  'RET ESCROW Payment 01.1.26-Q3-2026',       -498750.81),
    ('2/9/2026',   'RET Escrow - Per statement due 02.09.26',   203295.19),
    ('3/9/2026',   'RET Escrow - Per statement due 03.09.26',   203295.19),
    ('4/7/2026',   'RET Escrow - Per statement due 04.07.26',   203295.19),
    ('4/16/2026',  'RET ESCROW Payment 04.16.26-Q4-2026',      -498750.80),
]

# 115300 — Insurance Escrow seed ledger
_INSUR_SEED: List[tuple] = [
    ('4/25/2024',  'Property Insurance per 4.10.24 stmt due',    23431.00),
    ('5/25/2024',  'Property Insurance per 5.10.24 stmt due',    23431.00),
    ('6/25/2024',  'Property Insurance per 6.10.24 stmt due',    23431.00),
    ('7/25/2024',  'Property Insurance per 7.10.24 stmt due',    23431.00),
    ('8/25/2024',  'Property Insurance per 8.10.24 stmt due',    23431.00),
    ('9/9/2024',   'Property Insurance per 9.09.24 stmt due',    19280.11),
    ('10/7/2024',  'Property Insurance per 10.07.24 stmt due',   19280.11),
    ('11/7/2024',  'Property Insurance per 11.07.24 stmt due',   19280.11),
    ('12/9/2024',  'Property Insurance per 12.09.24 stmt due',   19280.11),
    ('1/7/2025',   'Property Insurance per 01.07.25 stmt due',   19280.11),
    ('2/7/2025',   'Property Insurance per 02.07.25 stmt due',   19280.11),
    ('3/7/2025',   'Property Insurance per 03.07.25 stmt due',   19280.11),
    ('4/7/2025',   'Property Insurance per 04.07.25 stmt due',   19280.11),
    ('5/9/2025',   'Property Insurance per 05.09.25 stmt due',   19280.11),
    ('6/9/2025',   'Property Insurance per 06.09.25 stmt due',   19280.11),
    ('7/7/2025',   'Property Insurance per 07.09.25 stmt due',   19280.11),
    ('8/7/2025',   'Property Insurance per 08.09.25 stmt due',   19280.11),
    ('8/15/2025',  'Property Insurance transfer to revlab owner Operator', -66912.85),
    ('9/8/2025',   'Property Insurance per 09.08.25 stmt due',   19280.11),
    ('10/7/2025',  'Property Insurance per 10.08.25 stmt due',   19280.11),
    ('11/7/2025',  'Property Insurance per 11.08.25 stmt due',   19280.11),
]

_ENTITY = 'revlabpm'

_MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
    'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_close_period(period: str):
    """Return (year, month) ints from 'Mar-2026'.  (0,0) on failure."""
    m = re.search(r'([A-Za-z]{3})[\s\-](\d{4})', period or '')
    if not m:
        return 0, 0
    return int(m.group(2)), _MONTH_MAP.get(m.group(1).lower(), 0)

def _parse_date(d: str) -> Optional[date]:
    """Parse M/D/YYYY or M/D/YY date strings."""
    for fmt in ('%m/%d/%Y', '%m/%d/%y'):
        try:
            return datetime.strptime(d.strip(), fmt).date()
        except (ValueError, AttributeError):
            pass
    return None

def _seed_rows_for_period(seed: List[tuple], close_year: int, close_month: int) -> List[tuple]:
    """Return seed rows whose date falls on or before the last day of close month."""
    result = []
    for date_str, desc, amt in seed:
        d = _parse_date(date_str)
        if d is None:
            continue
        if (d.year, d.month) <= (close_year, close_month):
            result.append((d, desc, amt))
    # Sort chronologically
    result.sort(key=lambda r: r[0])
    return result

def _write_tab_header(ws, account_code: str, account_name: str,
                      period: str, property_name: str, ncols: int = 5):
    """Write the standard 2-row title block at the top of every account tab."""
    ws.column_dimensions['A'].width = 2
    row = 1
    c = ws.cell(row=row, column=2, value=f'{account_code}  {account_name}')
    _apply(c, font=_font(bold=True, size=13, color='FFFFFF'), fill=_fill(DARK_BLUE),
           align=Alignment(horizontal='left', vertical='center'))
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=2 + ncols - 1)
    row += 1
    c = ws.cell(row=row, column=2,
                value=f'{property_name}  |  Period: {period}  |  Prepared: {datetime.now().strftime("%m/%d/%Y")}')
    _apply(c, font=_font(italic=True, size=10, color='FFFFFF'), fill=_fill(MED_BLUE),
           align=Alignment(horizontal='left', vertical='center'))
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=2 + ncols - 1)
    return 3   # next available row

def _write_col_headers(ws, row: int, headers: List[str],
                       col_widths: List[int]) -> int:
    for ci, (h, w) in enumerate(zip(headers, col_widths)):
        col = 2 + ci
        c = ws.cell(row=row, column=col, value=h)
        _apply(c, font=_font(bold=True, color='FFFFFF'),
               fill=_fill(DARK_BLUE), border=THIN,
               align=Alignment(horizontal='center', vertical='center'))
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.row_dimensions[row].height = 18
    return row + 1

def _write_tb_tieout(ws, row: int, gl_ending: float, tb_ending: float,
                     amount_col: int = 5) -> int:
    """Write GL/TB tie-out block at the bottom of any standard tab."""
    row += 1
    # Separator
    for col in range(2, amount_col + 1):
        ws.cell(row=row, column=col).border = THICK_BOTTOM
    row += 1

    tieout = [
        ('Ending Balance per GL',   gl_ending,  DARK_BLUE, 'FFFFFF'),
        ('Ending Balance per TB',   tb_ending,  MED_BLUE,  'FFFFFF'),
        ('Variance',                round(gl_ending - tb_ending, 2), None, None),
    ]
    for label, val, fill_hex, font_color in tieout:
        c_lbl = ws.cell(row=row, column=2, value=label)
        _apply(c_lbl,
               font=_font(bold=True, color=font_color or '000000'),
               fill=_fill(fill_hex) if fill_hex else None,
               border=THIN,
               align=Alignment(horizontal='left'))
        ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=amount_col - 1)
        c_val = ws.cell(row=row, column=amount_col, value=val)
        variance = round(gl_ending - tb_ending, 2)
        if label == 'Variance':
            ok = abs(variance) < 0.02
            _apply(c_val,
                   font=_font(bold=True, color='006400' if ok else 'CC0000'),
                   fill=_fill(GREEN_FILL if ok else RED_FILL),
                   fmt='$#,##0.00', border=DOUBLE_BTM)
        else:
            _apply(c_val,
                   font=_font(bold=True, color=font_color or '000000'),
                   fill=_fill(fill_hex) if fill_hex else None,
                   fmt='$#,##0.00', border=THIN)
        row += 1
    return row


# ── Standard 4-column tab  (Date | Description | Entity | Amount) ─────────────

def _write_standard_tab(wb, tab_name: str, tab_color: str,
                        account_code: str, account_name: str,
                        period: str, property_name: str,
                        rows: List[Dict],  # each: {date, description, entity, amount}
                        gl_ending: float, tb_ending: float,
                        col_widths=(14, 48, 16, 18)):
    ws = wb.create_sheet(tab_name[:31])
    ws.sheet_properties.tabColor = tab_color

    next_row = _write_tab_header(ws, account_code, account_name,
                                 period, property_name, ncols=4)
    next_row += 1  # blank spacer
    next_row = _write_col_headers(ws, next_row,
                                  ['Date', 'Description', 'Entity', 'Amount'],
                                  list(col_widths))

    data_start = next_row
    for i, r in enumerate(rows):
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None

        d = r.get('date')
        if isinstance(d, date):
            d = d.strftime('%m/%d/%Y')

        c1 = ws.cell(row=next_row, column=2, value=d or '')
        _apply(c1, font=_font(), fill=bg, border=THIN)

        c2 = ws.cell(row=next_row, column=3, value=r.get('description', ''))
        _apply(c2, font=_font(), fill=bg, border=THIN,
               align=Alignment(wrap_text=True))

        c3 = ws.cell(row=next_row, column=4, value=r.get('entity', _ENTITY))
        _apply(c3, font=_font(), fill=bg, border=THIN)

        amt = r.get('amount', 0) or 0
        c4 = ws.cell(row=next_row, column=5, value=amt)
        _apply(c4, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))
        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=5)
    return ws


# ── 115200 — RET Escrow ───────────────────────────────────────────────────────

def build_115200_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     **_):
    close_year, close_month = _parse_close_period(period)
    seed = _seed_rows_for_period(_RET_SEED, close_year, close_month)

    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    ws = wb.create_sheet('115200 RET Escrow'[:31])
    ws.sheet_properties.tabColor = '4472C4'

    next_row = _write_tab_header(ws, '115200', 'Real Estate Tax Escrow',
                                 period, property_name, ncols=5)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Date', 'Description', 'Entity', 'Amount', 'Running Balance'],
        [14, 52, 14, 18, 18],
    )

    running = 0.0
    for i, (d, desc, amt) in enumerate(seed):
        running = round(running + amt, 2)
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None
        is_payment = amt < 0

        c1 = ws.cell(row=next_row, column=2, value=d.strftime('%m/%d/%Y'))
        _apply(c1, font=_font(), fill=bg, border=THIN)

        c2 = ws.cell(row=next_row, column=3, value=desc)
        _apply(c2, font=_font(bold=is_payment), fill=bg, border=THIN)

        c3 = ws.cell(row=next_row, column=4, value=_ENTITY)
        _apply(c3, font=_font(), fill=bg, border=THIN)

        c4 = ws.cell(row=next_row, column=5, value=amt)
        _apply(c4, font=_font(bold=is_payment,
                              color='CC0000' if is_payment else '000000'),
               fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        c5 = ws.cell(row=next_row, column=6, value=running)
        _apply(c5, font=_font(bold=True), fill=_fill(LIGHT_BLUE),
               fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=6)
    return ws


# ── 115300 — Insurance Escrow ─────────────────────────────────────────────────

def build_115300_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     **_):
    close_year, close_month = _parse_close_period(period)
    seed = _seed_rows_for_period(_INSUR_SEED, close_year, close_month)

    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    ws = wb.create_sheet('115300 Insur Escrow'[:31])
    ws.sheet_properties.tabColor = '4472C4'

    next_row = _write_tab_header(ws, '115300', 'Insurance Escrow',
                                 period, property_name, ncols=5)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Date', 'Description', 'Entity', 'Amount', 'Running Balance'],
        [14, 52, 14, 18, 18],
    )

    running = 0.0
    for i, (d, desc, amt) in enumerate(seed):
        running = round(running + amt, 2)
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None
        is_payment = amt < 0

        c1 = ws.cell(row=next_row, column=2, value=d.strftime('%m/%d/%Y'))
        _apply(c1, font=_font(), fill=bg, border=THIN)

        c2 = ws.cell(row=next_row, column=3, value=desc)
        _apply(c2, font=_font(bold=is_payment), fill=bg, border=THIN)

        c3 = ws.cell(row=next_row, column=4, value=_ENTITY)
        _apply(c3, font=_font(), fill=bg, border=THIN)

        c4 = ws.cell(row=next_row, column=5, value=amt)
        _apply(c4, font=_font(bold=is_payment,
                              color='CC0000' if is_payment else '000000'),
               fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        c5 = ws.cell(row=next_row, column=6, value=running)
        _apply(c5, font=_font(bold=True), fill=_fill(LIGHT_BLUE),
               fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=6)
    return ws


# ── 133100 — AR Other ────────────────────────────────────────────────────────

_VAGUE_PATTERNS = re.compile(
    r'^(adj|adjustment|misc|miscellaneous|other|entry|je|reclassif|reclass|'
    r'correction|see note|n/a|tbd|unknown|manual)$',
    re.IGNORECASE,
)

def _is_vague(description: str) -> bool:
    if not description:
        return True
    desc = description.strip()
    if len(desc) <= 3:
        return True
    return bool(_VAGUE_PATTERNS.match(desc))

def build_133100_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None, **_):
    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    txns = list(getattr(gl_acct, 'transactions', []) or [])

    ws = wb.create_sheet('133100 AR Other'[:31])
    ws.sheet_properties.tabColor = 'BF8F00'

    next_row = _write_tab_header(ws, '133100', 'Accounts Receivable - Other',
                                 period, property_name, ncols=5)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Date', 'Description', 'Entity', 'Amount', 'Flag'],
        [14, 52, 14, 18, 22],
    )

    for i, txn in enumerate(txns):
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None

        d = getattr(txn, 'date', None)
        date_str = d.strftime('%m/%d/%Y') if isinstance(d, date) else str(d or '')
        desc = str(getattr(txn, 'description', '') or '')
        amt  = float(getattr(txn, 'debit', 0) or 0) - float(getattr(txn, 'credit', 0) or 0)
        flag = '⚠ Review description' if _is_vague(desc) else ''

        c1 = ws.cell(row=next_row, column=2, value=date_str)
        _apply(c1, font=_font(), fill=bg, border=THIN)
        c2 = ws.cell(row=next_row, column=3, value=desc)
        _apply(c2, font=_font(), fill=bg, border=THIN, align=Alignment(wrap_text=True))
        c3 = ws.cell(row=next_row, column=4, value=_ENTITY)
        _apply(c3, font=_font(), fill=bg, border=THIN)
        c4 = ws.cell(row=next_row, column=5, value=amt)
        _apply(c4, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))
        c5 = ws.cell(row=next_row, column=6, value=flag)
        if flag:
            _apply(c5, font=_font(bold=True, color='9C0006'),
                   fill=_fill('FFC7CE'), border=THIN)
        else:
            _apply(c5, font=_font(), fill=bg, border=THIN)
        next_row += 1

    if not txns:
        c = ws.cell(row=next_row, column=2, value='No activity this period')
        _apply(c, font=_font(italic=True, color='666666'))
        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=5)
    return ws


# ── 133110 — AR Tenant Billback ───────────────────────────────────────────────

def build_133110_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     je_lines: List[Dict] = None, **_):
    """
    Builds from Pass 1 JEs tagged source='tenant_utility_billing'.
    Description reformatted to: "Accrued: [period] [tenant name]"
    """
    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    # Filter to billback JEs touching 133110
    billback_rows = []
    for je in (je_lines or []):
        if str(je.get('account_code', '')).strip() == '133110':
            amt  = float(je.get('debit', 0) or 0) - float(je.get('credit', 0) or 0)
            raw_desc = str(je.get('description', '') or '')
            vendor   = str(je.get('vendor', '') or '')
            # Reformat: "Accrued: Mar-2026 Accent Therapeutics"
            tenant = vendor or raw_desc
            desc = f'Accrued: {period} {tenant}'.strip()
            # Period of accrual = the close period; GL account from the JE
            gl_acct_code = str(je.get('account_code', '133110'))
            billback_rows.append({
                'description': desc,
                'accrual_period': period,
                'gl_account': gl_acct_code,
                'amount': amt,
            })

    ws = wb.create_sheet('133110 AR Billback'[:31])
    ws.sheet_properties.tabColor = 'BF8F00'

    next_row = _write_tab_header(ws, '133110', 'AR - Tenant Billback',
                                 period, property_name, ncols=4)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Description', 'Accrual Period', 'GL Account', 'Amount'],
        [52, 18, 14, 18],
    )

    for i, r in enumerate(billback_rows):
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None

        c1 = ws.cell(row=next_row, column=2, value=r['description'])
        _apply(c1, font=_font(), fill=bg, border=THIN, align=Alignment(wrap_text=True))
        c2 = ws.cell(row=next_row, column=3, value=r['accrual_period'])
        _apply(c2, font=_font(), fill=bg, border=THIN)
        c3 = ws.cell(row=next_row, column=4, value=r['gl_account'])
        _apply(c3, font=_font(), fill=bg, border=THIN)
        c4 = ws.cell(row=next_row, column=5, value=r['amount'])
        _apply(c4, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))
        next_row += 1

    if not billback_rows:
        c = ws.cell(row=next_row, column=2, value='No tenant billback JEs this period')
        _apply(c, font=_font(italic=True, color='666666'))
        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=5)
    return ws


# ── 213100 — Accrued Expenses ─────────────────────────────────────────────────

def build_213100_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     je_lines: List[Dict] = None, **_):
    """
    Shows only current-month accrual JEs (no auto-reversals).
    Auto-reversals are J-type controls whose credit hits 213100 (the offset).
    We show only the DR-side entries that represent the actual accrual expense.
    """
    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    # Build from Pass 1 JE lines — exclude auto-reversal source entries
    # Auto-reversals have source = 'reversal' or have a J-type GL control that
    # credits 213100 (the auto-reverse leg). We exclude any JE where the
    # account is 213100 (the liability) and include only the expense-side entries.
    accrual_rows = []
    seen_je = set()
    for je in (je_lines or []):
        src = str(je.get('source', '') or '')
        if src in ('reversal', 'auto_reversal'):
            continue
        acct = str(je.get('account_code', '') or '').strip()
        if acct == '213100':
            continue   # skip the AP credit leg — show expense legs only
        je_num = str(je.get('je_number', '') or '')
        # One row per unique JE, using the expense DR side description
        key = (je_num, acct)
        if key in seen_je:
            continue
        seen_je.add(key)

        amt  = float(je.get('debit', 0) or 0) - float(je.get('credit', 0) or 0)
        desc = str(je.get('description', '') or '')
        vendor = str(je.get('vendor', '') or '')
        why = vendor or desc or ''
        accrual_rows.append({
            'je_number':   je_num,
            'description': why,
            'account':     acct,
            'account_name': str(je.get('account_name', '') or ''),
            'amount':      amt,
        })

    ws = wb.create_sheet('213100 Accr Exp'[:31])
    ws.sheet_properties.tabColor = 'FF0000'

    next_row = _write_tab_header(ws, '213100', 'Accrued Expenses',
                                 period, property_name, ncols=5)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['JE #', 'Expense Account', 'Description / Vendor', 'Entity', 'Amount'],
        [12, 28, 48, 14, 18],
    )

    for i, r in enumerate(accrual_rows):
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None
        acct_label = f"{r['account']} {r['account_name']}".strip()

        c1 = ws.cell(row=next_row, column=2, value=r['je_number'])
        _apply(c1, font=_font(), fill=bg, border=THIN)
        c2 = ws.cell(row=next_row, column=3, value=acct_label)
        _apply(c2, font=_font(), fill=bg, border=THIN)
        c3 = ws.cell(row=next_row, column=4, value=r['description'])
        _apply(c3, font=_font(), fill=bg, border=THIN, align=Alignment(wrap_text=True))
        c4 = ws.cell(row=next_row, column=5, value=_ENTITY)
        _apply(c4, font=_font(), fill=bg, border=THIN)
        c5 = ws.cell(row=next_row, column=6, value=r['amount'])
        _apply(c5, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))
        next_row += 1

    if not accrual_rows:
        c = ws.cell(row=next_row, column=2, value='No accrual JEs this period')
        _apply(c, font=_font(italic=True, color='666666'))
        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=6)
    return ws


# ── 135150 — PPD Other (Prepaid Ledger) ──────────────────────────────────────

def build_135150_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     prepaid_ledger: List[Dict] = None, **_):
    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    ws = wb.create_sheet('135150 PPD Other'[:31])
    ws.sheet_properties.tabColor = '70AD47'

    next_row = _write_tab_header(ws, '135150', 'Prepaid - Other',
                                 period, property_name, ncols=8)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Vendor', 'Description', 'Start Date', 'End Date',
         'Monthly Amt', 'Months Amort.', 'Remaining', 'Balance'],
        [24, 32, 14, 14, 16, 16, 16, 16],
    )

    ledger = prepaid_ledger or []
    for i, item in enumerate(ledger):
        alt = i % 2 == 1
        bg = _fill(LIGHT_GRAY) if alt else None

        def _v(key): return item.get(key, '') if isinstance(item, dict) else getattr(item, key, '')

        vendor      = str(_v('vendor') or _v('description') or '')
        desc        = str(_v('description') or '')
        start       = _v('start_date') or _v('first_added_period') or ''
        end         = _v('end_date') or ''
        monthly     = float(_v('monthly_amount') or _v('monthly_amt') or 0)
        months_am   = int(_v('months_amortized') or 0)
        total_cost  = float(_v('total_cost') or _v('original_amount') or 0)
        remaining   = max(0, total_cost - monthly * months_am)
        balance     = float(_v('current_balance') or remaining)

        for ci, (col, val, fmt) in enumerate([
            (2, vendor,    None),
            (3, desc,      None),
            (4, str(start), None),
            (5, str(end),   None),
            (6, monthly,   '$#,##0.00'),
            (7, months_am, '0'),
            (8, remaining, '$#,##0.00'),
            (9, balance,   '$#,##0.00'),
        ]):
            c = ws.cell(row=next_row, column=col, value=val)
            _apply(c, font=_font(), fill=bg, border=THIN,
                   fmt=fmt, align=Alignment(wrap_text=(ci < 2)))
        next_row += 1

    if not ledger:
        c = ws.cell(row=next_row, column=2, value='No active prepaid items')
        _apply(c, font=_font(italic=True, color='666666'))
        next_row += 1

    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=9)
    return ws


# ── 115100 — DACA Restricted Cash ────────────────────────────────────────────

def _group_daca_gl_transactions(gl_acct) -> list:
    """
    Group the Yardi GL transactions for account 115100 by (date, tenant/description)
    and return a list of row dicts ready for the workpaper.

    Yardi posts many individual lines per tenant per day (charge-code breakdown
    plus APPLY clearing entries that net to zero).  We group by (date, clean_desc),
    net the debits and credits, then discard groups that net to zero (APPLY pairs).

    Classification:
      - "Sweep" in description  → Adjustments (negative, labelled "Transfer to PNC")
      - "bank fee" / "service"  → Adjustments (negative, labelled "Bank Fee")
      - net > 0                 → Deposits (tenant name as label)
      - net < 0 (other)         → Disbursements

    Returns list of dicts: {date, desc, deposits, disb, adj, sort_date}
    sorted chronologically.
    """
    from collections import defaultdict
    from datetime import date as _date

    net_by_key: Dict[tuple, float] = defaultdict(float)
    meta: Dict[tuple, dict] = {}   # key -> {date_str, sort_date, desc}

    for txn in (getattr(gl_acct, 'transactions', []) or []):
        d = getattr(txn, 'date', None)
        if isinstance(d, _date):
            date_str  = d.strftime('%m/%d/%Y')
            sort_date = d
        else:
            date_str  = str(d or '')
            sort_date = _date(1900, 1, 1)

        desc_raw   = str(getattr(txn, 'description', '') or '')
        # Strip Yardi tenant codes "(t0000011)" / "(v0000123)"
        desc_clean = re.sub(r'\s*\([tv]\d+\)\s*$', '', desc_raw).strip()

        debit  = float(getattr(txn, 'debit',  0) or 0)
        credit = float(getattr(txn, 'credit', 0) or 0)

        key = (sort_date, desc_clean)
        net_by_key[key] = round(net_by_key[key] + debit - credit, 2)
        if key not in meta:
            meta[key] = {'date_str': date_str, 'sort_date': sort_date,
                         'desc': desc_clean}

    rows = []
    for key in sorted(net_by_key.keys()):
        net  = net_by_key[key]
        info = meta[key]
        desc = info['desc']

        if abs(net) < 0.01:
            continue   # APPLY pairs net to zero — skip

        desc_lower = desc.lower()
        is_sweep   = 'sweep' in desc_lower or 'transfer to pnc' in desc_lower
        is_fee     = any(kw in desc_lower for kw in
                         ('bank fee', 'analysis service', 'service chg', 'monthly bank'))

        if is_sweep:
            rows.append({'date': info['date_str'], 'desc': 'Transfer to PNC',
                         'deposits': 0.0, 'disb': 0.0, 'adj': net,
                         'sort_date': info['sort_date']})
        elif is_fee:
            rows.append({'date': info['date_str'], 'desc': 'Bank Fee',
                         'deposits': 0.0, 'disb': 0.0, 'adj': net,
                         'sort_date': info['sort_date']})
        elif net > 0:
            rows.append({'date': info['date_str'], 'desc': desc,
                         'deposits': net, 'disb': 0.0, 'adj': 0.0,
                         'sort_date': info['sort_date']})
        else:
            rows.append({'date': info['date_str'], 'desc': desc,
                         'deposits': 0.0, 'disb': abs(net), 'adj': 0.0,
                         'sort_date': info['sort_date']})

    return rows


def build_115100_tab(wb, period: str, property_name: str,
                     gl_acct=None, tb_entry=None,
                     daca_data: Dict = None,
                     **_):
    """
    Rolling balance workpaper for DACA (KeyBank x5132 → GL 115100).

    Columns: Date | Tenant / Description | Deposits | Disbursements |
             Adjustments | Ending Balance

    GL transactions are grouped by (date, tenant name) so the workpaper
    matches the hand-prepared format:
      • One row per tenant per day (net of all charge-code sub-lines)
      • Sweeps to PNC → "Transfer to PNC" in the Adjustments column
      • Bank fees     → "Bank Fee" in the Adjustments column
      • Ending Balance is an Excel running-balance formula referencing
        the row above so users can inspect calculations in Excel.

    Reconciliation footer (if daca_data from YardiDACARec / KeyBankDACA is
    provided):
      Bank Statement Balance | GL Ending Balance | Difference
    """
    gl_ending = float(getattr(gl_acct, 'ending_balance', 0) or 0)
    gl_beg    = float(getattr(gl_acct, 'beginning_balance', 0) or 0)
    tb_ending = float(getattr(tb_entry, 'ending_balance', 0) or 0) if tb_entry else gl_ending

    ws = wb.create_sheet('115100 DACA'[:31])
    ws.sheet_properties.tabColor = '2E75B6'

    next_row = _write_tab_header(ws, '115100', 'Restricted Cash - DACA (KeyBank x5132)',
                                 period, property_name, ncols=6)
    next_row += 1
    next_row = _write_col_headers(
        ws, next_row,
        ['Date', 'Tenant / Description', 'Deposits', 'Disbursements', 'Adjustments', 'Ending Balance'],
        [14, 44, 16, 18, 16, 18],
    )

    # ── Beginning balance row ─────────────────────────────────────────────────
    c_beg = ws.cell(row=next_row, column=3, value='Balance Forward')
    _apply(c_beg, font=_font(italic=True, color='444444'), border=THIN,
           align=Alignment(horizontal='left'))
    ws.merge_cells(start_row=next_row, start_column=3,
                   end_row=next_row, end_column=6)
    c_beg_bal = ws.cell(row=next_row, column=7, value=gl_beg)
    _apply(c_beg_bal, font=_font(bold=True), fill=_fill(LIGHT_BLUE),
           fmt='$#,##0.00', border=THIN, align=Alignment(horizontal='right'))
    beg_balance_row = next_row
    next_row += 1

    # ── Build data rows from GL transactions (grouped by date × tenant) ───────
    daca_rows = _group_daca_gl_transactions(gl_acct)

    # Write rows with running Ending Balance formula
    for i, r in enumerate(daca_rows):
        alt = i % 2 == 1
        bg  = _fill(LIGHT_GRAY) if alt else None

        c1 = ws.cell(row=next_row, column=2, value=r['date'])
        _apply(c1, font=_font(), fill=bg, border=THIN)

        c2 = ws.cell(row=next_row, column=3, value=r['desc'])
        _apply(c2, font=_font(), fill=bg, border=THIN)

        c3 = ws.cell(row=next_row, column=4, value=r['deposits'] or None)
        _apply(c3, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        c4 = ws.cell(row=next_row, column=5, value=r['disb'] or None)
        _apply(c4, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        c5 = ws.cell(row=next_row, column=6, value=r['adj'] or None)
        _apply(c5, font=_font(), fill=bg, fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        # Ending balance formula: prior G + deposits(D) - disbursements(E) + adjustments(F)
        prev_ref = f'G{next_row - 1}'
        formula  = (f'={prev_ref}'
                    f'+IFERROR(D{next_row},0)'
                    f'-IFERROR(E{next_row},0)'
                    f'+IFERROR(F{next_row},0)')
        c6 = ws.cell(row=next_row, column=7, value=formula)
        _apply(c6, font=_font(bold=True), fill=_fill(LIGHT_BLUE),
               fmt='$#,##0.00', border=THIN,
               align=Alignment(horizontal='right'))

        next_row += 1

    if not daca_rows:
        c = ws.cell(row=next_row, column=2,
                    value='No DACA GL activity for this period')
        _apply(c, font=_font(italic=True, color='666666'))
        next_row += 1

    # ── Totals row ────────────────────────────────────────────────────────────
    next_row += 1
    c_tot_lbl = ws.cell(row=next_row, column=2, value='Period Totals')
    _apply(c_tot_lbl, font=_font(bold=True), border=THICK_BOTTOM)

    data_start = beg_balance_row + 1
    data_end   = next_row - 2

    for col_letter, col_idx in [('D', 4), ('E', 5), ('F', 6)]:
        if data_end >= data_start:
            tot_formula = f'=SUM({col_letter}{data_start}:{col_letter}{data_end})'
        else:
            tot_formula = 0
        ct = ws.cell(row=next_row, column=col_idx, value=tot_formula)
        _apply(ct, font=_font(bold=True), fmt='$#,##0.00', border=THICK_BOTTOM,
               align=Alignment(horizontal='right'))

    # Ending balance column just shows the last GL ending balance
    ct_end = ws.cell(row=next_row, column=7, value=gl_ending)
    _apply(ct_end, font=_font(bold=True), fill=_fill(LIGHT_BLUE),
           fmt='$#,##0.00', border=THICK_BOTTOM,
           align=Alignment(horizontal='right'))
    next_row += 2

    # ── Bank reconciliation block ─────────────────────────────────────────────
    # Uses daca_data from YardiDACARec or KeyBankDACA parser
    bank_bal = None
    rec_diff = None
    if daca_data and isinstance(daca_data, dict):
        bank_bal = (daca_data.get('reconciled_bank_balance')
                    or daca_data.get('ending_balance'))
        rec_diff = daca_data.get('reconciling_difference')

    rec_rows = [
        ('Bank Statement Balance (Reconciled)',
         bank_bal if bank_bal is not None else '— upload DACA rec to populate —',
         DARK_BLUE, 'FFFFFF'),
        ('Ending Balance per GL',
         gl_ending,
         MED_BLUE,  'FFFFFF'),
        ('Difference',
         round(gl_ending - (bank_bal or gl_ending), 2),
         None, None),
    ]
    c_rec_hdr = ws.cell(row=next_row, column=2, value='Bank Reconciliation')
    _apply(c_rec_hdr, font=_font(bold=True, size=10), border=THIN)
    ws.merge_cells(start_row=next_row, start_column=2,
                   end_row=next_row, end_column=6)
    next_row += 1

    for label, val, fill_hex, font_color in rec_rows:
        c_lbl = ws.cell(row=next_row, column=2, value=label)
        _apply(c_lbl,
               font=_font(bold=True, color=font_color or '000000'),
               fill=_fill(fill_hex) if fill_hex else None,
               border=THIN,
               align=Alignment(horizontal='left'))
        ws.merge_cells(start_row=next_row, start_column=2,
                       end_row=next_row, end_column=6)
        c_val = ws.cell(row=next_row, column=7, value=val)
        if label == 'Difference':
            ok = isinstance(val, (int, float)) and abs(val) < 0.02
            _apply(c_val,
                   font=_font(bold=True, color='006400' if ok else 'CC0000'),
                   fill=_fill(GREEN_FILL if ok else RED_FILL),
                   fmt='$#,##0.00' if isinstance(val, (int, float)) else None,
                   border=DOUBLE_BTM)
        elif isinstance(val, (int, float)):
            _apply(c_val,
                   font=_font(bold=True, color=font_color or '000000'),
                   fill=_fill(fill_hex) if fill_hex else None,
                   fmt='$#,##0.00', border=THIN)
        else:
            _apply(c_val,
                   font=_font(italic=True, color='888888'),
                   border=THIN)
        next_row += 1

    # ── TB tie-out ────────────────────────────────────────────────────────────
    next_row += 1
    _write_tb_tieout(ws, next_row, gl_ending, tb_ending, amount_col=7)
    return ws


# ── Dispatch table ────────────────────────────────────────────────────────────

CUSTOM_BUILDERS: Dict[str, Any] = {
    '115100': build_115100_tab,
    '115200': build_115200_tab,
    '115300': build_115300_tab,
    '133100': build_133100_tab,
    '133110': build_133110_tab,
    '135150': build_135150_tab,
    '213100': build_213100_tab,
}
