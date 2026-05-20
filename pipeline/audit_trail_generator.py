"""
Audit Trail Generator
=====================
Produces a multi-tab Excel workbook documenting everything the pipeline built
during a monthly close cycle.  Intended audience: auditors, Lauren (CFO review),
and GRP principals who want a complete, traceable record of automated entries.

Workbook tabs:
  1. Summary        — Pipeline run metadata, file inventory, totals, QC status
  2. JE Log         — Every journal entry generated in Pass 1 (one row per JE pair)
  3. Management Fee — Fee calculation detail with basis, rates, invoice reference
  4. Accrual Check  — Prior-month accruals vs actuals received (Item 6 reconciliation)
  5. QC Checks      — All QC check results with findings detail

Usage:
    from audit_trail_generator import generate_audit_trail

    path = generate_audit_trail(
        output_path   = '/tmp/GA_Audit_Trail.xlsx',
        period        = 'Mar-2026',
        property_name = 'Revolution Labs Owner, LLC',
        all_je_lines  = [...],          # from pass1_output_files['all_je_lines']
        fee_result    = fee_result,     # MgmtFeeResult dataclass
        qc_report     = qc_report,      # QCReport dataclass (or None)
        prior_accrual_check = [...],    # from check_prior_accrual_vs_actual()
        files_uploaded = {...},         # dict of {label: path} for processed files
    )
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from openpyxl import Workbook
from openpyxl.styles import (
    Alignment, Border, Font, PatternFill, Side,
)
from openpyxl.utils import get_column_letter


# ── Palette ──────────────────────────────────────────────────────────────────
_GRP_GREEN      = '1A5C22'    # dark GRP green — main headers
_GRP_GREEN_MID  = '2E7D32'    # mid green — section headers
_GRP_GREEN_LITE = 'E8F5E9'    # very light green — alternating rows / PASS rows
_WHITE          = 'FFFFFF'
_GREY_LITE      = 'F5F5F5'
_GREY_MED       = 'EEEEEE'
_AMBER_LITE     = 'FFF9C4'    # FLAG row background
_RED_LITE       = 'FFEBEE'    # FAIL row background
_BLACK          = '000000'

# Source → (display label, fill hex)
_SOURCE_META: Dict[str, tuple] = {
    'nexus':                  ('Nexus AP',          'BBDEFB'),   # blue
    'invoice_proration':      ('Invoice Proration',  'E1BEE7'),   # purple
    'historical':             ('Historical Pattern', 'FFF8E1'),   # amber
    'prepaid_amortization':   ('Prepaid Amort.',     'E0F2F1'),   # teal
    'prepaid_ledger':         ('Prepaid Release',    'E0F7FA'),   # cyan
    'management_fee':         ('Management Fee',     'C8E6C9'),   # green
    'management_fee_catchup': ('Mgmt Fee Catch-up',  'FFCCBC'),   # orange
    'contract_supplement':    ('One-Off Accrual',    'FFE0B2'),   # light orange
    'tenant_utility_billing': ('Tenant Utility',     'E1F5FE'),   # light cyan
    'bonus_accrual':          ('Bonus Accrual',      'FCE4EC'),   # pink
    'manual':                 ('Manual JE',          'F0F4C3'),   # yellow-green
}

_STATUS_FILL = {
    'PASS': _GRP_GREEN_LITE,
    'FLAG': _AMBER_LITE,
    'FAIL': _RED_LITE,
}


# ── Low-level styling helpers ─────────────────────────────────────────────────

def _font(bold=False, size=10, color=_BLACK, italic=False):
    return Font(name='Calibri', size=size, bold=bold, color=color, italic=italic)


def _fill(hex_color: str):
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type='solid')


def _border(style='thin'):
    s = Side(style=style)
    return Border(left=s, right=s, top=s, bottom=s)


def _thin_bottom():
    return Border(bottom=Side(style='thin'))


def _align(h='left', v='center', wrap=False):
    return Alignment(horizontal=h, vertical=v, wrap_text=wrap)


def _money(v: float) -> str:
    if v == 0:
        return '$-'
    return f'${abs(v):,.2f}' if v >= 0 else f'(${abs(v):,.2f})'


def _pct(v: float) -> str:
    return f'{v:.2%}'


def _write_header_row(ws, row: int, headers: List[str],
                       fill_hex=_GRP_GREEN, font_size=10):
    """Write a full-width bold header row with white text on dark green."""
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=col, value=h)
        c.font      = _font(bold=True, size=font_size, color=_WHITE)
        c.fill      = _fill(fill_hex)
        c.alignment = _align('center', wrap=True)
        c.border    = _border()


def _write_section_header(ws, row: int, text: str, ncols: int):
    """Merged section label on mid-green."""
    ws.merge_cells(
        start_row=row, start_column=1, end_row=row, end_column=ncols
    )
    c = ws.cell(row=row, column=1, value=text)
    c.font      = _font(bold=True, size=10, color=_WHITE)
    c.fill      = _fill(_GRP_GREEN_MID)
    c.alignment = _align('left')


def _write_kv(ws, row: int, key: str, value, key_bold=True, val_color=_BLACK,
              bg_hex=None):
    """Write a key-value pair spanning two cells."""
    ck = ws.cell(row=row, column=1, value=key)
    ck.font      = _font(bold=key_bold, size=10)
    ck.alignment = _align('left')
    if bg_hex:
        ck.fill = _fill(bg_hex)

    cv = ws.cell(row=row, column=2, value=value)
    cv.font      = _font(size=10, color=val_color)
    cv.alignment = _align('left')
    if bg_hex:
        cv.fill = _fill(bg_hex)
    return row + 1


def _freeze(ws, row=2, col=1):
    ws.freeze_panes = ws.cell(row=row, column=col)


# ── Tab 1: Summary ────────────────────────────────────────────────────────────

def _build_summary(
    ws,
    period: str,
    property_name: str,
    all_je_lines: List[dict],
    fee_result,
    qc_report,
    files_uploaded: Dict[str, str],
    run_ts: str,
    property_config=None,
):
    ws.title = '1 - Summary'
    ws.sheet_properties.tabColor = _GRP_GREEN

    ws.column_dimensions['A'].width = 34
    ws.column_dimensions['B'].width = 42
    ws.column_dimensions['C'].width = 18

    # ── Workbook title banner ────────────────────────────────────────────────
    ws.merge_cells('A1:C1')
    c = ws.cell(row=1, column=1,
                value=f'PIPELINE AUDIT TRAIL — {property_name} — {period}')
    c.font      = _font(bold=True, size=13, color=_WHITE)
    c.fill      = _fill(_GRP_GREEN)
    c.alignment = _align('center')
    ws.row_dimensions[1].height = 22

    row = 3
    # Run metadata
    _write_section_header(ws, row, 'RUN METADATA', 3)
    row += 1
    row = _write_kv(ws, row, 'Property',        property_name)
    row = _write_kv(ws, row, 'Close Period',     period)
    row = _write_kv(ws, row, 'Report Generated', run_ts)
    # C-9: derive management company from property config; fallback to generic label
    _mgmt_co = (getattr(property_config, 'management_company', '') or 'GRP')
    _prop_disp = (getattr(property_config, 'property_display_name', '') or property_name or 'GRP Properties')
    row = _write_kv(ws, row, 'Pipeline Version', f'GA Automation v2 — {_mgmt_co} / {_prop_disp}')

    row += 1
    # Files processed
    _write_section_header(ws, row, 'FILES PROCESSED', 3)
    row += 1
    _write_header_row(ws, row, ['File Type', 'Path / Filename', 'Status'],
                      fill_hex=_GRP_GREEN_MID)
    row += 1
    _FILE_LABELS = {
        'gl':                  'Yardi GL (pre-close)',
        'trial_balance':       'Trial Balance',
        'budget_comparison':   'Budget Comparison',
        'kardin_budget':       'Kardin Annual Budget',
        'nexus_accrual':       'Nexus AP Accrual Detail',
        'bank_rec':            'Yardi Bank Rec',
        'daca_bank':           'KeyBank DACA Statement',
        'receivable_detail':   'Yardi Receivable Detail',
        'ar_aging':            'Yardi AR Aging',
        'loan':                'Berkadia Loan Statement',
        'prepaid_ledger':      'Prior Month Prepaid Ledger',
        't12_statement':       '12-Month Income Statement',
    }
    alt = False
    for key, label in _FILE_LABELS.items():
        path = files_uploaded.get(key)
        has  = bool(path and os.path.exists(str(path)))
        bg   = _GRP_GREEN_LITE if alt else _WHITE
        alt  = not alt
        for col, val in enumerate([label,
                                    os.path.basename(str(path)) if has else '—',
                                    '✓ Uploaded' if has else '— Not uploaded'], 1):
            c = ws.cell(row=row, column=col, value=val)
            c.font      = _font(size=9,
                                color='2E7D32' if has else '9E9E9E')
            c.fill      = _fill(bg)
            c.alignment = _align('left')
        row += 1

    row += 1
    # JE summary
    _write_section_header(ws, row, 'PASS 1 — JOURNAL ENTRIES GENERATED', 3)
    row += 1
    _write_header_row(ws, row, ['Source Layer', 'JE Count', 'Total Debits ($)'],
                      fill_hex=_GRP_GREEN_MID)
    row += 1
    je_dr_lines = [l for l in all_je_lines if (l.get('debit') or 0) > 0]
    by_source: Dict[str, Dict] = {}
    for l in je_dr_lines:
        src = l.get('source', 'other')
        if src not in by_source:
            by_source[src] = {'count': 0, 'total': 0.0}
        by_source[src]['count'] += 1
        by_source[src]['total'] += float(l.get('debit') or 0)

    grand_count = 0
    grand_total = 0.0
    alt = False
    for src, meta in _SOURCE_META.items():
        if src not in by_source:
            continue
        label, fill_h = meta
        info = by_source[src]
        bg = fill_h if alt else _WHITE
        alt = not alt
        for col, val in enumerate([label, info['count'], _money(info['total'])], 1):
            c = ws.cell(row=row, column=col, value=val)
            c.font      = _font(size=9)
            c.fill      = _fill(bg)
            c.alignment = _align('right' if col > 1 else 'left')
        grand_count += info['count']
        grand_total += info['total']
        row += 1

    # Grand total row
    for col, val in enumerate(['TOTAL', grand_count, _money(grand_total)], 1):
        c = ws.cell(row=row, column=col, value=val)
        c.font      = _font(bold=True, size=9)
        c.fill      = _fill(_GREY_MED)
        c.alignment = _align('right' if col > 1 else 'left')
        c.border    = _thin_bottom()
    row += 2

    # Management fee summary
    if fee_result and getattr(fee_result, 'cash_received', 0) > 0:
        _write_section_header(ws, row, 'MANAGEMENT FEE', 3)
        row += 1
        row = _write_kv(ws, row, 'Cash Received Basis',
                        _money(fee_result.cash_received))
        _total_rate = getattr(fee_result, 'total_rate', None)
        _rate_str   = f' ({_total_rate:.2%})' if _total_rate else ''
        row = _write_kv(ws, row, f'Total Fee{_rate_str}',
                        _money(getattr(fee_result, 'total_fee', 0)))
        # C-9: Iterate config-driven fee lines when available; fall back to JLL/GRP labels
        _fee_lines_cfg = getattr(property_config, 'management_fees', None) or []
        if _fee_lines_cfg:
            for _fl in _fee_lines_cfg:
                _fl_label = f'{_fl.name} Fee ({_fl.rate:.2%})'
                _fl_amt   = _money(getattr(fee_result, 'cash_received', 0) * _fl.rate)
                row = _write_kv(ws, row, _fl_label, _fl_amt)
        else:
            # Legacy RevLabs JLL + GRP fallback
            row = _write_kv(ws, row, 'Less JLL Portion (1.25%)',
                            _money(getattr(fee_result, 'jll_fee', 0)))
            row = _write_kv(ws, row, 'GRP Net Fee (1.75%)',
                            _money(getattr(fee_result, 'grp_fee', 0)))
        row += 1

    # QC summary
    if qc_report:
        _write_section_header(ws, row, 'QC CHECK SUMMARY', 3)
        row += 1
        _write_header_row(ws, row, ['Check', 'Status', 'Summary'],
                          fill_hex=_GRP_GREEN_MID)
        row += 1
        for chk in qc_report.checks:
            bg = _STATUS_FILL.get(chk.status, _WHITE)
            for col, val in enumerate(
                [f'{chk.check_id}: {chk.check_name}', chk.status, chk.summary], 1
            ):
                c = ws.cell(row=row, column=col, value=val)
                c.font      = _font(size=9,
                                    bold=(col == 2),
                                    color=('2E7D32' if chk.status == 'PASS'
                                           else 'B71C1C' if chk.status == 'FAIL'
                                           else 'E65100'))
                c.fill      = _fill(bg)
                c.alignment = _align('left', wrap=(col == 3))
            ws.row_dimensions[row].height = 28
            row += 1


# ── Tab 2: JE Log ─────────────────────────────────────────────────────────────

def _build_je_log(ws, all_je_lines: List[dict]):
    ws.title = '2 - JE Log'
    ws.sheet_properties.tabColor = '1565C0'

    # Column widths
    cols_w = {
        'A': 14, 'B': 20, 'C': 13, 'D': 28, 'E': 13, 'F': 28,
        'G': 44, 'H': 28, 'I': 15, 'J': 10, 'K': 12,
    }
    for col, w in cols_w.items():
        ws.column_dimensions[col].width = w

    # Banner
    ws.merge_cells('A1:K1')
    c = ws.cell(row=1, column=1, value='PASS 1 — JOURNAL ENTRY LOG')
    c.font      = _font(bold=True, size=12, color=_WHITE)
    c.fill      = _fill('1565C0')
    c.alignment = _align('center')
    ws.row_dimensions[1].height = 20

    headers = [
        'JE #', 'Source Layer',
        'DR Account', 'DR Account Name',
        'CR Account', 'CR Account Name',
        'Description', 'Vendor / Invoice',
        'Amount ($)', 'Confidence', 'Auto-Rev.',
    ]
    _write_header_row(ws, 2, headers, fill_hex='1565C0', font_size=9)
    _freeze(ws, row=3)

    # Build JE pair dict: je_number → {dr line, cr line}
    je_pairs: Dict[str, Dict] = {}
    for l in all_je_lines:
        jn = l.get('je_number', '')
        if jn not in je_pairs:
            je_pairs[jn] = {'dr': None, 'cr': None, '_src': l.get('source', ''),
                             '_rev': l.get('auto_reverse', True)}
        if (l.get('debit') or 0) > 0:
            je_pairs[jn]['dr'] = l
        elif (l.get('credit') or 0) > 0:
            je_pairs[jn]['cr'] = l

    row = 3
    # Sort: source order then JE number
    _SRC_ORDER = list(_SOURCE_META.keys())
    def _sort_key(item):
        jn, pair = item
        src = pair['_src']
        try:
            si = _SRC_ORDER.index(src)
        except ValueError:
            si = 99
        return (si, jn)

    for jn, pair in sorted(je_pairs.items(), key=_sort_key):
        dr   = pair.get('dr') or {}
        cr   = pair.get('cr') or {}
        src  = pair['_src']
        _, fill_h = _SOURCE_META.get(src, ('Other', _GREY_LITE))

        amount   = float(dr.get('debit') or cr.get('credit') or 0)
        vendor   = str(dr.get('vendor') or '')
        inv_num  = str(dr.get('invoice_number') or '')
        vendor_s = f'{vendor}  {inv_num}'.strip('  ').strip()

        desc     = str(dr.get('description') or cr.get('description') or '')
        conf     = str(dr.get('confidence') or '')
        src_lbl  = _SOURCE_META.get(src, (src, ''))[0]
        auto_rev = '✓' if pair['_rev'] else '—'

        vals = [
            jn,
            src_lbl,
            dr.get('account_code', ''),
            dr.get('account_name', ''),
            cr.get('account_code', ''),
            cr.get('account_name', ''),
            desc,
            vendor_s,
            amount,
            conf,
            auto_rev,
        ]
        for col, val in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=val)
            c.fill      = _fill(fill_h)
            c.alignment = _align('right' if col == 9 else 'left', wrap=(col == 7))
            c.font      = _font(size=9)
            if col == 9 and isinstance(val, (int, float)):
                c.number_format = '#,##0.00'
        row += 1

    # Legend row
    row += 1
    ws.merge_cells(
        start_row=row, start_column=1, end_row=row, end_column=11
    )
    c = ws.cell(row=row, column=1, value='Color legend:  ' + '   '.join(
        f'■ {lbl}' for lbl, _ in _SOURCE_META.values()
    ))
    c.font      = _font(size=8, italic=True, color='616161')
    c.alignment = _align('left')


# ── Tab 3: Management Fee ─────────────────────────────────────────────────────

def _build_mgmt_fee(ws, fee_result, period: str, property_name: str, property_config=None):
    ws.title = '3 - Management Fee'
    ws.sheet_properties.tabColor = '2E7D32'

    ws.column_dimensions['A'].width = 36
    ws.column_dimensions['B'].width = 22
    ws.column_dimensions['C'].width = 22

    ws.merge_cells('A1:C1')
    c = ws.cell(row=1, column=1,
                value=f'MANAGEMENT FEE CALCULATION — {period}')
    c.font      = _font(bold=True, size=12, color=_WHITE)
    c.fill      = _fill(_GRP_GREEN)
    c.alignment = _align('center')
    ws.row_dimensions[1].height = 20

    if not fee_result or not getattr(fee_result, 'cash_received', 0):
        ws.cell(row=3, column=1,
                value='No management fee calculated this period.').font = _font(italic=True)
        return

    row = 3
    _write_section_header(ws, row, 'CASH RECEIVED BASIS', 3)
    row += 1
    row = _write_kv(ws, row, 'Cash Received',
                    _money(fee_result.cash_received))
    row = _write_kv(ws, row, 'Source',
                    getattr(fee_result, 'cash_source', '—'))
    prepay = getattr(fee_result, 'prepayment_excluded', 0) or 0
    if prepay:
        row = _write_kv(ws, row, 'Less Prepayments Excluded',
                        f'({_money(prepay)})')
    row += 1

    _write_section_header(ws, row, 'FEE CALCULATION', 3)
    row += 1
    _write_header_row(ws, row, ['Component', 'Rate', 'Amount'],
                      fill_hex=_GRP_GREEN_MID, font_size=9)
    row += 1

    cash = fee_result.cash_received
    total_fee = getattr(fee_result, 'total_fee', round(cash * 0.03, 2))
    jll_fee   = getattr(fee_result, 'jll_fee',   round(cash * 0.0125, 2))
    grp_fee   = getattr(fee_result, 'grp_fee',   round(cash * 0.0175, 2))

    _rows = [
        ('Total Management Fee',   '3.00%',  total_fee, False),
        ('Less JLL Portion',       '1.25%',  -jll_fee,  False),
        ('GRP Net Fee (Balance Due)', '1.75%', grp_fee, True),
    ]
    for lbl, rate, amt, bold in _rows:
        bg = _GRP_GREEN_LITE if bold else _WHITE
        for col, val in enumerate([lbl, rate, _money(amt)], 1):
            c = ws.cell(row=row, column=col, value=val)
            c.font      = _font(bold=bold, size=10)
            c.fill      = _fill(bg)
            c.alignment = _align('right' if col > 1 else 'left')
            if bold:
                c.border = _thin_bottom()
        row += 1

    row += 1
    _write_section_header(ws, row, 'JOURNAL ENTRY', 3)
    row += 1
    row = _write_kv(ws, row, 'Debit',  '637130  Admin-Management Fees')
    row = _write_kv(ws, row, 'Credit', '213100  Accrued Expenses')
    row = _write_kv(ws, row, 'Amount', _money(total_fee))
    row = _write_kv(ws, row, 'JE Reference', 'MGT-001')

    # C-9: resolve per-property strings from config instead of hardcoding RevLabs values.
    _inv_prefix   = (getattr(property_config, 'invoice_prefix', '') or 'RevLabsPM') if property_config else 'RevLabsPM'
    _bill_to      = (getattr(property_config, 'property_name', '') or 'Revolution Labs Owner, LLC') if property_config else 'Revolution Labs Owner, LLC'
    _payable_to   = (getattr(property_config, 'management_company', '') or 'Greatland Realty Partners') if property_config else 'Greatland Realty Partners'
    # Append 'LLC' only when the company name doesn't already contain a suffix.
    if _payable_to and not any(s in _payable_to for s in ('LLC', 'LP', 'Inc', 'Corp')):
        _payable_to = _payable_to + ' LLC'

    inv_num = None
    try:
        import re, calendar
        m = re.search(r'([A-Za-z]{3})[- ](\d{4})', period)
        if m:
            mos = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                   'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
            mo = mos.get(m.group(1).lower(), 0)
            yr = int(m.group(2))
            if mo:
                inv_num = f'{_inv_prefix}{mo:02d}{yr}'
    except Exception:
        pass
    if inv_num:
        row += 1
        _write_section_header(ws, row, 'INVOICE', 3)
        row += 1
        row = _write_kv(ws, row, 'Invoice Number', inv_num)
        row = _write_kv(ws, row, 'Bill To', _bill_to)
        row = _write_kv(ws, row, 'Payable To', _payable_to)
        row = _write_kv(ws, row, 'GRP Net Due', _money(grp_fee))


# ── Tab 4: Accrual vs Actual ──────────────────────────────────────────────────

def _build_accrual_check(ws, prior_accrual_check: List[dict]):
    ws.title = '4 - Accrual Check'
    ws.sheet_properties.tabColor = 'E65100'

    ws.column_dimensions['A'].width = 12
    ws.column_dimensions['B'].width = 30
    ws.column_dimensions['C'].width = 16
    ws.column_dimensions['D'].width = 16
    ws.column_dimensions['E'].width = 16
    ws.column_dimensions['F'].width = 20
    ws.column_dimensions['G'].width = 14

    ws.merge_cells('A1:G1')
    c = ws.cell(row=1, column=1,
                value='PRIOR MONTH ACCRUAL vs ACTUALS RECEIVED')
    c.font      = _font(bold=True, size=12, color=_WHITE)
    c.fill      = _fill('E65100')
    c.alignment = _align('center')
    ws.row_dimensions[1].height = 20

    row = 3
    ws.merge_cells(f'A{row}:G{row}')
    note = ws.cell(row=row, column=1,
        value=(
            'J-type auto-reversals of last month\'s pipeline accruals compared to actual '
            'invoices received this period.  NOT YET BILLED = invoice not yet received; '
            'pipeline has re-accrued.  MATCHED = actual within 5% of accrual.'
        ))
    note.font      = _font(size=9, italic=True, color='616161')
    note.alignment = _align('left', wrap=True)
    ws.row_dimensions[row].height = 28
    row += 2

    if not prior_accrual_check:
        ws.cell(row=row, column=1,
                value='No prior-month accrual auto-reversals detected in GL.').font = (
                    _font(italic=True, color='9E9E9E'))
        return

    headers = [
        'Account', 'Account Name',
        'Prior Accrual', 'Actual Billed', 'Variance',
        'Status', 'JE Reference',
    ]
    _write_header_row(ws, row, headers, fill_hex='E65100', font_size=9)
    _freeze(ws, row=row + 1)
    row += 1

    _STATUS_BG = {
        'MATCHED':        _GRP_GREEN_LITE,
        'NOT YET BILLED': _AMBER_LITE,
        'PARTIAL':        _AMBER_LITE,
        'OVER INVOICED':  _RED_LITE,
    }
    _STATUS_ICON = {
        'MATCHED':        '✅ MATCHED',
        'NOT YET BILLED': '🔄 NOT YET BILLED',
        'PARTIAL':        '⚠️ PARTIAL',
        'OVER INVOICED':  '⚠️ OVER INVOICED',
    }

    for r in prior_accrual_check:
        bg  = _STATUS_BG.get(r['status'], _WHITE)
        var = r['variance']
        var_s = (f'+${var:,.2f}' if var >= 0 else f'-${abs(var):,.2f}')
        vals = [
            r['account_code'],
            r['account_name'],
            r['reversal_amount'],
            r['actual_amount'],
            var_s,
            _STATUS_ICON.get(r['status'], r['status']),
            r['je_refs'],
        ]
        for col, val in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=val)
            c.fill      = _fill(bg)
            c.alignment = _align('right' if col in (3, 4) else 'left')
            c.font      = _font(size=9)
            if col in (3, 4) and isinstance(val, (int, float)):
                c.number_format = '#,##0.00'
        row += 1

    # Summary counts below
    row += 1
    counts = {}
    for r in prior_accrual_check:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    for status, cnt in sorted(counts.items()):
        c1 = ws.cell(row=row, column=1, value=f'{cnt}x  {status}')
        c1.font      = _font(size=9, bold=True)
        c1.fill      = _fill(_STATUS_BG.get(status, _WHITE))
        c1.alignment = _align('left')
        row += 1


# ── Tab 5: QC Checks ──────────────────────────────────────────────────────────

def _build_qc_checks(ws, qc_report):
    ws.title = '5 - QC Checks'
    ws.sheet_properties.tabColor = '6A1B9A'

    ws.column_dimensions['A'].width = 10
    ws.column_dimensions['B'].width = 36
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 55
    ws.column_dimensions['E'].width = 20
    ws.column_dimensions['F'].width = 16

    ws.merge_cells('A1:F1')
    c = ws.cell(row=1, column=1, value='QC CHECK RESULTS')
    c.font      = _font(bold=True, size=12, color=_WHITE)
    c.fill      = _fill('6A1B9A')
    c.alignment = _align('center')
    ws.row_dimensions[1].height = 20

    if not qc_report:
        ws.cell(row=3, column=1,
                value='QC checks not run this period (Pass 2 not executed).').font = (
                    _font(italic=True, color='9E9E9E'))
        return

    row = 3
    _write_header_row(ws, row,
        ['Check ID', 'Check Name', 'Status', 'Summary', 'Account / Item', 'Amount'],
        fill_hex='6A1B9A', font_size=9)
    _freeze(ws, row=row + 1)
    row += 1

    for chk in qc_report.checks:
        bg       = _STATUS_FILL.get(chk.status, _WHITE)
        txt_col  = ('2E7D32' if chk.status == 'PASS'
                    else 'B71C1C' if chk.status == 'FAIL' else 'E65100')

        if not chk.findings:
            # Single summary row
            for col, val in enumerate(
                [chk.check_id, chk.check_name, chk.status, chk.summary, '', ''], 1
            ):
                c = ws.cell(row=row, column=col, value=val)
                c.fill      = _fill(bg)
                c.font      = _font(size=9, bold=(col == 3), color=txt_col if col == 3 else _BLACK)
                c.alignment = _align('left', wrap=(col == 4))
            ws.row_dimensions[row].height = 30
            row += 1
        else:
            # First row: check header
            for col, val in enumerate(
                [chk.check_id, chk.check_name, chk.status, chk.summary, '', ''], 1
            ):
                c = ws.cell(row=row, column=col, value=val)
                c.fill      = _fill(bg)
                c.font      = _font(size=9, bold=True, color=txt_col if col == 3 else _BLACK)
                c.alignment = _align('left', wrap=(col == 4))
            ws.row_dimensions[row].height = 30
            row += 1

            # Findings detail rows
            for f in chk.findings[:25]:   # cap at 25 per check to keep readable
                diff = getattr(f, 'difference', None)
                diff_s = _money(diff) if diff is not None else ''
                acct_s = (getattr(f, 'account_name', '') or
                           getattr(f, 'account_code', '') or '')
                note_s = getattr(f, 'note', '') or ''
                for col, val in enumerate(
                    ['', '', '', note_s, acct_s, diff_s], 1
                ):
                    c = ws.cell(row=row, column=col, value=val)
                    c.fill      = _fill(_GREY_LITE)
                    c.font      = _font(size=8, italic=True)
                    c.alignment = _align('left', wrap=(col == 4))
                row += 1

        # Separator
        for col in range(1, 7):
            ws.cell(row=row, column=col).border = _thin_bottom()
        row += 1


# ── Public entry point ────────────────────────────────────────────────────────

def generate_audit_trail(
    output_path: str,
    period: str,
    property_name: str,
    all_je_lines: Optional[List[dict]] = None,
    fee_result=None,
    qc_report=None,
    prior_accrual_check: Optional[List[dict]] = None,
    files_uploaded: Optional[Dict[str, Any]] = None,
    property_config=None,
) -> str:
    """
    Generate the GA Pipeline Audit Trail workbook.

    Args:
        output_path:         Destination .xlsx path.
        period:              Close period string, e.g. 'Mar-2026'.
        property_name:       Property display name.
        all_je_lines:        All JE lines from pass1_output_files['all_je_lines'].
        fee_result:          MgmtFeeResult from management_fee.py (or None).
        qc_report:           QCReport from qc_engine.py (or None).
        prior_accrual_check: Output of check_prior_accrual_vs_actual() (or None).
        files_uploaded:      Dict {key: path} of uploaded files for inventory tab.
        property_config:     Optional PropertyConfig — when provided, per-property
                             values (invoice prefix, entity names) override the
                             RevLabs defaults hardcoded in earlier versions (C-9).

    Returns:
        output_path (for chaining).
    """
    all_je_lines        = all_je_lines        or []
    prior_accrual_check = prior_accrual_check or []
    files_uploaded      = files_uploaded      or {}

    run_ts = datetime.now().strftime('%Y-%m-%d  %H:%M')

    wb = Workbook()
    wb.remove(wb.active)   # remove default empty sheet

    ws1 = wb.create_sheet()
    _build_summary(ws1, period, property_name, all_je_lines,
                   fee_result, qc_report, files_uploaded, run_ts,
                   property_config=property_config)

    ws2 = wb.create_sheet()
    _build_je_log(ws2, all_je_lines)

    ws3 = wb.create_sheet()
    _build_mgmt_fee(ws3, fee_result, period, property_name, property_config=property_config)

    ws4 = wb.create_sheet()
    _build_accrual_check(ws4, prior_accrual_check)

    ws5 = wb.create_sheet()
    _build_qc_checks(ws5, qc_report)

    wb.save(output_path)
    return output_path
