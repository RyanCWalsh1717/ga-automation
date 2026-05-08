"""
Management Fee Invoice Generator
=================================
Populates the GRP Excel invoice template and exports to PDF.

Template: pipeline/templates/mgmt_fee_invoice_template.xlsx
  (REv Labs Management fee invoice tempalte.xlsx — exact formatting preserved)

Only 4 cells need to be written each month:
  H7  — Invoice number  (RevLabsPM{MM}{YYYY})
  H8  — Invoice date    (last day of the close month; H9 = =H8 follows automatically)
  E15 — Period label    ('January 2026 Property Management Fee')
  F15 — Collections     (cash received — all fee amounts are Excel formulas off this)

All fee calculations live as Excel formulas in the template:
  H15 = =F15 * G15          (3.00% total fee)
  H16 = =-MAX(F16*G16,5000) (JLL deduction: greater of 1.25% or $5,000)
  H19 = =IF(SUM(H15:H18)>0, SUM(H15:H18), 0)  (Balance Due = GRP's portion)

PDF conversion priority:
  1. win32com  — Windows + Microsoft Excel installed (exact rendering)
  2. LibreOffice headless  — cross-platform if `libreoffice` available
  3. reportlab fallback  — always works, close match to template
"""

from __future__ import annotations

import io
import os
import re
import shutil
import calendar
import tempfile
from datetime import date, datetime
from typing import Optional

# ── Template path ─────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_TEMPLATE = os.path.join(_HERE, 'templates', 'mgmt_fee_invoice_template.xlsx')

# ── Constants ─────────────────────────────────────────────────────────────────
_JLL_MIN   = 5_000.0   # $5,000 minimum for JLL deduction (per template formula)
_JLL_RATE  = 0.0125
_TOTAL_RATE = 0.0300

_MONTH_MAP = {
    1: 'January', 2: 'February', 3: 'March',    4: 'April',
    5: 'May',     6: 'June',     7: 'July',      8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December',
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_period(period: str):
    """Return (year, month_int) from 'Jan-2026'. Returns (0, 0) on failure."""
    abbr = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    m = re.search(r'([A-Za-z]{3})[\s\-](\d{4})', period or '')
    if not m:
        return 0, 0
    return int(m.group(2)), abbr.get(m.group(1).lower(), 0)


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


# ── Excel population ──────────────────────────────────────────────────────────

def _populate_excel(period: str, cash_received: float, out_xlsx: str) -> None:
    """
    Copy the invoice template and fill in the 4 dynamic cells.
    Writes the populated workbook to out_xlsx.
    """
    import openpyxl

    if not os.path.exists(_TEMPLATE):
        raise FileNotFoundError(
            f'Invoice template not found: {_TEMPLATE}\n'
            'Place mgmt_fee_invoice_template.xlsx in pipeline/templates/'
        )

    year, month = _parse_period(period)
    if not year or not month:
        raise ValueError(f"Cannot parse period '{period}'")

    inv_date   = _month_end(year, month)
    inv_num    = f'RevLabsPM{month:02d}{year}'
    month_lbl  = _MONTH_MAP.get(month, str(month))
    period_lbl = f'{month_lbl} {year} Property Management Fee'

    shutil.copy2(_TEMPLATE, out_xlsx)
    wb = openpyxl.load_workbook(out_xlsx)
    ws = wb.active

    # Only 4 cells need updating — everything else is static or a formula
    ws['H7'] = inv_num
    ws['H8'] = datetime(inv_date.year, inv_date.month, inv_date.day)
    ws['E15'] = period_lbl
    ws['F15'] = round(cash_received, 2)

    wb.save(out_xlsx)


# ── PDF conversion ────────────────────────────────────────────────────────────

def _pdf_via_win32com(xlsx_path: str, pdf_path: str) -> bool:
    """Convert Excel to PDF using Microsoft Excel (Windows only)."""
    try:
        import win32com.client
        excel = win32com.client.Dispatch('Excel.Application')
        excel.Visible = False
        excel.DisplayAlerts = False
        try:
            wb = excel.Workbooks.Open(os.path.abspath(xlsx_path))
            wb.Worksheets(1).ExportAsFixedFormat(
                0,                          # xlTypePDF
                os.path.abspath(pdf_path),
                1,                          # xlQualityStandard
                True,                       # IncludeDocProperties
                False,                      # IgnorePrintAreas
            )
            wb.Close(False)
            return True
        finally:
            excel.Quit()
    except Exception:
        return False


def _pdf_via_libreoffice(xlsx_path: str, pdf_dir: str) -> bool:
    """Convert Excel to PDF using LibreOffice headless."""
    import subprocess
    try:
        result = subprocess.run(
            ['libreoffice', '--headless', '--convert-to', 'pdf',
             '--outdir', pdf_dir, os.path.abspath(xlsx_path)],
            capture_output=True, timeout=60,
        )
        return result.returncode == 0
    except Exception:
        return False


def _pdf_via_reportlab(period: str, cash_received: float, pdf_path: str) -> None:
    """
    Reportlab fallback — closely matches the template layout.
    Used when neither win32com nor LibreOffice is available.
    """
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors

    GREEN_DARK  = colors.HexColor('#2A651C')
    GREEN_LIGHT = colors.HexColor('#D4E0D2')
    BLACK       = colors.black

    GRP_NAME = 'Greatland Realty Partners LLC'
    GRP_A1   = 'One Federal Street, 28th Floor'
    GRP_A2   = 'Boston, MA 02110'
    GRP_WEB  = 'www.greatlandpartners.com'

    year, month = _parse_period(period)
    inv_date   = _month_end(year, month)
    inv_date_s = f'{inv_date.month}/{inv_date.day}/{inv_date.year}'
    inv_num    = f'RevLabsPM{month:02d}{year}'
    month_lbl  = _MONTH_MAP.get(month, str(month))

    total_fee  = round(cash_received * _TOTAL_RATE, 2)
    jll_fee    = round(max(cash_received * _JLL_RATE, _JLL_MIN), 2)
    balance    = round(max(total_fee - jll_fee, 0.0), 2)

    W, H = letter
    buf  = io.BytesIO()
    c    = rl_canvas.Canvas(buf, pagesize=letter)

    def _y(top): return H - top

    L, R = 57.0, 555.0
    DATE_X, ACT_X, DESC_X = 57.0, 135.0, 255.0
    R_COLL, R_RATE, R_AMT = 420.0, 468.0, 555.0

    # Company header
    c.setFillColor(BLACK)
    c.setFont('Times-Bold',   10); c.drawString(L, _y(42), GRP_NAME)
    c.setFont('Times-Roman',  10)
    c.drawString(L, _y(56), GRP_A1)
    c.drawString(L, _y(70), GRP_A2)
    c.drawString(L, _y(84), GRP_WEB)

    # INVOICE title
    c.setFillColor(GREEN_DARK)
    c.setFont('Times-Bold', 20)
    c.drawString(L, _y(108), 'INVOICE')
    c.setFillColor(BLACK)

    # Meta block
    META = [('INVOICE #', inv_num, 112), ('DATE', inv_date_s, 126),
            ('DUE DATE', inv_date_s, 140), ('TERMS', 'Due on receipt', 154)]
    for lbl, val, top in META:
        c.setFont('Times-Bold',  10); c.drawString(355, _y(top), lbl)
        c.setFont('Times-Roman', 10); c.drawString(460, _y(top), val)

    # Bill To
    c.setFont('Times-Bold',  10); c.drawString(L, _y(112), 'BILL TO:')
    c.setFont('Times-Roman', 10); c.drawString(L, _y(126), 'Revolution Labs Owner, LLC')

    # Table
    c.setStrokeColor(BLACK); c.setLineWidth(0.75)
    c.line(L, _y(174), R, _y(174))

    # Header fill
    c.setFillColor(GREEN_LIGHT); c.setStrokeColor(GREEN_LIGHT)
    c.rect(L, _y(196), R - L, 22, fill=1, stroke=0)
    c.setStrokeColor(BLACK); c.line(L, _y(196), R, _y(196))

    # Column headers
    c.setFont('Times-Bold', 9); c.setFillColor(BLACK)
    c.drawString(DATE_X, _y(188), 'DATE')
    c.drawString(ACT_X,  _y(188), 'ACTIVITY')
    c.drawString(DESC_X, _y(188), 'DESCRIPTION')
    c.setFillColor(GREEN_DARK)
    c.drawRightString(R_COLL, _y(188), 'COLLECTIONS')
    c.setFillColor(BLACK)
    c.drawRightString(R_RATE, _y(188), 'RATE')
    c.drawRightString(R_AMT,  _y(188), 'AMOUNT')

    def _money(v, neg=False):
        return f'{"-" if neg else ""}${abs(v):,.2f}'

    # Row 1
    c.setFont('Times-Roman', 10); c.drawString(DATE_X, _y(214), inv_date_s)
    c.setFont('Times-Bold',  10); c.drawString(ACT_X,  _y(214), 'Rev Labs Property Management Fee')
    c.setFont('Times-Roman', 10)
    c.drawString(DESC_X, _y(214), f'{month_lbl} {year} Property Management Fee')
    c.drawRightString(R_COLL, _y(214), _money(cash_received))
    c.drawRightString(R_RATE, _y(214), '3.00%')
    c.drawRightString(R_AMT,  _y(214), _money(total_fee))
    c.setLineWidth(0.5); c.line(L, _y(222), R, _y(222))

    # Row 2
    c.setFont('Times-Roman', 10)
    c.drawString(DESC_X, _y(238), 'Less JLL Portion')
    c.drawRightString(R_COLL, _y(238), _money(cash_received))
    c.drawRightString(R_RATE, _y(238), '1.25%')
    c.drawRightString(R_AMT,  _y(238), _money(jll_fee, neg=True))
    c.line(L, _y(246), R, _y(246))

    # Balance Due
    c.setLineWidth(0.75); c.line(L, _y(272), R, _y(272))
    c.setFont('Times-Bold', 10)
    c.drawString(L,     _y(284), 'BALANCE DUE')
    c.drawRightString(R_AMT, _y(284), _money(balance))

    # Payment instructions
    c.setFont('Times-Bold', 9); c.drawString(L, _y(464), 'PAYMENT INSTRUCTIONS:')
    c.drawString(L,     _y(480), 'Electronic Payment:')
    c.drawString(310.0, _y(480), 'Check Payment:')
    ach = [('Account Name:', GRP_NAME), ('Bank Name:', 'Bank of America'),
           ('Bank Account #:', '466007913255'), ('Bank Routing (ABA) #:', '026009593'),
           ('Bank Address:', '1 Federal St, Boston, MA 02110')]
    for i, (lbl, val) in enumerate(ach):
        ry = 496 + i * 13
        c.setFont('Times-Bold',  9); c.drawString(L,     _y(ry), lbl)
        c.setFont('Times-Roman', 9); c.drawString(170.0, _y(ry), val)
    chk = [('Payable to:', GRP_NAME), ('Mailing Address:', ''), ('', GRP_A1[:-16].strip()),
           ('', GRP_A1), ('', GRP_A2), ('Attention:', 'Lauren Sullivan')]
    ry = 496
    for lbl, val in chk:
        if lbl: c.setFont('Times-Bold', 9);  c.drawString(310.0, _y(ry), lbl)
        if val: c.setFont('Times-Roman', 9); c.drawString(400.0 if lbl else 310.0, _y(ry), val)
        ry += 13

    c.showPage(); c.save()
    with open(pdf_path, 'wb') as fh:
        fh.write(buf.getvalue())


# ── Public API ────────────────────────────────────────────────────────────────

def generate_invoice(
    period: str,
    cash_received: float,
    output_path: Optional[str] = None,
) -> bytes:
    """
    Generate the management fee invoice PDF by populating the Excel template.

    Conversion priority:
      1. win32com  (Windows + Excel — exact template rendering)
      2. LibreOffice headless  (cross-platform)
      3. reportlab  (always works, close match)

    Args:
        period:        Period string e.g. 'Jan-2026'.
        cash_received: Cash collected this period (management fee basis).
        output_path:   Optional path to write the PDF. Always returns bytes.

    Returns:
        PDF bytes.
    """
    if output_path is None:
        year, month = _parse_period(period)
        output_path = os.path.join(
            tempfile.gettempdir(),
            f'RevLabsPM_Invoice_{period.replace("-", "")}.pdf',
        )

    pdf_path  = output_path
    xlsx_path = pdf_path.replace('.pdf', '_populated.xlsx')

    # Step 1 — populate the Excel template
    try:
        _populate_excel(period, cash_received, xlsx_path)
    except Exception as e:
        # Template missing or openpyxl issue — skip straight to reportlab
        _pdf_via_reportlab(period, cash_received, pdf_path)
        with open(pdf_path, 'rb') as fh:
            return fh.read()

    # Step 2 — convert populated Excel → PDF
    pdf_ok = False

    # Try win32com (Windows + Excel)
    if not pdf_ok:
        pdf_ok = _pdf_via_win32com(xlsx_path, pdf_path)

    # Try LibreOffice
    if not pdf_ok:
        pdf_dir = os.path.dirname(os.path.abspath(pdf_path))
        if _pdf_via_libreoffice(xlsx_path, pdf_dir):
            # LibreOffice names the output after the input file
            lo_name = os.path.splitext(os.path.basename(xlsx_path))[0] + '.pdf'
            lo_path = os.path.join(pdf_dir, lo_name)
            if os.path.exists(lo_path) and lo_path != pdf_path:
                shutil.move(lo_path, pdf_path)
            pdf_ok = os.path.exists(pdf_path)

    # Reportlab fallback
    if not pdf_ok:
        _pdf_via_reportlab(period, cash_received, pdf_path)

    # Clean up temp xlsx
    try:
        os.remove(xlsx_path)
    except OSError:
        pass

    with open(pdf_path, 'rb') as fh:
        return fh.read()
