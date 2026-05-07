"""
Management Fee Invoice Generator
=================================
Produces a PDF invoice for the monthly property management fee,
matching the RevLabsPM reference invoice exactly.

Reference invoice extracted via pdfplumber (page 612 × 792 pt = US Letter):
  Font:     TimesNewRoman Bold (Times-Bold) and Regular (Times-Roman)
  Sizes:    6.72 pt body, 6.0 pt column headers, 13.44 pt INVOICE title
  Green:    RGB(0.164, 0.395, 0.109)  — headers and INVOICE title
  Layout:   text on white, NO colored background blocks

All y-coordinates below are pdfplumber convention (from top of page).
Converted to reportlab (from bottom) via:  rl_y = PAGE_H - pdf_y

Invoice # format:  RevLabsPM{MM}{YYYY}   e.g. RevLabsPM022026
"""

from __future__ import annotations

import io
import re
import calendar
from datetime import date
from typing import Optional

from reportlab.pdfgen import canvas as rl_canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors

# ── Palette ──────────────────────────────────────────────────────────────────
_GREEN = colors.Color(0.164, 0.395, 0.109)   # dark green (table headers / INVOICE title)
_BLACK = colors.black

# ── GRP / property constants ──────────────────────────────────────────────────
_GRP_NAME    = 'Greatland Realty Partners LLC'
_GRP_ADDR1   = 'One Federal Street, 28th Floor'
_GRP_ADDR2   = 'Boston, MA 02110'
_GRP_WEB     = 'www.greatlandpartners.com'

_BILL_TO     = 'Revolution Labs Owner, LLC'
_TERMS       = 'Due on receipt'

_ACH_BANK    = 'Bank of America'
_ACH_ACCT    = '466007913255'
_ACH_RTG     = '026009593'
_ACH_ADDR    = '1 Federal St, Boston, MA 02110'

_CHK_PAYABLE = 'Greatland Realty Partners LLC'
_CHK_ADDR1   = 'Greatland Realty Partners'
_CHK_ADDR2   = '1 Federal Street, 28th Floor'
_CHK_ADDR3   = 'Boston, MA 02110'
_CHK_ATTN    = 'Lauren Sullivan'

# Fee rates
_TOTAL_RATE  = 0.0300   # 3.00%
_JLL_RATE    = 0.0125   # 1.25%
_GRP_RATE    = 0.0175   # 1.75%

_MONTH_MAP = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May',     6: 'June',     7: 'July',   8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December',
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _parse_period(period: str):
    """Return (year, month_int) from a string like 'Feb-2026'.  (0, 0) on failure."""
    abbr = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    m = re.search(r'([A-Za-z]{3})[\s\-](\d{4})', period or '')
    if not m:
        return 0, 0
    return int(m.group(2)), abbr.get(m.group(1).lower(), 0)


def _money(v: float, neg: bool = False) -> str:
    sign = '-' if neg else ''
    return f'{sign}${abs(v):,.2f}'


# ── Public API ────────────────────────────────────────────────────────────────

def generate_invoice(
    period: str,
    cash_received: float,
    output_path: Optional[str] = None,
) -> bytes:
    """
    Generate the GRP management fee invoice PDF, matching the RevLabsPM reference.

    Args:
        period:        Accounting period string, e.g. 'Feb-2026'.
        cash_received: Management fee basis (cash collected this period).
        output_path:   Optional path to write PDF file.  Returns bytes always.

    Returns:
        PDF bytes.
    """
    year, month = _parse_period(period)
    if not year or not month:
        raise ValueError(f"Cannot parse period '{period}'")

    inv_date   = _month_end(year, month)
    inv_date_s = f'{inv_date.month}/{inv_date.day}/{inv_date.year}'
    inv_num    = f'RevLabsPM{month:02d}{year}'
    month_lbl  = _MONTH_MAP.get(month, str(month))

    total_fee = round(cash_received * _TOTAL_RATE, 2)
    jll_fee   = round(max(cash_received * _JLL_RATE, 5_000.0), 2)  # greater of 1.25% or $5,000 minimum
    balance   = round(max(total_fee - jll_fee, 0.0), 2)

    buf = io.BytesIO()
    c   = rl_canvas.Canvas(buf, pagesize=letter)
    W, H = letter  # 612 × 792 pt

    # Convert pdfplumber y-from-top to reportlab y-from-bottom (baseline approx)
    def y(pdf_y: float) -> float:
        return H - pdf_y

    # ── SECTION 1: Company header (top-left) ────────────────────────────────
    # pdfplumber positions: company name y=64.2, addresses y=72.6/81.1/89.7
    c.setFillColor(_BLACK)
    c.setFont('Times-Bold', 6.72)
    c.drawString(56.9, y(64.2), _GRP_NAME)

    c.setFont('Times-Roman', 6.72)
    c.drawString(56.9, y(72.6), _GRP_ADDR1)
    c.drawString(56.9, y(81.1), _GRP_ADDR2)
    c.drawString(56.9, y(89.7), _GRP_WEB)

    # ── SECTION 2: "INVOICE" title (large, green, left-aligned) ─────────────
    # pdfplumber: x=57.7, y=104.4, size=13.44pt
    c.setFillColor(_GREEN)
    c.setFont('Times-Bold', 13.44)
    c.drawString(57.7, y(104.4), 'INVOICE')
    c.setFillColor(_BLACK)

    # ── SECTION 3: Invoice meta block (right side) ───────────────────────────
    # Labels at x=382.1, values at x=468.5
    # pdfplumber y rows: 120.6, 130.6, 139.1, 147.6
    _meta = [
        ('INVOICE #', inv_num,    120.6),
        ('DATE',      inv_date_s, 130.6),
        ('DUE DATE',  inv_date_s, 139.1),
        ('TERMS',     _TERMS,     147.6),
    ]
    for lbl, val, pdf_y in _meta:
        c.setFont('Times-Bold', 6.72)
        c.drawString(382.1, y(pdf_y), lbl)
        c.setFont('Times-Roman', 6.72)
        c.drawString(468.5, y(pdf_y), val)

    # ── SECTION 4: Bill To (left side, same vertical band as meta) ───────────
    # "BILL TO:" at y=122.1; bill-to name one line below
    c.setFont('Times-Bold', 6.72)
    c.drawString(56.9, y(122.1), 'BILL TO:')
    c.setFont('Times-Roman', 6.72)
    c.drawString(56.9, y(131.0), _BILL_TO)

    # ── SECTION 5: Line-item table ───────────────────────────────────────────
    # Column x anchors (pdfplumber):
    #   DATE hdr:        x=85.1   (left-align)
    #   ACTIVITY hdr:    x=146.5  (left-align)
    #   DESCRIPTION hdr: x=240.0  (left-align)
    #   COLLECTIONS hdr: x=375.6  (right-align to col right edge ≈432)
    #   RATE hdr:        x=434.6  (right-align to col right edge ≈472)
    #   AMOUNT hdr:      x=473.5  (right-align to col right edge ≈555)
    #
    # Data row 1: date at x=79.3, activity bold at x=136.4, desc at x=240.0
    # Data row 2: activity bold at x=136.4, desc at x=240.0

    TBL_L    = 57.0    # table left edge (horizontal rule)
    TBL_R    = 555.0   # table right edge
    R_COLL   = 432.0   # right edge of COLLECTIONS column
    R_RATE   = 472.0   # right edge of RATE column
    R_AMT    = 555.0   # right edge of AMOUNT column

    HDR_Y    = 181.7   # header baseline (pdfplumber)
    ROW1_Y   = 193.1   # row 1 baseline
    ROW2_Y   = 201.6   # row 2 baseline

    # Horizontal rules (estimated from extracted row spacing)
    c.setStrokeColor(_BLACK)
    c.setLineWidth(0.5)
    c.line(TBL_L, y(175.5), TBL_R, y(175.5))   # above header
    c.line(TBL_L, y(187.5), TBL_R, y(187.5))   # below header
    c.line(TBL_L, y(197.0), TBL_R, y(197.0))   # below row 1
    c.line(TBL_L, y(206.0), TBL_R, y(206.0))   # below row 2

    # Column headers — 6.0pt bold, green
    c.setFont('Times-Bold', 6.0)
    c.setFillColor(_GREEN)
    c.drawString(     85.1, y(HDR_Y), 'DATE')
    c.drawString(    146.5, y(HDR_Y), 'ACTIVITY')
    c.drawString(    240.0, y(HDR_Y), 'DESCRIPTION')
    c.drawRightString(R_COLL, y(HDR_Y), 'COLLECTIONS')
    c.drawRightString(R_RATE, y(HDR_Y), 'RATE')
    c.drawRightString(R_AMT,  y(HDR_Y), 'AMOUNT')
    c.setFillColor(_BLACK)

    # Row 1: total management fee
    c.setFont('Times-Roman', 6.72)
    c.drawString(79.3, y(ROW1_Y), inv_date_s)
    c.setFont('Times-Bold', 6.72)
    c.drawString(136.4, y(ROW1_Y), 'Rev Labs Property Management Fee')
    c.setFont('Times-Roman', 6.72)
    c.drawString(240.0, y(ROW1_Y), f'{month_lbl} {year} Property Management Fee')
    c.drawRightString(R_COLL, y(ROW1_Y), _money(cash_received))
    c.drawRightString(R_RATE, y(ROW1_Y), '3.00%')
    c.drawRightString(R_AMT,  y(ROW1_Y), _money(total_fee))

    # Row 2: less JLL portion (no bold activity label — description only)
    c.setFont('Times-Roman', 6.72)
    c.drawString(240.0, y(ROW2_Y), 'Less JLL Portion')
    c.drawRightString(R_COLL, y(ROW2_Y), _money(cash_received))
    c.drawRightString(R_RATE, y(ROW2_Y), '1.25%')
    c.drawRightString(R_AMT,  y(ROW2_Y), _money(jll_fee, neg=True))

    # ── SECTION 6: Balance Due ───────────────────────────────────────────────
    # pdfplumber: "BALANCE DUE" at y=233.7, bold amount at y=233.8
    BAL_Y = 233.7
    c.line(TBL_L, y(227.0), TBL_R, y(227.0))   # rule above balance line

    c.setFont('Times-Bold', 6.72)
    c.drawString(    TBL_L,  y(BAL_Y), 'BALANCE DUE')
    c.drawRightString(R_AMT, y(BAL_Y), _money(balance))

    # ── SECTION 7: Payment Instructions ─────────────────────────────────────
    # "PAYMENT INSTRUCTIONS:" at pdfplumber y=489.3 (bold)
    # Electronic Payment left col x=56.9; Check Payment right col x=250.1
    # Data rows y=506.4 through ~574.5 (≈6-7 rows, ~10pt leading)
    PAY_HDR_Y  = 489.3
    PAY_SECT_Y = 499.0    # "Electronic Payment:" / "Check Payment:" headers
    PAY_ROW_Y0 = 510.0    # first data row baseline
    PAY_LEAD   = 9.5      # row leading (pt)

    # Section heading
    c.setFont('Times-Bold', 6.72)
    c.drawString(56.9, y(PAY_HDR_Y), 'PAYMENT INSTRUCTIONS:')

    # Sub-section headings
    c.drawString( 56.9, y(PAY_SECT_Y), 'Electronic Payment:')
    c.drawString(250.1, y(PAY_SECT_Y), 'Check Payment:')

    # ACH detail rows (label bold, value regular)
    _ach_rows = [
        ('Account Name:',        'Greatland Realty Partners LLC'),
        ('Bank Name:',           _ACH_BANK),
        ('Bank Account #:',      _ACH_ACCT),
        ('Bank Routing (ABA) #:', _ACH_RTG),
        ('Bank Address:',        _ACH_ADDR),
    ]
    ACH_LBL_X  = 56.9
    ACH_VAL_X  = 160.0   # value indent (right of longest label)
    for i, (lbl, val) in enumerate(_ach_rows):
        row_y = PAY_ROW_Y0 + i * PAY_LEAD
        c.setFont('Times-Bold', 6.72)
        c.drawString(ACH_LBL_X, y(row_y), lbl)
        c.setFont('Times-Roman', 6.72)
        c.drawString(ACH_VAL_X, y(row_y), val)

    # Check detail rows
    _chk_rows = [
        ('Payable to:',       _CHK_PAYABLE),
        ('Mailing Address:',  ''),
        ('',                  _CHK_ADDR1),
        ('',                  _CHK_ADDR2),
        ('',                  _CHK_ADDR3),
        ('Attention:',        _CHK_ATTN),
    ]
    CHK_LBL_X = 250.1
    CHK_VAL_X = 340.0
    row_y = PAY_ROW_Y0
    for lbl, val in _chk_rows:
        if lbl:
            c.setFont('Times-Bold', 6.72)
            c.drawString(CHK_LBL_X, y(row_y), lbl)
        if val:
            c.setFont('Times-Roman', 6.72)
            c.drawString(CHK_VAL_X if lbl else CHK_LBL_X, y(row_y), val)
        row_y += PAY_LEAD

    # ── Finalise ─────────────────────────────────────────────────────────────
    c.showPage()
    c.save()
    pdf_bytes = buf.getvalue()

    if output_path:
        with open(output_path, 'wb') as fh:
            fh.write(pdf_bytes)

    return pdf_bytes
