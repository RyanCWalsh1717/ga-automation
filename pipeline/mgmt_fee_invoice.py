"""
Management Fee Invoice Generator
=================================
Produces a GRP-branded PDF invoice for the monthly property management fee.

Layout mirrors the reference invoice (RevLabsPM022026):
  - Header:  GRP name/address block (left) + "INVOICE" label (right)
  - Bill To: Revolution Labs Owner, LLC
  - Meta box: Invoice #, Date, Due Date, Terms
  - Line items: Total fee (3.00%) and Less JLL Portion (1.25%)
  - Balance Due: GRP net (1.75%)
  - Payment instructions (ACH + Check)

Invoice # format:  RevLabsPM{MM}{YYYY}   e.g. RevLabsPM022026
"""

from __future__ import annotations

import io
import calendar
from datetime import date
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable,
)
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER

# ── GRP Brand colours ────────────────────────────────────────────────────────
_NAVY   = colors.HexColor('#1F3864')
_BLUE   = colors.HexColor('#2E5496')
_GRAY   = colors.HexColor('#555555')
_LGRAY  = colors.HexColor('#F2F2F2')
_BLACK  = colors.black
_WHITE  = colors.white

# ── Static GRP / property constants ─────────────────────────────────────────
_GRP_NAME    = 'Greatland Realty Partners LLC'
_GRP_ADDR1   = 'One Federal Street, 28th Floor'
_GRP_ADDR2   = 'Boston, MA 02110'
_GRP_WEB     = 'www.greatlandpartners.com'

_BILL_TO     = 'Revolution Labs Owner, LLC'
_PROP_LABEL  = 'Rev Labs Property Management Fee'
_TERMS       = 'Due on receipt'

# Payment instructions
_ACH_NAME    = 'Greatland Realty Partners LLC'
_ACH_BANK    = 'Bank of America'
_ACH_ACCT    = '466007913255'
_ACH_RTG     = '026009593'
_ACH_ADDR    = '1 Federal St\nBoston, MA 02110'

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
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December',
}


def _month_end(year: int, month: int) -> date:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, last)


def _parse_period(period: str):
    """
    Parse a period string like 'Feb-2026' → (2026, 2).
    Returns (0, 0) on failure.
    """
    month_abbr = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
        'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    import re
    m = re.search(r'([A-Za-z]{3})[\s\-](\d{4})', period or '')
    if not m:
        return 0, 0
    mon = month_abbr.get(m.group(1).lower(), 0)
    yr  = int(m.group(2))
    return yr, mon


# ── Styles ───────────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    s = {}

    s['company'] = ParagraphStyle(
        'company', parent=base['Normal'],
        fontSize=13, fontName='Helvetica-Bold',
        textColor=_NAVY, spaceAfter=2,
    )
    s['addr'] = ParagraphStyle(
        'addr', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=_GRAY, spaceAfter=1, leading=13,
    )
    s['web'] = ParagraphStyle(
        'web', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=_BLUE, spaceAfter=0,
    )
    s['invoice_title'] = ParagraphStyle(
        'invoice_title', parent=base['Normal'],
        fontSize=28, fontName='Helvetica-Bold',
        textColor=_NAVY, alignment=TA_RIGHT,
    )
    s['label'] = ParagraphStyle(
        'label', parent=base['Normal'],
        fontSize=8, fontName='Helvetica-Bold',
        textColor=_GRAY, spaceAfter=2, leading=11,
    )
    s['value'] = ParagraphStyle(
        'value', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=_BLACK, leading=13,
    )
    s['bill_label'] = ParagraphStyle(
        'bill_label', parent=base['Normal'],
        fontSize=8, fontName='Helvetica-Bold',
        textColor=_WHITE,
    )
    s['bill_value'] = ParagraphStyle(
        'bill_value', parent=base['Normal'],
        fontSize=10, fontName='Helvetica',
        textColor=_BLACK, spaceAfter=2,
    )
    s['tbl_hdr'] = ParagraphStyle(
        'tbl_hdr', parent=base['Normal'],
        fontSize=9, fontName='Helvetica-Bold',
        textColor=_WHITE,
    )
    s['tbl_body'] = ParagraphStyle(
        'tbl_body', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=_BLACK, leading=13,
    )
    s['tbl_body_r'] = ParagraphStyle(
        'tbl_body_r', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=_BLACK, alignment=TA_RIGHT, leading=13,
    )
    s['tbl_body_neg'] = ParagraphStyle(
        'tbl_body_neg', parent=base['Normal'],
        fontSize=9, fontName='Helvetica',
        textColor=colors.HexColor('#C00000'),
        alignment=TA_RIGHT, leading=13,
    )
    s['bal_label'] = ParagraphStyle(
        'bal_label', parent=base['Normal'],
        fontSize=11, fontName='Helvetica-Bold',
        textColor=_NAVY, alignment=TA_RIGHT,
    )
    s['bal_value'] = ParagraphStyle(
        'bal_value', parent=base['Normal'],
        fontSize=11, fontName='Helvetica-Bold',
        textColor=_NAVY, alignment=TA_RIGHT,
    )
    s['pay_hdr'] = ParagraphStyle(
        'pay_hdr', parent=base['Normal'],
        fontSize=9, fontName='Helvetica-Bold',
        textColor=_WHITE,
    )
    s['pay_body'] = ParagraphStyle(
        'pay_body', parent=base['Normal'],
        fontSize=8.5, fontName='Helvetica',
        textColor=_BLACK, leading=13,
    )
    s['pay_label'] = ParagraphStyle(
        'pay_label', parent=base['Normal'],
        fontSize=8, fontName='Helvetica-Bold',
        textColor=_GRAY, spaceAfter=1,
    )

    return s


# ── Public API ────────────────────────────────────────────────────────────────

def generate_invoice(
    period: str,
    cash_received: float,
    output_path: Optional[str] = None,
) -> bytes:
    """
    Generate the GRP management fee invoice as a PDF.

    Args:
        period:        Accounting period string, e.g. 'Feb-2026'.
        cash_received: The management fee basis (cash collected this period).
        output_path:   Optional file path to save PDF.  If None, returns bytes only.

    Returns:
        PDF bytes.
    """
    year, month = _parse_period(period)
    if not year or not month:
        raise ValueError(f"Cannot parse period '{period}'")

    inv_date   = _month_end(year, month)
    inv_date_s = f'{inv_date.month}/{inv_date.day}/{inv_date.year}'

    inv_num = f'RevLabsPM{month:02d}{year}'
    month_label = _MONTH_MAP.get(month, str(month))

    # Amounts
    total_fee = round(cash_received * _TOTAL_RATE, 2)
    jll_fee   = round(cash_received * _JLL_RATE, 2)
    balance   = round(cash_received * _GRP_RATE, 2)

    def _money(v: float, neg: bool = False) -> str:
        sign = '-' if neg else ''
        return f'{sign}${abs(v):,.2f}'

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.75 * inch,
    )

    S = _styles()
    W = letter[0] - 1.5 * inch   # usable width

    story = []

    # ── SECTION 1: Header — company block (left) + INVOICE title (right) ──
    hdr_data = [[
        [
            Paragraph(_GRP_NAME, S['company']),
            Paragraph(_GRP_ADDR1, S['addr']),
            Paragraph(_GRP_ADDR2, S['addr']),
            Paragraph(_GRP_WEB, S['web']),
        ],
        Paragraph('INVOICE', S['invoice_title']),
    ]]
    hdr_tbl = Table(hdr_data, colWidths=[W * 0.55, W * 0.45])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN',  (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING',  (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING',   (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 6),
    ]))
    story.append(hdr_tbl)
    story.append(HRFlowable(width=W, thickness=2, color=_NAVY, spaceAfter=10))

    # ── SECTION 2: Bill To + Invoice Meta ──────────────────────────────────
    meta_rows = [
        ['INVOICE #', inv_num],
        ['DATE',      inv_date_s],
        ['DUE DATE',  inv_date_s],
        ['TERMS',     _TERMS],
    ]
    meta_cells = []
    for lbl, val in meta_rows:
        meta_cells.append([
            Paragraph(lbl, S['label']),
            Paragraph(val, S['value']),
        ])
    meta_tbl = Table(meta_cells, colWidths=[1.1 * inch, 1.5 * inch])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN',  (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING',  (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING',   (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 3),
        ('BACKGROUND', (0, 0), (-1, -1), _LGRAY),
        ('BOX',      (0, 0), (-1, -1), 0.5, _GRAY),
        ('LINEBELOW',(0, 0), (-1, -2), 0.3, colors.HexColor('#CCCCCC')),
    ]))

    bill_header = Table(
        [[Paragraph('BILL TO', S['bill_label'])]],
        colWidths=[1.5 * inch]
    )
    bill_header.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), _NAVY),
        ('LEFTPADDING',  (0, 0), (-1, -1), 6),
        ('TOPPADDING',   (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 4),
    ]))

    bill_section = Table(
        [[
            [
                bill_header,
                Spacer(1, 4),
                Paragraph(_BILL_TO, S['bill_value']),
            ],
            '',
            meta_tbl,
        ]],
        colWidths=[W * 0.38, W * 0.12, W * 0.50],
    )
    bill_section.setStyle(TableStyle([
        ('VALIGN',  (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING',  (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING',   (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 0),
        ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
    ]))
    story.append(bill_section)
    story.append(Spacer(1, 18))

    # ── SECTION 3: Line Items Table ────────────────────────────────────────
    col_widths = [0.95*inch, 2.6*inch, 1.45*inch, 0.75*inch, 1.1*inch]

    def _hcell(txt):
        return Paragraph(txt, S['tbl_hdr'])

    def _cell(txt):
        return Paragraph(txt, S['tbl_body'])

    def _rcell(txt):
        return Paragraph(txt, S['tbl_body_r'])

    def _negcell(txt):
        return Paragraph(txt, S['tbl_body_neg'])

    tbl_header = [
        _hcell('DATE'),
        _hcell('ACTIVITY DESCRIPTION'),
        _hcell('COLLECTIONS'),
        _hcell('RATE'),
        _hcell('AMOUNT'),
    ]

    desc_activity = f'{month_label} {year} {_PROP_LABEL}'

    tbl_rows = [
        # Header
        tbl_header,
        # Row 1: Total fee
        [
            _cell(inv_date_s),
            _cell(desc_activity),
            _rcell(_money(cash_received)),
            _rcell('3.00%'),
            _rcell(_money(total_fee)),
        ],
        # Row 2: Less JLL (indented description)
        [
            _cell(''),
            _cell('Less JLL Portion'),
            _rcell(_money(cash_received)),
            _rcell('1.25%'),
            _negcell(_money(jll_fee, neg=True)),
        ],
        # Spacer row
        ['', '', '', '', ''],
    ]

    item_tbl = Table(tbl_rows, colWidths=col_widths, repeatRows=1)
    item_tbl.setStyle(TableStyle([
        # Header row
        ('BACKGROUND',   (0, 0), (-1, 0), _NAVY),
        ('TEXTCOLOR',    (0, 0), (-1, 0), _WHITE),
        ('TOPPADDING',   (0, 0), (-1, 0), 6),
        ('BOTTOMPADDING',(0, 0), (-1, 0), 6),
        ('LEFTPADDING',  (0, 0), (-1, 0), 6),
        ('RIGHTPADDING', (0, 0), (-1, 0), 6),
        # Data rows
        ('TOPPADDING',   (0, 1), (-1, -1), 5),
        ('BOTTOMPADDING',(0, 1), (-1, -1), 5),
        ('LEFTPADDING',  (0, 1), (-1, -1), 6),
        ('RIGHTPADDING', (0, 1), (-1, -1), 6),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        # Alternating row shading
        ('BACKGROUND', (0, 1), (-1, 1), _LGRAY),
        # Grid
        ('LINEBELOW', (0, 0), (-1, -2), 0.3, colors.HexColor('#CCCCCC')),
        ('BOX', (0, 0), (-1, -1), 0.5, _GRAY),
    ]))
    story.append(item_tbl)
    story.append(Spacer(1, 4))

    # ── SECTION 4: Balance Due ─────────────────────────────────────────────
    bal_tbl = Table(
        [[Paragraph('BALANCE DUE', S['bal_label']),
          Paragraph(_money(balance), S['bal_value'])]],
        colWidths=[sum(col_widths[:-1]), col_widths[-1]],
    )
    bal_tbl.setStyle(TableStyle([
        ('BACKGROUND',   (0, 0), (-1, -1), colors.HexColor('#D6E4F0')),
        ('TOPPADDING',   (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 8),
        ('LEFTPADDING',  (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('BOX', (0, 0), (-1, -1), 0.75, _NAVY),
        ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
        ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
    ]))
    story.append(bal_tbl)
    story.append(Spacer(1, 24))

    # ── SECTION 5: Payment Instructions ───────────────────────────────────
    pay_hdr_style = TableStyle([
        ('BACKGROUND',   (0, 0), (-1, 0), _NAVY),
        ('TOPPADDING',   (0, 0), (-1, 0), 5),
        ('BOTTOMPADDING',(0, 0), (-1, 0), 5),
        ('LEFTPADDING',  (0, 0), (-1, 0), 8),
        ('RIGHTPADDING', (0, 0), (-1, 0), 8),
    ])

    def _pay_row(label, value):
        return [
            Paragraph(label, S['pay_label']),
            Paragraph(value, S['pay_body']),
        ]

    # Electronic column
    ach_rows = [
        [Paragraph('Electronic Payment:', S['pay_hdr']), ''],
        _pay_row('Account Name:', _ACH_NAME),
        _pay_row('Bank Name:', _ACH_BANK),
        _pay_row('Bank Account #:', _ACH_ACCT),
        _pay_row('Bank Routing (ABA) #:', _ACH_RTG),
        _pay_row('Bank Address:', '1 Federal St, Boston, MA 02110'),
    ]
    ach_tbl = Table(ach_rows, colWidths=[1.45*inch, 1.85*inch])
    ach_tbl.setStyle(TableStyle([
        ('SPAN',         (0, 0), (-1, 0)),
        ('BACKGROUND',   (0, 0), (-1, 0), _NAVY),
        ('TOPPADDING',   (0, 0), (-1, 0), 6),
        ('BOTTOMPADDING',(0, 0), (-1, 0), 6),
        ('LEFTPADDING',  (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING',   (0, 1), (-1, -1), 3),
        ('BOTTOMPADDING',(0, 1), (-1, -1), 3),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOX', (0, 0), (-1, -1), 0.5, _GRAY),
        ('LINEBELOW', (0, 1), (-1, -2), 0.3, colors.HexColor('#DDDDDD')),
        ('BACKGROUND', (0, 1), (-1, -1), _LGRAY),
    ]))

    # Check column
    chk_rows = [
        [Paragraph('Check Payment:', S['pay_hdr']), ''],
        _pay_row('Payable to:', _CHK_PAYABLE),
        [Paragraph('Mailing Address:', S['pay_label']), ''],
        [Paragraph(
            f'{_CHK_ADDR1}<br/>{_CHK_ADDR2}<br/>{_CHK_ADDR3}',
            S['pay_body'],
        ), ''],
        _pay_row('Attention:', _CHK_ATTN),
    ]
    chk_tbl = Table(chk_rows, colWidths=[1.2*inch, 2.0*inch])
    chk_tbl.setStyle(TableStyle([
        ('SPAN',         (0, 0), (-1, 0)),
        ('SPAN',         (0, 2), (-1, 2)),
        ('SPAN',         (0, 3), (-1, 3)),
        ('BACKGROUND',   (0, 0), (-1, 0), _NAVY),
        ('TOPPADDING',   (0, 0), (-1, 0), 6),
        ('BOTTOMPADDING',(0, 0), (-1, 0), 6),
        ('LEFTPADDING',  (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING',   (0, 1), (-1, -1), 3),
        ('BOTTOMPADDING',(0, 1), (-1, -1), 3),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOX', (0, 0), (-1, -1), 0.5, _GRAY),
        ('LINEBELOW', (0, 1), (-1, -2), 0.3, colors.HexColor('#DDDDDD')),
        ('BACKGROUND', (0, 1), (-1, -1), _LGRAY),
    ]))

    pay_hdr_row = Table(
        [[Paragraph('PAYMENT INSTRUCTIONS:', S['tbl_hdr'])]],
        colWidths=[W],
    )
    pay_hdr_row.setStyle(TableStyle([
        ('BACKGROUND',   (0, 0), (-1, -1), _NAVY),
        ('TOPPADDING',   (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 6),
        ('LEFTPADDING',  (0, 0), (-1, -1), 8),
    ]))
    story.append(pay_hdr_row)
    story.append(Spacer(1, 6))

    pay_cols = Table(
        [[ach_tbl, Spacer(0.2*inch, 1), chk_tbl]],
        colWidths=[3.3*inch, 0.2*inch, 3.2*inch],
    )
    pay_cols.setStyle(TableStyle([
        ('VALIGN',  (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING',  (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING',   (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 0),
    ]))
    story.append(pay_cols)

    # ── Build ──────────────────────────────────────────────────────────────
    doc.build(story)
    pdf_bytes = buf.getvalue()

    if output_path:
        with open(output_path, 'wb') as f:
            f.write(pdf_bytes)

    return pdf_bytes
