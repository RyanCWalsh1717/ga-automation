"""
Yardi DACA Bank Reconciliation Report Parser
=============================================
Parses the combined PDF that Yardi generates for the KeyBank DACA account
(account 115100, Restricted Cash – Deposit Escrow, x5132).

JLL exports this monthly.  The combined PDF contains:
  Page 1:    Yardi Bank Reconciliation Report (DACA)
             - Summary: bank balance, GL balance, $0 difference
             - Cleared Deposits  (tenant wire batches)
             - Cleared Other Items (sweeps to PNC, bank fees)
  Pages 2-N: KeyBank Corporate Banking Statement
             - Beginning/ending balance, additions, subtractions
  Pages M+:  Yardi GL detail for account 115100

Detection:
  ``is_yardi_daca_rec()`` returns True when page 1 contains both
  "Bank Reconciliation Report" and ("Deposit account" or "DACA").

Output dict keys (superset of ``parsers.keybank_daca.parse()`` for
drop-in backwards compatibility):

  bank_type               'YardiDACARec'
  account_number          str   (e.g. '329681415132' or last-4 '5132')
  statement_date          str   (e.g. '1/25/2026')
  bank_statement_balance  float (Balance Per Bank Statement)
  reconciled_bank_balance float (Reconciled Bank Balance)
  gl_balance              float (Balance per GL)
  reconciling_difference  float (Difference — normally 0.00)
  cleared_deposits        list of dicts {date, tran_number, notes, amount, date_cleared}
  cleared_other_items     list of dicts {date, tran_number, notes, amount, date_cleared}
  additions               float (KeyBank gross deposits — management fee basis)
  subtractions            float (KeyBank gross subtractions)
  beginning_balance       float (KeyBank statement beginning balance)
  ending_balance          float (= reconciled_bank_balance for compat with keybank_daca)
  _parse_error            str | None
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# ── Detection ─────────────────────────────────────────────────────────────────

def is_yardi_daca_rec(filepath: str) -> bool:
    """Return True if the PDF is a Yardi Bank Rec Report for the DACA account."""
    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            if not pdf.pages:
                return False
            text = pdf.pages[0].extract_text() or ''
            return (
                'Bank Reconciliation Report' in text
                and (
                    'Deposit account' in text
                    or 'DACA' in text.upper()
                    or 'KeyBank' in text
                    or '329681415132' in text   # DACA account number
                )
            )
    except Exception:
        return False


# ── Public entry point ────────────────────────────────────────────────────────

def parse(filepath: str) -> Dict[str, Any]:
    """
    Parse a Yardi DACA Bank Reconciliation PDF.

    Returns the structured dict described in the module docstring.
    Falls back gracefully — missing fields are None/[]/0.0.
    """
    result: Dict[str, Any] = {
        'bank_type':               'YardiDACARec',
        'account_number':          None,
        'statement_date':          None,
        'bank_statement_balance':  None,
        'reconciled_bank_balance': None,
        'gl_balance':              None,
        'reconciling_difference':  0.0,
        'cleared_deposits':        [],
        'cleared_other_items':     [],
        'additions':               None,
        'subtractions':            None,
        'beginning_balance':       None,
        'ending_balance':          None,
        '_parse_error':            None,
    }

    try:
        import pdfplumber
        pages_text: List[str] = []
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                pages_text.append(page.extract_text() or '')

        if not pages_text:
            result['_parse_error'] = 'Empty PDF'
            return result

        # Page 1 is always the Yardi rec section
        _parse_yardi_rec_section(pages_text[0], result)

        # Find KeyBank statement pages (contain "Corporate Banking Statement")
        keybank_chunks: List[str] = []
        for txt in pages_text[1:]:
            if 'Corporate Banking Statement' in txt or (
                'KeyBank' in txt and ('Beginning balance' in txt or 'Additions' in txt)
            ):
                keybank_chunks.append(txt)
        if keybank_chunks:
            _parse_keybank_section('\n'.join(keybank_chunks), result)

        # ending_balance = reconciled bank balance (backward compat with keybank_daca.py)
        result['ending_balance'] = (
            result['reconciled_bank_balance']
            or result['bank_statement_balance']
        )

    except Exception as exc:
        result['_parse_error'] = str(exc)

    return result


# ── Yardi rec section (page 1) ────────────────────────────────────────────────

def _parse_yardi_rec_section(text: str, result: Dict[str, Any]) -> None:
    lines = text.split('\n')

    # ── Account number ────────────────────────────────────────────────────────
    # Yardi DACA rec prints the account number as a standalone line of digits,
    # or after "Bank Reconciliation Report".
    for line in lines:
        stripped = line.strip()
        if re.match(r'^\d{8,}$', stripped):
            result['account_number'] = stripped
            break

    # ── Statement date ────────────────────────────────────────────────────────
    # "Balance Per Bank Statement as of 1/25/2026 4,375.00"
    for line in lines:
        m = re.search(r'Balance Per Bank Statement as of\s+(\d{1,2}/\d{1,2}/\d{4})', line)
        if m:
            result['statement_date'] = m.group(1)
            break

    # ── Summary balances ──────────────────────────────────────────────────────
    for line in lines:
        m = re.search(r'Balance Per Bank Statement as of\s+[\d/]+\s+([\d,]+\.\d{2})', line)
        if m:
            result['bank_statement_balance'] = _f(m.group(1))

        m = re.search(r'Reconciled Bank Balance\s+([\d,]+\.\d{2})', line)
        if m:
            result['reconciled_bank_balance'] = _f(m.group(1))

        m = re.search(r'Balance per GL as of\s+[\d/]+\s+([\d,]+\.\d{2})', line)
        if m:
            result['gl_balance'] = _f(m.group(1))

        m = re.search(r'Reconciled Balance Per G/L\s+([\d,]+\.\d{2})', line)
        if m and result['gl_balance'] is None:
            result['gl_balance'] = _f(m.group(1))

        m = re.search(r'Difference\s*\(.*?\)\s+([\d,]+\.\d{2})', line)
        if m:
            result['reconciling_difference'] = _f(m.group(1))

    # ── Cleared Deposits ──────────────────────────────────────────────────────
    result['cleared_deposits'] = _parse_cleared_deposits(lines)

    # ── Cleared Other Items ───────────────────────────────────────────────────
    result['cleared_other_items'] = _parse_cleared_other_items(lines)


def _parse_cleared_deposits(lines: List[str]) -> List[dict]:
    """
    Parse the Cleared Deposits section.

    DACA format — Notes column is always empty:
        Date  Tran #  Notes  Amount  Date Cleared
        1/2/2026  134  555,863.01  1/25/2026

    The tran# is a plain integer (no notes text between it and the amount).
    """
    deposits: List[dict] = []
    in_section = False
    past_header = False

    for line in lines:
        if 'Cleared Deposits' in line and 'Total' not in line:
            in_section = True
            past_header = False
            continue

        if not in_section:
            continue

        # Column header row
        if re.search(r'Date\s+Tran\s*#', line, re.IGNORECASE):
            past_header = True
            continue

        # End markers
        if 'Total Cleared Deposits' in line or 'Cleared Other Items' in line:
            break

        if not past_header or not line.strip():
            continue

        # Row: "1/2/2026  134  555,863.01  1/25/2026"
        # No notes, so: date tran# amount date_cleared (4 fields)
        m = re.match(
            r'(\d{1,2}/\d{1,2}/\d{4})\s+'   # date
            r'(\S+)\s+'                       # tran# (may be "JE 18364" but usually just int)
            r'([\d,]+\.\d{2})\s+'            # amount (always positive for deposits)
            r'(\d{1,2}/\d{1,2}/\d{4})',      # date cleared
            line.strip(),
        )
        if m:
            deposits.append({
                'date':         m.group(1),
                'tran_number':  m.group(2),
                'notes':        '',
                'amount':       _f(m.group(3)),
                'date_cleared': m.group(4),
            })

    return deposits


def _parse_cleared_other_items(lines: List[str]) -> List[dict]:
    """
    Parse the Cleared Other Items section (sweeps to PNC, bank fees — negative amounts).

    Format:
        1/2/2026  JE 18364  01.02 Sweep  -217,871.54  1/25/2026
    """
    items: List[dict] = []
    in_section = False
    past_header = False

    for line in lines:
        if 'Cleared Other Items' in line and 'Total' not in line:
            in_section = True
            past_header = False
            continue

        if not in_section:
            continue

        if re.search(r'Date\s+Tran\s*#', line, re.IGNORECASE):
            past_header = True
            continue

        if 'Total Cleared Other Items' in line:
            break

        if not past_header or not line.strip():
            continue

        # Row: "date  tran#(may be 'JE 18364')  notes  amount  date_cleared"
        # Amount can be negative.
        m = re.match(
            r'(\d{1,2}/\d{1,2}/\d{4})\s+'           # date
            r'(JE\s+\d+|\d+)\s+'                     # tran# — "JE 18364" or plain int
            r'(.+?)\s+'                               # notes (description)
            r'(-?[\d,]+\.\d{2})\s+'                  # amount (may be negative)
            r'(\d{1,2}/\d{1,2}/\d{4})',              # date cleared
            line.strip(),
        )
        if m:
            items.append({
                'date':         m.group(1),
                'tran_number':  m.group(2).strip(),
                'notes':        m.group(3).strip(),
                'amount':       _f(m.group(4)),
                'date_cleared': m.group(5),
            })

    return items


# ── KeyBank statement section ─────────────────────────────────────────────────

def _parse_keybank_section(text: str, result: Dict[str, Any]) -> None:
    """
    Extract additions / subtractions / balances from the embedded KeyBank
    Corporate Banking Statement.  Used as management fee basis (additions).

    KeyBank format:
        Beginning balance m-d-yy  $4,375.00
        5 Additions               +1,767,462.08
        3 Subtractions            -1,766,837.08
        Net fees and charges      -625.00
        Ending balance m-d-yy     $4,375.00
    """
    # Additions
    add_m = re.search(r'\d+\s+[Aa]dditions\s+\+?([\d,]+\.\d{2})', text)
    if add_m:
        result['additions'] = _f(add_m.group(1))

    # Subtractions
    sub_m = re.search(r'\d+\s+[Ss]ubtractions\s+-?([\d,]+\.\d{2})', text)
    if sub_m:
        result['subtractions'] = _f(sub_m.group(1))

    # Beginning balance: "Beginning balance m-d-yy $X"
    beg_m = re.search(
        r'[Bb]eginning\s+balance\s+\d{1,2}-\d{1,2}-\d{2,4}\s+\$?([\d,]+\.\d{2})',
        text,
    )
    if beg_m and result['beginning_balance'] is None:
        result['beginning_balance'] = _f(beg_m.group(1))

    # Ending balance
    end_m = re.search(
        r'[Ee]nding\s+balance\s+\d{1,2}-\d{1,2}-\d{2,4}\s+\$?([\d,]+\.\d{2})',
        text,
    )
    if end_m and result['bank_statement_balance'] is None:
        result['bank_statement_balance'] = _f(end_m.group(1))


# ── Utility ───────────────────────────────────────────────────────────────────

def _f(s: str) -> float:
    try:
        return float(str(s).replace(',', '').replace('$', '').replace('+', ''))
    except (ValueError, TypeError):
        return 0.0
