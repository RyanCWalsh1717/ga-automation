"""
Bank of America Statement Parser — Development Account (revlabs)
================================================================
Parses a BofA Full Analysis Business Checking PDF statement.

Page 1 Account Summary layout:
  Your Full Analysis Business Checking
  for March 1, 2026 to March 31, 2026  Account number: 4660 0791 3132

  Account summary
  Beginning balance on March 1, 2026            $153,410.10
  # of deposits/credits: 0
  Deposits and other credits                          0.00
  # of withdrawals/debits: 0
  Withdrawals and other debits                       -0.00
  # of days in cycle: 31
  Checks                                             -0.00
  Average ledger balance:                      $153,410.10
  Service fees                                       -0.00
  Ending balance on March 31, 2026             $153,410.10

Returns a dict compatible with _write_bank_rec_tab() in bs_workpaper_generator.py:
  bank_type                'BofA'
  account_number           str  (e.g. 'x3132')
  account_name             str  (e.g. 'Revolution Labs Owner, LLC')
  statement_period         dict  {'start': 'March 1, 2026', 'end': 'March 31, 2026'}
  beginning_balance        float
  ending_balance           float
  bank_statement_balance   float  (= ending_balance)
  reconciled_bank_balance  float  (= ending_balance — no outstanding items)
  total_outstanding_checks float  (= 0.0 for dormant account)
  outstanding_checks       list   (= [])
  cleared_checks           list   (= [])
  gl_balance               float  (= 0.0 — revlabs GL not parsed separately)
  reconciling_difference   None
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


def parse(filepath: str) -> Dict[str, Any]:
    """
    Parse a Bank of America Full Analysis Business Checking PDF.

    Returns a dict in the standard bank rec format expected by
    _write_bank_rec_tab() in bs_workpaper_generator.py.
    """
    result: Dict[str, Any] = {
        'bank_type':               'BofA',
        'account_number':          None,
        'account_name':            None,
        'statement_period':        {},
        'beginning_balance':       None,
        'ending_balance':          None,
        'bank_statement_balance':  None,
        'reconciled_bank_balance': None,
        'gl_balance':              0.0,
        'reconciling_difference':  None,
        'total_outstanding_checks': 0.0,
        'outstanding_checks':      [],
        'cleared_checks':          [],
        'cleared_other_items':     [],
    }

    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            # All parsing lives on page 1
            page1_text = pdf.pages[0].extract_text() or '' if pdf.pages else ''

        _parse_page1(page1_text, result)

    except Exception as exc:
        result['_parse_error'] = str(exc)

    return result


# ── Internal ───────────────────────────────────────────────────────────────────

def _f(s: str) -> float:
    """Convert '$1,234.56' or '1,234.56' to float."""
    try:
        return float(re.sub(r'[,$]', '', str(s)))
    except (ValueError, TypeError):
        return 0.0


def _parse_page1(text: str, result: Dict[str, Any]) -> None:
    lines = text.split('\n')

    # ── Account number ────────────────────────────────────────────────────────
    # "Account number: 4660 0791 3132"  or  "Account # 4660 0791 3132"
    for line in lines:
        m = re.search(r'Account\s+(?:number|#)[:\s]+([\d\s]+)', line, re.IGNORECASE)
        if m:
            raw = m.group(1).strip().replace(' ', '')
            result['account_number'] = f'x{raw[-4:]}' if len(raw) >= 4 else raw
            break

    # ── Account name ──────────────────────────────────────────────────────────
    # Appears as a standalone all-caps line near the top (e.g. "REVOLUTION LABS OWNER, LLC")
    for line in lines:
        stripped = line.strip()
        if (stripped.isupper() and len(stripped) > 5
                and not any(kw in stripped for kw in ('P.O.', 'BOSTON', 'TAMPA', 'FL', 'MA', 'DE'))
                and re.search(r'[A-Z]{3}', stripped)):
            result['account_name'] = stripped.title()
            break

    # ── Statement period ──────────────────────────────────────────────────────
    # "for March 1, 2026 to March 31, 2026"
    for line in lines:
        m = re.search(
            r'for\s+(\w+ \d+,\s*\d{4})\s+to\s+(\w+ \d+,\s*\d{4})',
            line, re.IGNORECASE,
        )
        if m:
            result['statement_period'] = {
                'start': m.group(1).strip(),
                'end':   m.group(2).strip(),
            }
            break

    # ── Beginning balance ─────────────────────────────────────────────────────
    # "Beginning balance on March 1, 2026  $153,410.10"
    for line in lines:
        m = re.search(r'Beginning balance\b.+?\$([\d,]+\.\d{2})', line, re.IGNORECASE)
        if m:
            result['beginning_balance'] = _f(m.group(1))
            break

    # ── Ending balance ────────────────────────────────────────────────────────
    # "Ending balance on March 31, 2026  $153,410.10"
    for line in lines:
        m = re.search(r'Ending balance\b.+?\$([\d,]+\.\d{2})', line, re.IGNORECASE)
        if m:
            result['ending_balance'] = _f(m.group(1))
            break

    # ── Populate bank rec alias fields ────────────────────────────────────────
    ending = result['ending_balance'] or result['beginning_balance'] or 0.0
    result['bank_statement_balance']  = ending
    result['reconciled_bank_balance'] = ending   # no outstanding items
