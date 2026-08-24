"""
Eastern Bank Statement Parser
================================
Parses two distinct Eastern Bank PDF formats seen in the wild (confirmed on
real 25 & 40 Hartwell statements 2026-08-24):

1. "Customer Statement" — the primary checking account statement. Shows the
   FULL, unmasked account number (unlike PNC/BofA/KeyBank, which only ever
   reveal a masked last-4). Often bundles a "SWEEP TO ICS" sub-account
   (an overnight cash-sweep investment vehicle) in the same document.

   Page 1 layout:
     Customer Statement Pg 1 of N
     Statement Period: Jul 01, 2026 thru Jul 31, 2026
     Account Number: 03562069801
     Summary - All Accounts
     Type              Account #      Ending Balance
     CASH MGMT CHECKING 03562069801   $249,000.00
     SWEEP TO ICS        500055041    $154,726.09
     25 HARTWELL OWNER LLC
     C/O GREATLAND REALTY PARTNERSLLC
     ...
     CASH MANAGEMENT CHECKING - 03562069801
     Date Transaction Description Withdrawal Deposit Balance
     STARTING BALANCE $249,000.00
     ...

2. "IntraFi Cash Service (ICS) Monthly Statement" — a standalone summary of
   the sweep sub-account only, with a MASKED account ID (e.g.
   '*********801' — note fewer visible digits than the usual last-4
   convention other banks use).

     Account
     25 Hartwell Owner LLC
     ...
     Account ID       Deposit Option   Interest Rate   Opening Balance   Ending Balance
     *********801     Demand           0.00%           $108,831.06       $120,473.84

Returns a dict compatible with the same shape other bank parsers use
(_write_bank_rec_tab() in bs_workpaper_generator.py, and account_number is
what bank_statement_detector.py reads for the Property Setup auto-extract).

account_number is returned as the FULL number when available (Customer
Statement) rather than reformatted to a masked 'xNNNN' style like the other
parsers — Eastern Bank's own statement doesn't mask it, so there's no reason
to throw away real information the other banks' PDFs never gave us in the
first place.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


def parse(filepath: str) -> Dict[str, Any]:
    """Parse an Eastern Bank PDF statement (Customer Statement or ICS Monthly Statement)."""
    result: Dict[str, Any] = {
        'bank_type':               'Eastern Bank',
        'account_number':          None,
        'account_name':            None,
        'sweep_account_number':    None,   # only present on a Customer Statement
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
            full_text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
    except Exception as exc:
        result['_parse_error'] = str(exc)
        return result

    if 'customer statement' in full_text.lower():
        _parse_customer_statement(full_text, result)
    elif 'ics' in full_text.lower() or 'intrafi' in full_text.lower():
        _parse_ics_statement(full_text, result)
    else:
        result['_parse_error'] = 'Recognized as Eastern Bank but neither known layout matched.'

    return result


# ── Internal ─────────────────────────────────────────────────────────────────

def _f(s: str) -> float:
    """Convert '$1,234.56', '(1,234.56)', or '1,234.56' to float (parens = negative)."""
    s = str(s).strip()
    neg = s.startswith('(') and s.endswith(')')
    try:
        val = float(re.sub(r'[,$()]', '', s))
        return -val if neg else val
    except (ValueError, TypeError):
        return 0.0


def _parse_customer_statement(text: str, result: Dict[str, Any]) -> None:
    lines = text.split('\n')

    # ── Primary (checking) account number ───────────────────────────────────
    # "Account Number: 03562069801" — full, unmasked.
    m = re.search(r'Account\s+Number:\s*(\d+)', text, re.IGNORECASE)
    if m:
        result['account_number'] = m.group(1).strip()

    # ── Sweep sub-account number, if present ────────────────────────────────
    # "SWEEP TO ICS          500055041          $154,726.09"
    m = re.search(r'SWEEP\s+TO\s+ICS\s+(\d+)', text, re.IGNORECASE)
    if m:
        result['sweep_account_number'] = m.group(1).strip()

    # ── Account holder name ──────────────────────────────────────────────────
    # The first all-caps line right after the "Summary - All Accounts" table,
    # before the transaction detail section begins (e.g. '25 HARTWELL OWNER
    # LLC'). Anchored to that specific region rather than scanned across the
    # whole (multi-page) text -- a generic uppercase-line heuristic picked up
    # page-2 disclosure boilerplate instead, since a digit-based guard meant
    # to dodge account-number lines was ALSO wrongly excluding the real name,
    # which starts with a building number for this portfolio ('25 Hartwell').
    # Confirmed as a real bug against the actual statement 2026-08-24.
    _summary_start = text.find('Summary - All Accounts')
    _detail_start = text.find('CASH MANAGEMENT CHECKING -')
    if _summary_start != -1:
        _region_end = _detail_start if _detail_start != -1 else _summary_start + 800
        _skip_kw = ('SUMMARY', 'TYPE ACCOUNT', 'CASH MGMT', 'SWEEP TO ICS',
                    'C/O', 'FEDERAL', 'BOSTON', 'LYNN', 'P.O.', 'BOX')
        for line in text[_summary_start:_region_end].split('\n'):
            stripped = line.strip()
            if (stripped and stripped.isupper() and len(stripped) > 5
                    and not any(kw in stripped for kw in _skip_kw)
                    and re.search(r'[A-Z]{3}', stripped)):
                result['account_name'] = stripped
                break

    # ── Statement period ──────────────────────────────────────────────────────
    m = re.search(r'Statement Period:\s*(.+?)\s+thru\s+(.+?)(?:\n|$)', text, re.IGNORECASE)
    if m:
        result['statement_period'] = {'start': m.group(1).strip(), 'end': m.group(2).strip()}

    # ── Checking account starting/ending balance ────────────────────────────
    m = re.search(r'STARTING BALANCE\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
    if m:
        result['beginning_balance'] = _f(m.group(1))

    # Ending balance for the checking account specifically, from the Summary
    # table's first data row (CASH MGMT CHECKING <acct#> <balance>).
    m = re.search(r'CASH MGMT CHECKING\s+\d+\s+\$?([\d,]+\.\d{2})', text, re.IGNORECASE)
    if m:
        result['ending_balance'] = _f(m.group(1))
        result['bank_statement_balance'] = result['ending_balance']
        result['reconciled_bank_balance'] = result['ending_balance']


def _parse_ics_statement(text: str, result: Dict[str, Any]) -> None:
    lines = text.split('\n')

    # ── Masked account ID ─────────────────────────────────────────────────────
    # '*********801' -- fewer visible digits than other banks' last-4 convention;
    # kept as-is rather than padded/guessed.
    m = re.search(r'\*{3,}(\d+)', text)
    if m:
        result['account_number'] = f'*{m.group(1)}'

    # ── Account holder name ──────────────────────────────────────────────────
    # 'Account' label followed by the holder name on the next non-blank line.
    # NOTE: unreliable on this statement's two-column letterhead layout —
    # pdfplumber's extract_text() can interleave the address column with the
    # 'Account:' label column, landing on an address line instead of the
    # real name. Confirmed on a real statement 2026-08-24. Not fixed further
    # since nothing consumes account_name today — only account_number feeds
    # the Property Setup auto-extract, and that's correct on this format.
    for i, line in enumerate(lines):
        if line.strip() == 'Account' and i + 1 < len(lines):
            candidate = lines[i + 1].strip()
            if candidate and candidate.lower() != 'date':
                result['account_name'] = candidate
                break

    # ── Opening / ending balance from the Summary of Accounts row ───────────
    m = re.search(r'Demand\s+[\d.]+%\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})', text)
    if m:
        result['beginning_balance'] = _f(m.group(1))
        result['ending_balance'] = _f(m.group(2))
        result['bank_statement_balance'] = result['ending_balance']
        result['reconciled_bank_balance'] = result['ending_balance']
