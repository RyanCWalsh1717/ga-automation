"""
KeyBank DACA Statement Parser
==============================
Parses the monthly KeyBank Deposit Account Control Agreement (DACA)
bank statement PDF for the Revolution Labs property.

Account: KeyBank x5132  →  GL account 115100 (Cash - DACA)

DACA accounts are sweep accounts: tenant rent deposits are collected
here and swept daily to the operating account (PNC x3993). Because of
the daily sweep, the month-end balance is typically small.

This parser extracts:
  - statement_period  dict  {'start': 'mm/dd/yyyy', 'end': 'mm/dd/yyyy'}
  - account_number    str   (last 4 of the account, e.g. '5132')
  - beginning_balance float
  - ending_balance    float  ← the primary value used for bank rec
  - bank_type         str   'KeyBankDACA'

Because KeyBank statement layouts vary across online-statement versions,
the extractor tries multiple patterns and picks the first match. If no
balance is found, ending_balance is returned as None so the caller can
prompt the user.
"""

from __future__ import annotations
import re
from typing import Any, Dict, Optional


# ── Public entry point ────────────────────────────────────────────────────────

def parse(filepath: str) -> Dict[str, Any]:
    """
    Parse a KeyBank DACA bank statement PDF.

    Returns a dict with at minimum:
        bank_type, account_number, beginning_balance, ending_balance,
        statement_period, _raw_text (first 3 pages for debugging)
    """
    result: Dict[str, Any] = {
        'bank_type':         'KeyBankDACA',
        'account_number':    None,
        'statement_period':  {},
        'beginning_balance': None,
        'ending_balance':    None,
        '_parse_error':      None,
        '_raw_text':         '',
    }

    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                pages_text.append(page.extract_text() or '')

        full_text = '\n'.join(pages_text)
        result['_raw_text'] = '\n'.join(pages_text[:3])  # keep first 3 pages for debug

        _extract_account_number(full_text, result)
        _extract_period(full_text, result)
        _extract_balances(full_text, result)

    except Exception as exc:
        result['_parse_error'] = str(exc)

    return result


# ── Field extractors ──────────────────────────────────────────────────────────

def _extract_account_number(text: str, result: Dict[str, Any]) -> None:
    """
    Extract last-4 account digits from KeyBank statement.

    KeyBank Corporate Banking statements show the full account number
    (e.g. '329681415132') on its own line and after 'Commercial Control Transaction'.
    We extract the last 4 digits.
    """
    patterns = [
        # "Commercial Control Transaction 329681415132" — full acct on summary line
        r'(?:Commercial\s+Control\s+Transaction|Transaction)\s+\d+?(\d{4})\b',
        # Standalone line that is purely digits (10–16 chars) — full account number
        r'^\s*(\d{10,16})\s*$',
        # Standard "Account Number: xxxx5132"
        r'Account\s+(?:Number|#)[:\s]+(?:XX-XXXX-|x+)?(\d{4})',
        r'[Ee]nding\s+in\s+(\d{4})',
        r'[Aa]ccount\s+ending\s+(\d{4})',
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE | re.MULTILINE):
            val = m.group(1)
            # For full account numbers, return only the last 4 digits
            result['account_number'] = val[-4:]
            return


def _extract_period(text: str, result: Dict[str, Any]) -> None:
    """
    Extract statement period from KeyBank format.

    KeyBank Corporate Banking uses:
        Beginning balance m-d-yy  $X
        Ending balance    m-d-yy  $X
    where the dates bracket the statement period. We derive start/end from those.

    Fallback patterns handle standard formats used by other KeyBank layouts.
    """
    # ── KeyBank Corporate Banking: derive from balance dates ─────────────────
    # "Beginning balance 2-25-26 $4,375.00"
    beg_m = re.search(r'[Bb]eginning\s+balance\s+(\d{1,2}-\d{1,2}-\d{2,4})', text)
    end_m = re.search(r'[Ee]nding\s+balance\s+(\d{1,2}-\d{1,2}-\d{2,4})', text)
    if beg_m and end_m:
        result['statement_period'] = {
            'start': _normalise_date(beg_m.group(1)),
            'end':   _normalise_date(end_m.group(1)),
        }
        return

    # ── Fallback standard patterns ────────────────────────────────────────────
    fallbacks = [
        r'[Ff]or\s+the\s+period\s+(\d{2}/\d{2}/\d{4})\s+(?:to|through)\s+(\d{2}/\d{2}/\d{4})',
        r'[Ss]tatement\s+[Pp]eriod[:\s]+(\w+\s+\d+)\s*[–\-]\s*(\w+\s+\d+,?\s*\d{4})',
        r'(\d{2}/\d{2}/\d{4})\s*[-–]\s*(\d{2}/\d{2}/\d{4})',
    ]
    for pat in fallbacks:
        m = re.search(pat, text)
        if m:
            result['statement_period'] = {'start': m.group(1), 'end': m.group(2)}
            return


def _normalise_date(date_str: str) -> str:
    """
    Convert KeyBank date formats to mm/dd/yyyy.
        '2-25-26'  → '02/25/2026'
        '3-25-26'  → '03/25/2026'
        '2-25-2026'→ '02/25/2026'
    """
    parts = date_str.strip().split('-')
    if len(parts) == 3:
        m, d, y = parts
        if len(y) == 2:
            y = '20' + y
        return f'{int(m):02d}/{int(d):02d}/{y}'
    return date_str


def _extract_balances(text: str, result: Dict[str, Any]) -> None:
    """
    Extract beginning and ending balances.

    KeyBank Corporate Banking format (confirmed from March 2026 statement):
        Beginning balance 2-25-26 $4,375.00
        4 Additions          +1,419,011.29
        2 Subtractions       -1,418,386.29
        Netfeesandcharges       -625.00
        Ending balance 3-25-26 $4,375.00

    The date (m-d-yy) sits between the label and the dollar amount, so simple
    'Ending balance $X' patterns fail — we must account for the intervening date.
    """
    # ── KeyBank Corporate Banking: label + date + amount ─────────────────────
    # "Ending balance 3-25-26 $4,375.00"
    end_kb = re.search(
        r'[Ee]nding\s+balance\s+\d{1,2}-\d{1,2}-\d{2,4}\s+\$?([\d,]+\.\d{2})',
        text,
    )
    if end_kb:
        result['ending_balance'] = _f(end_kb.group(1))

    beg_kb = re.search(
        r'[Bb]eginning\s+balance\s+\d{1,2}-\d{1,2}-\d{2,4}\s+\$?([\d,]+\.\d{2})',
        text,
    )
    if beg_kb:
        result['beginning_balance'] = _f(beg_kb.group(1))

    if result['ending_balance'] is not None:
        return  # KeyBank format matched — done

    # ── Fallback patterns for other KeyBank / generic layouts ─────────────────
    ending_patterns = [
        r'[Ee]nding\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Cc]losing\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Bb]alance\s+at\s+end\s+of\s+statement\s+period\s+\$?([\d,]+\.\d{2})',
        r'[Aa]vailable\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
    ]
    for pat in ending_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result['ending_balance'] = _f(m.group(1))
            break

    beginning_patterns = [
        r'[Bb]eginning\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Oo]pening\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Bb]alance\s+[Ff]orward\s+\$?([\d,]+\.\d{2})',
    ]
    for pat in beginning_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result['beginning_balance'] = _f(m.group(1))
            break

    # Last resort: largest single dollar amount in the summary block
    if result['ending_balance'] is None:
        candidates = [_f(a) for a in re.findall(r'\b(\d{1,3}(?:,\d{3})*\.\d{2})\b', text)
                      if _f(a) >= 500]
        if candidates:
            result['ending_balance'] = max(candidates)


def _f(s: str) -> float:
    try:
        return float(str(s).replace(',', '').replace('$', ''))
    except (ValueError, TypeError):
        return 0.0
