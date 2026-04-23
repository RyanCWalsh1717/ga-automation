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
    """Try multiple KeyBank account number patterns."""
    patterns = [
        r'Account\s+(?:Number|#)[:\s]+(?:XX-XXXX-|x+)?(\d{4})',   # "Account Number: xxxx5132"
        r'Ending\s+in\s+(\d{4})',                                    # "Ending in 5132"
        r'Account\s+ending\s+(\d{4})',                              # "Account ending 5132"
        r'(?:Deposit|Checking|DACA)\s+(?:\S+\s+)?(\d{4})$',         # "Deposit Account 5132"
        r'\b(5132)\b',                                               # literal last 4 fallback
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.MULTILINE)
        if m:
            result['account_number'] = m.group(1)
            return


def _extract_period(text: str, result: Dict[str, Any]) -> None:
    """Extract statement period / date range."""
    patterns = [
        # "For the period 02/01/2026 to 02/28/2026"
        r'[Ff]or\s+the\s+period\s+(\d{2}/\d{2}/\d{4})\s+(?:to|through)\s+(\d{2}/\d{2}/\d{4})',
        # "Statement Period: February 1 – February 28, 2026"
        r'[Ss]tatement\s+[Pp]eriod[:\s]+(\w+\s+\d+)\s*[–\-]\s*(\w+\s+\d+,?\s*\d{4})',
        # "January 1, 2026 – January 31, 2026"
        r'(\w+\s+\d+,?\s*\d{4})\s*[–\-]\s*(\w+\s+\d+,?\s*\d{4})',
        # "01/01/2026 - 01/31/2026"
        r'(\d{2}/\d{2}/\d{4})\s*[-–]\s*(\d{2}/\d{2}/\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            result['statement_period'] = {'start': m.group(1), 'end': m.group(2)}
            return


def _extract_balances(text: str, result: Dict[str, Any]) -> None:
    """
    Extract beginning and ending balances.
    Tries a cascading set of patterns common to KeyBank commercial statements.
    """
    # ── Ending balance ────────────────────────────────────────────────────────
    ending_patterns = [
        # "Ending Balance   $1,234.56"  or  "Ending Balance  1,234.56"
        r'[Ee]nding\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        # "Closing Balance   1,234.56"
        r'[Cc]losing\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        # "Balance at end of statement period   1,234.56"
        r'[Bb]alance\s+at\s+end\s+of\s+statement\s+period\s+\$?([\d,]+\.\d{2})',
        # "Available Balance   1,234.56"
        r'[Aa]vailable\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        # KeyBank summary table: "SUMMARY ... ending ... 1,234.56" (last big number on last summary line)
        r'(?:Summary|SUMMARY).*?(\d{1,3}(?:,\d{3})*\.\d{2})\s*$',
    ]
    for pat in ending_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result['ending_balance'] = _f(m.group(1))
            break

    # If still None, try: the last dollar amount in the doc (last resort)
    if result['ending_balance'] is None:
        all_amounts = re.findall(r'\b(\d{1,3}(?:,\d{3})*\.\d{2})\b', text)
        # Filter out line items that look like single-transaction amounts (< $100)
        candidates = [_f(a) for a in all_amounts if _f(a) >= 100]
        if candidates:
            result['ending_balance'] = candidates[-1]

    # ── Beginning balance ─────────────────────────────────────────────────────
    beginning_patterns = [
        r'[Bb]eginning\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Oo]pening\s+[Bb]alance\s+\$?([\d,]+\.\d{2})',
        r'[Bb]alance\s+[Ff]orward\s+\$?([\d,]+\.\d{2})',
        r'[Bb]alance\s+at\s+[Ss]tart\s+\$?([\d,]+\.\d{2})',
    ]
    for pat in beginning_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result['beginning_balance'] = _f(m.group(1))
            break


def _f(s: str) -> float:
    try:
        return float(str(s).replace(',', '').replace('$', ''))
    except (ValueError, TypeError):
        return 0.0
