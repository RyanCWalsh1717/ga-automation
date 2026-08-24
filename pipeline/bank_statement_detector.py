"""
Bank Statement Detector — Onboarding Account Auto-Extract
==============================================================
At property onboarding time, reads a real bank statement PDF and extracts
its account number so the Property Setup "Bank Accounts" table (step 8) can
be pre-filled for confirmation instead of typed in blind.

Deliberately separate from file_classifier.py's classify_file(): that one is
built for MONTHLY re-classification and leans on property_config bank-account
signals (numbers/names already on file) to disambiguate Operating vs.
Development vs. DACA. At onboarding those signals don't exist yet — this is
the discovery step that produces them — so detection here is simpler and
self-contained: which bank does this statement's own text say it's from.

Only recognizes the banks this pipeline already has a parser for (PNC, Bank
of America, KeyBank, Eastern Bank). Any other bank is correctly reported as
unrecognized — a new bank statement FORMAT needs a new parser, same as a new
lender does for loan statements (see berkadia_loan.py). This never guesses
at a format it can't actually parse.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BankStatementDetectResult:
    bank_type:      str = ''            # 'pnc' | 'bofa' | 'keybank' | 'eastern' | ''
    bank_label:     str = ''            # e.g. 'PNC', 'Bank of America', 'KeyBank', 'Eastern Bank'
    account_number: Optional[str] = None
    suggested_slug: str = ''            # e.g. 'pnc_operating' (type only — dev/operating/daca guess needs a human)
    recognized:     bool = False
    _parse_error:   Optional[str] = None


def detect_and_extract(filepath: str) -> BankStatementDetectResult:
    """Detect which bank a statement PDF is from and extract its account number."""
    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            # Scan every page, not just page 1 — confirmed on a real Eastern
            # Bank "Customer Statement" that the bank's own name never
            # appears on page 1 at all (only from page 2 onward, in the
            # electronic-transfer disclosure boilerplate). Page-1-only
            # detection silently failed to recognize a real file.
            full_text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
    except Exception as exc:
        return BankStatementDetectResult(_parse_error=str(exc))

    text_lower = full_text.lower()

    if 'keybank' in text_lower:
        bank_type, bank_label, slug = 'keybank', 'KeyBank', 'keybank_daca'
        parser_module = 'keybank_daca'
    elif 'bank of america' in text_lower or 'bofa' in text_lower:
        bank_type, bank_label, slug = 'bofa', 'Bank of America', 'bofa_development'
        parser_module = 'bofa_statement'
    elif 'pnc' in text_lower:
        bank_type, bank_label, slug = 'pnc', 'PNC', 'pnc_operating'
        parser_module = 'pnc_bank_statement'
    elif 'eastern bank' in text_lower or 'intrafi' in text_lower:
        bank_type, bank_label, slug = 'eastern', 'Eastern Bank', 'eastern_operating'
        parser_module = 'eastern_bank'
    else:
        return BankStatementDetectResult(
            recognized=False,
            _parse_error=(
                "Bank not recognized — this pipeline only has a parser for PNC, "
                "Bank of America, KeyBank, and Eastern Bank statements. A new bank "
                "needs a new parser built first (same as a new lender does for "
                "loan statements)."
            ),
        )

    try:
        import importlib
        _mod = importlib.import_module(f'parsers.{parser_module}')
        _parsed = _mod.parse(filepath)
        account_number = (_parsed or {}).get('account_number')
    except Exception as exc:
        return BankStatementDetectResult(
            bank_type=bank_type, bank_label=bank_label, recognized=True,
            _parse_error=f'Recognized as {bank_label} but failed to extract details: {exc}',
        )

    return BankStatementDetectResult(
        bank_type=bank_type,
        bank_label=bank_label,
        account_number=account_number,
        suggested_slug=slug,
        recognized=True,
    )
