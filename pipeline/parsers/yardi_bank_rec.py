"""
Yardi Bank Reconciliation Report Parser
=========================================
Parses the PDF that Yardi generates as the official bank reconciliation workpaper.
JLL provides this monthly — it is a combined PDF containing:

  Pages 1-3:  Yardi Bank Reconciliation Report
               - Summary: bank balance, outstanding checks, GL balance, difference
               - Outstanding checks list (issued but not yet cleared)
               - Cleared checks detail
               - Cleared other items (sweeps, mortgage wires, etc.)

  Pages 4-5:  Actual PNC Corporate Business Account Statement
               - Beginning/ending balance, transaction detail

  Pages 6-9:  Yardi GL detail for account 111100 (Cash - Operating)

  Page 10:    JLL cash reconciliation worksheet (running balance summary)

Detection:    Page 1 contains "Bank Reconciliation Report" (Yardi heading)

Output dict keys:
  bank_type               'YardiBankRec'
  account_number          str  (e.g. '1092223993')
  property_name           str
  report_date             str  (date report was run, e.g. '3/30/2026')
  statement_date          str  (bank statement end date, e.g. '3/25/2026')
  statement_period        dict  {'start': 'mm/dd/yyyy', 'end': 'mm/dd/yyyy'}
  beginning_balance       float (PNC statement beginning balance)
  ending_balance          float (PNC statement ending balance = bank balance)
  bank_statement_balance  float (same as ending_balance — the pre-rec bank total)
  reconciled_bank_balance float (bank balance less outstanding checks)
  gl_balance              float (GL balance per Yardi rec)
  reconciling_difference  float (should be 0.00 if reconciled)
  outstanding_checks      list of dicts:
                            date, check_number, payee, amount
  total_outstanding_checks float
  cleared_checks          list of dicts:
                            date, tran_number, notes, amount, date_cleared
  cleared_other_items     list of dicts:
                            date, tran_number, notes, amount, date_cleared
  checks                  list of dicts (from PNC statement, for backward compat):
                            date, check_number, amount, reference
  ach_debits              list of dicts (from PNC statement):
                            date, amount, description, reference
  deposits                list of dicts (from PNC statement):
                            date, amount, description, reference
  transactions            list (combined from PNC statement)
  gl_transactions         list of dicts (from Yardi GL detail pages 6-9):
                            date, period, description, vendor, control, reference,
                            debit, credit, remarks, is_check, is_sweep, is_mortgage, is_rent
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# ── Public entry point ────────────────────────────────────────────────────────

def is_yardi_bank_rec(filepath: str) -> bool:
    """Return True if the PDF is a Yardi Bank Reconciliation Report."""
    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            if not pdf.pages:
                return False
            text = pdf.pages[0].extract_text() or ''
            return 'Bank Reconciliation Report' in text
    except Exception:
        return False


def parse(filepath: str, property_code: str = 'revlabpm') -> Dict[str, Any]:
    """
    Parse a Yardi Bank Reconciliation PDF.

    Args:
        filepath:      Path to the PDF file.
        property_code: Yardi property code (e.g. 'revlabpm').  Used to detect
                       the GL detail section in the PDF, which begins every
                       transaction line with the property code.  Must match the
                       code Yardi prints in the exported PDF exactly (lowercase).

    Returns the structured dict described in the module docstring.
    Returns an empty dict with bank_type='YardiBankRec' if parsing fails.
    """
    result: Dict[str, Any] = {
        'bank_type': 'YardiBankRec',
        'account_number': None,
        'property_name': None,
        'report_date': None,
        'statement_date': None,
        'statement_period': {},
        'beginning_balance': None,
        'ending_balance': None,
        'bank_statement_balance': None,
        'reconciled_bank_balance': None,
        'gl_balance': None,
        'reconciling_difference': None,
        'outstanding_checks': [],
        'total_outstanding_checks': 0.0,
        'cleared_checks': [],
        'cleared_other_items': [],
        'checks': [],
        'ach_debits': [],
        'deposits': [],
        'transactions': [],
        'gl_transactions': [],
    }

    try:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            all_pages = [p.extract_text() or '' for p in pdf.pages]

        # Split pages into three sections:
        #   Pages 1-3: Yardi Bank Reconciliation Report
        #   Pages 4-5: PNC Corporate Business Account Statement
        #   Pages 6-9: Yardi GL detail for account 111100
        #   Page 10:   JLL cash worksheet
        # The PNC statement starts with "Corporate Business Account Statement"
        # The GL detail section starts with "111100" on a line (or "revlabpm")
        pnc_start_page = None
        gl_start_page = None

        for i, text in enumerate(all_pages):
            if pnc_start_page is None and 'Corporate Business Account Statement' in text:
                pnc_start_page = i
            elif pnc_start_page is not None and gl_start_page is None:
                # GL section: look for "111100 Cash" header or property-code lines
                _prop_pattern = re.escape(property_code)
                if re.search(r'111100\s+Cash', text) or re.search(rf'^{_prop_pattern}\s', text, re.MULTILINE):
                    gl_start_page = i

        yardi_text = '\n'.join(all_pages[:pnc_start_page] if pnc_start_page else all_pages)

        if pnc_start_page is not None and gl_start_page is not None:
            pnc_text = '\n'.join(all_pages[pnc_start_page:gl_start_page])
            gl_text = '\n'.join(all_pages[gl_start_page:])
        elif pnc_start_page is not None:
            pnc_text = '\n'.join(all_pages[pnc_start_page:])
            gl_text = ''
        else:
            pnc_text = ''
            gl_text = ''

        _parse_yardi_rec_section(yardi_text, result)
        if pnc_text:
            _parse_pnc_statement_section(pnc_text, result)
        if gl_text:
            result['gl_transactions'] = parse_gl_section(gl_text, property_code=property_code)

    except Exception as exc:
        result['_parse_error'] = str(exc)

    return result


# ── Yardi rec section parser ──────────────────────────────────────────────────

def _parse_yardi_rec_section(text: str, result: Dict[str, Any]) -> None:
    """Parse the Yardi Bank Rec Report pages (pages 1-3 of the combined PDF)."""
    lines = text.split('\n')

    # Header fields: property name, report date, statement date, account number
    # Line 1: "{Property Name} PNC {report_date}"
    # Line 2: "Bank Reconciliation Report"
    # Line 3: "{statement_date}"
    # Line 4: "{account_number}"
    for i, line in enumerate(lines):
        if 'Bank Reconciliation Report' in line:
            # Property name and report date are on the previous line
            if i > 0:
                prev = lines[i - 1].strip()
                # Pattern: "Rev Labs Owner LLC PNC 3/30/2026"
                m = re.match(r'^(.+?)\s+PNC\s+(\d{1,2}/\d{1,2}/\d{4})$', prev)
                if m:
                    result['property_name'] = m.group(1).strip()
                    result['report_date'] = m.group(2)
            # Statement date is the next non-empty line
            for j in range(i + 1, min(i + 5, len(lines))):
                stripped = lines[j].strip()
                if stripped and re.match(r'\d{1,2}/\d{1,2}/\d{4}', stripped):
                    result['statement_date'] = stripped
                    break
            # Account number is the next non-empty line after statement_date
            found_date = False
            for j in range(i + 1, min(i + 8, len(lines))):
                stripped = lines[j].strip()
                if not stripped:
                    continue
                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', stripped) and not found_date:
                    found_date = True
                    continue
                if found_date and re.match(r'^\d{7,}$', stripped):
                    result['account_number'] = stripped
                    break
            break

    # Bank Statement Balance
    for line in lines:
        m = re.search(r'Balance Per Bank Statement as of\s+[\d/]+\s+([\d,]+\.\d{2})', line)
        if m:
            result['bank_statement_balance'] = _f(m.group(1))
            result['ending_balance'] = result['bank_statement_balance']
            break

    # Outstanding checks
    outstanding = _parse_outstanding_checks(lines)
    result['outstanding_checks'] = outstanding
    result['total_outstanding_checks'] = sum(c['amount'] for c in outstanding)

    # Reconciled Bank Balance / GL Balance / Difference
    for line in lines:
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

    # Cleared checks and other items
    result['cleared_checks'] = _parse_cleared_checks(lines)
    result['cleared_other_items'] = _parse_cleared_other_items(lines)


def _parse_outstanding_checks(lines: List[str]) -> List[dict]:
    """
    Extract outstanding checks from the Yardi bank rec section.

    Format:
        Outstanding Checks
        Check Date Check Number Payee Amount
        1/6/2026 2712 v0000716 - Weston Ground Lessee LLC 7,151.23
        ...
        Less: Outstanding Checks 135,443.00
    """
    checks = []
    in_section = False
    past_header = False

    for line in lines:
        # Start of section
        if 'Outstanding Checks' in line and 'Check Date' not in line and 'Less:' not in line:
            in_section = True
            past_header = False
            continue

        if not in_section:
            continue

        # Skip the column header row
        if 'Check Date' in line and 'Check Number' in line:
            past_header = True
            continue

        # End of section
        if 'Less: Outstanding Checks' in line or 'Reconciled Bank Balance' in line:
            break

        if not past_header or not line.strip():
            continue

        # Parse row: "1/6/2026 2712 v0000716 - Weston Ground Lessee LLC 7,151.23"
        # Date is m/d/yyyy or mm/dd/yyyy, then check number (digits), then payee, then amount
        m = re.match(
            r'(\d{1,2}/\d{1,2}/\d{4})\s+(\d+)\s+(.+?)\s+([\d,]+\.\d{2})\s*$',
            line.strip(),
        )
        if m:
            checks.append({
                'date': m.group(1),
                'check_number': m.group(2),
                'payee': m.group(3).strip(),
                'amount': _f(m.group(4)),
            })

    return checks


def _parse_cleared_checks(lines: List[str]) -> List[dict]:
    """
    Extract cleared checks from the Yardi bank rec section.

    Format:
        Cleared Checks
        Date Tran # Notes Amount Date Cleared
        1/30/2026 2738 v0000221 - HEARTLINE FITNESS PRODUCTS INC 300.00 3/25/2026
    """
    checks = []
    in_section = False
    past_header = False

    for line in lines:
        if 'Cleared Checks' in line and 'Date Tran' not in line:
            in_section = True
            past_header = False
            continue

        if not in_section:
            continue

        if 'Date Tran #' in line or ('Date' in line and 'Notes' in line and 'Amount' in line):
            past_header = True
            continue

        if 'Total Cleared Checks' in line:
            break

        if 'Cleared Other Items' in line:
            break

        if not past_header or not line.strip():
            continue

        # Pattern: date tran# notes amount date_cleared
        m = re.match(
            r'(\d{1,2}/\d{1,2}/\d{4})\s+(\S+)\s+(.+?)\s+([\d,]+\.\d{2})\s+(\d{1,2}/\d{1,2}/\d{4})\s*$',
            line.strip(),
        )
        if m:
            checks.append({
                'date': m.group(1),
                'tran_number': m.group(2),
                'notes': m.group(3).strip(),
                'amount': _f(m.group(4)),
                'date_cleared': m.group(5),
            })

    return checks


def _parse_cleared_other_items(lines: List[str]) -> List[dict]:
    """
    Extract cleared other items (sweeps, mortgage wires, etc.)

    Format:
        Cleared Other Items
        Date Tran # Notes Amount Date Cleared
        3/3/2026 JE 19482 03.03 Sweep 885,955.88 3/25/2026
    """
    items = []
    in_section = False
    past_header = False

    for line in lines:
        if 'Cleared Other Items' in line:
            in_section = True
            past_header = False
            continue

        if not in_section:
            continue

        if 'Date Tran #' in line or ('Date' in line and 'Notes' in line and 'Amount' in line):
            past_header = True
            continue

        if 'Total Cleared Other Items' in line:
            break

        if not past_header or not line.strip():
            continue

        # Pattern: date tran# notes amount date_cleared (amount may be negative)
        # E.g.: "3/9/2026 JE 19588 03.09.26 Mortgage Wire -778,571.45 3/25/2026"
        m = re.match(
            r'(\d{1,2}/\d{1,2}/\d{4})\s+(\S+(?:\s+\S+)?)\s+(.+?)\s+(-?[\d,]+\.\d{2})\s+(\d{1,2}/\d{1,2}/\d{4})\s*$',
            line.strip(),
        )
        if m:
            items.append({
                'date': m.group(1),
                'tran_number': m.group(2).strip(),
                'notes': m.group(3).strip(),
                'amount': _f(m.group(4)),
                'date_cleared': m.group(5),
            })

    return items


# ── PNC statement section parser ──────────────────────────────────────────────

def _parse_pnc_statement_section(text: str, result: Dict[str, Any]) -> None:
    """
    Parse the embedded PNC Corporate Business Account Statement.
    Extracts beginning/ending balance, period, checks, ACH debits, wire deposits.
    """
    lines = text.split('\n')

    # Statement period
    for line in lines:
        m = re.search(r'For the period\s+(\d{2}/\d{2}/\d{4})\s+to\s+(\d{2}/\d{2}/\d{4})', line)
        if m:
            result['statement_period'] = {'start': m.group(1), 'end': m.group(2)}
            break

    # Account number
    for line in lines:
        m = re.search(r'Account Number:\s+(XX-XXXX-\d+)', line)
        if m and result['account_number'] is None:
            # Keep the full account number (already have the real one from Yardi rec)
            break

    # Beginning / ending balance from Balance Summary
    # Format: line with 4 numbers: beginning, deposits, debits, ending
    for i, line in enumerate(lines):
        if 'Balance Summary' in line:
            # Look for the values line (4 dollar amounts in a row)
            for j in range(i + 1, min(i + 8, len(lines))):
                amounts = re.findall(r'[\d,]+\.\d{2}', lines[j])
                if len(amounts) >= 4:
                    result['beginning_balance'] = _f(amounts[0])
                    # ending_balance = bank_statement_balance; don't overwrite with PNC if already set
                    if result['ending_balance'] is None:
                        result['ending_balance'] = _f(amounts[-1])
                    break
                if len(amounts) >= 2 and result['beginning_balance'] is None:
                    # Some PNC layouts split beginning/ending across lines
                    result['beginning_balance'] = _f(amounts[0])
            break

    # Checks (from PNC statement check grid)
    checks = _extract_pnc_checks(text)
    result['checks'] = checks
    for c in checks:
        result['transactions'].append({
            'type': 'check',
            'date': c['date'],
            'amount': -c['amount'],
            'check_number': c['check_number'],
            'description': f"Check #{c['check_number']}",
        })

    # ACH debits
    ach = _extract_pnc_ach_debits(text)
    result['ach_debits'] = ach
    for a in ach:
        result['transactions'].append({
            'type': 'ach_debit',
            'date': a['date'],
            'amount': -a['amount'],
            'description': a['description'],
        })

    # Wire deposits
    deposits = _extract_pnc_deposits(text)
    result['deposits'] = deposits
    for d in deposits:
        result['transactions'].append({
            'type': 'deposit',
            'date': d['date'],
            'amount': d['amount'],
            'description': d['description'],
        })


def _extract_pnc_checks(text: str) -> List[dict]:
    """Extract checks from the PNC statement check grid."""
    lines = text.split('\n')
    checks = []
    in_checks = False

    for i, line in enumerate(lines):
        if 'Checks and Substitute Checks' in line:
            in_checks = True
            continue

        if in_checks:
            if 'ACH Debits' in line or 'Other Debits' in line or 'Member FDIC' in line:
                break

            # Header row: skip
            if 'Date' in line and 'Check' in line and 'Reference' in line:
                continue

            # PNC uses a 3-column check grid: date check# amount ref  date check# amount ref ...
            # Pattern: mm/dd check_num amount reference_num (repeated)
            matches = re.findall(
                r'(\d{2}/\d{2})\s+(\d+)\s+([\d,]+\.\d{2})\s+(\d+)',
                line,
            )
            for m in matches:
                checks.append({
                    'date': m[0],
                    'check_number': m[1],
                    'amount': _f(m[2]),
                    'reference': m[3],
                })

    return checks


def _extract_pnc_ach_debits(text: str) -> List[dict]:
    """Extract ACH debits from the PNC statement (structured section only)."""
    lines = text.split('\n')
    ach = []
    seen_refs: set = set()

    for i, line in enumerate(lines):
        # "ACH Debits N transactions" section header — process the block below it
        if 'ACH Debits' in line and 'transactions' in line:
            for j in range(i + 1, min(i + 40, len(lines))):
                jline = lines[j]
                if 'Member FDIC' in jline or 'Other Debits' in jline:
                    break
                # Skip header and blank lines
                if not jline.strip() or 'Date' in jline or 'posted' in jline:
                    continue
                m = re.match(
                    r'(\d{2}/\d{2})\s+([\d,]+\.\d{2})\s+(.+?)\s+(\d{10,})',
                    jline.strip(),
                )
                if m:
                    ref = m.group(4)
                    if ref in seen_refs:
                        continue
                    seen_refs.add(ref)
                    desc = m.group(3).strip()
                    # Check next line for continuation (e.g. "Berkadia Loan#011159010")
                    if j + 1 < len(lines):
                        nxt = lines[j + 1].strip()
                        if (nxt and not re.match(r'\d{2}/\d{2}', nxt)
                                and 'Member FDIC' not in nxt
                                and not re.match(r'\d{10,}', nxt)):
                            desc += ' ' + nxt
                    ach.append({
                        'date': m.group(1),
                        'amount': _f(m.group(2)),
                        'description': desc,
                        'reference': ref,
                    })
            break  # Only one ACH Debits section per statement

    return ach


def _extract_pnc_deposits(text: str) -> List[dict]:
    """Extract wire/fund-transfer deposits from the PNC statement."""
    lines = text.split('\n')
    deposits = []
    in_credits = False

    for i, line in enumerate(lines):
        if 'Funds Transfer In' in line and 'transactions' in line:
            in_credits = True
            continue

        if in_credits:
            if line.strip() == '' or any(
                kw in line for kw in ['Checks and Other Debits', 'ACH Debits', 'Member FDIC']
            ):
                break

            # "Date posted Amount description reference"
            m = re.match(r'(\d{2}/\d{2})\s+([\d,]+\.\d{2})\s+(.+?)\s+(\S+)\s*$', line.strip())
            if m:
                deposits.append({
                    'date': m.group(1),
                    'amount': _f(m.group(2)),
                    'description': m.group(3).strip(),
                    'reference': m.group(4),
                })

    return deposits


# ── Yardi GL section parser (pages 6-9) ──────────────────────────────────────

def parse_gl_section(text: str, property_code: str = 'revlabpm') -> List[dict]:
    """
    Parse the Yardi GL detail section (account 111100, Cash - Operating) from
    the text of pages 6-9 of the bank rec PDF.

    pdfplumber renders each transaction as a single line starting with the
    Yardi property code (e.g. 'revlabpm').  Pass the correct property_code
    so the parser can identify GL transaction lines.

    Three line formats appear in practice:

    AP check (K- control):
      {property_code} Entity date period Vendor Name (vNNNNNN) K-NNNNN CHECKNUM DEBIT CREDIT BALANCE[remarks]

    Journal entry / Sweep / Mortgage (J- control):
      {property_code} Entity date period Description J-NNNNN DEBIT CREDIT BALANCE[remarks]
      (no vendor code in parens, no separate reference token)

    Rent application (R- control):
      {property_code} Entity date period Tenant Name (tNNNNNN) R-NNNN APPLY DEBIT CREDIT BALANCE[remarks]

    The balance number is concatenated directly with remarks (no space) by pdfplumber.

    Returns a list of transaction dicts. See module docstring for field definitions.
    """
    transactions = []
    lines = text.split('\n')

    # Pattern for the three amount fields at the end of every line:
    #   DEBIT  CREDIT  BALANCE[optional_remarks]
    # Balance may be followed immediately by remarks without a space.
    # We capture: debit, credit, balance (numbers with commas), then any trailing text.
    AMOUNTS_RE = re.compile(
        r'([\d,]+\.\d{2})\s+'       # debit
        r'([\d,]+\.\d{2})\s+'       # credit
        r'([\d,]+\.\d{2})'          # balance (immediately followed by optional remarks)
        r'(.*)?$'                    # optional remarks (no leading space guaranteed)
    )

    # Control token pattern: K-NNNNN, J-NNNNN, R-NNNN, etc.
    CONTROL_RE = re.compile(r'^([KJR])-\d+$', re.IGNORECASE)

    for raw_line in lines:
        line = raw_line.rstrip()
        if not line.startswith(property_code):
            continue

        # Find the three trailing amount fields
        amt_m = AMOUNTS_RE.search(line)
        if not amt_m:
            continue

        debit   = _f(amt_m.group(1))
        credit  = _f(amt_m.group(2))
        remarks = (amt_m.group(4) or '').strip()

        # Everything before the three amounts is the "prefix" containing
        # entity, date, period, description, control, and optional reference
        prefix = line[:amt_m.start()].strip()

        # Tokenize the prefix; date and period are anchors
        # Expected token order: revlabpm, entity..., date, period, description..., control, [reference]
        tokens = prefix.split()
        if len(tokens) < 7:
            continue

        # Find the date token (m/d/yyyy or mm/dd/yyyy)
        date_idx = None
        for i, tok in enumerate(tokens):
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', tok):
                date_idx = i
                break

        if date_idx is None:
            continue

        date_str = tokens[date_idx]

        # Period token follows date
        period_idx = date_idx + 1
        if period_idx >= len(tokens) or not re.match(r'^\d{2}-\d{4}$', tokens[period_idx]):
            continue
        period = tokens[period_idx]

        # Tokens after period up to (but not including) the control token
        # form the description + control [+ reference]
        remaining = tokens[period_idx + 1:]

        # Find the control token (K-NNN, J-NNN, R-NNN)
        control_idx = None
        for i, tok in enumerate(remaining):
            if CONTROL_RE.match(tok):
                control_idx = i
                break

        if control_idx is None:
            continue

        description = ' '.join(remaining[:control_idx]).strip()
        control     = remaining[control_idx]

        # Reference: token after control, if it exists and is not clearly an amount
        after_control = remaining[control_idx + 1:]
        reference = ''
        if after_control:
            candidate = after_control[0]
            # Reference is alphanumeric (e.g. "2786", "APPLY") — not an amount
            if not re.match(r'^[\d,]+\.\d{2}$', candidate):
                reference = candidate

        # Vendor: description up to the first "(vNNNNNN)" or "(tNNNNNN)" code
        vendor = re.sub(r'\s*\([vt]\d+\)\s*$', '', description).strip()

        # Classify.
        # is_check: reference is purely numeric AND short (≤6 digits = real AP check number).
        # PCard ACH transactions have long date-format reference codes (e.g. "30926001" = 8 digits)
        # which are numeric but are NOT AP checks — they clear via ACH, not as paper checks.
        is_check    = bool(re.match(r'^\d{1,6}$', reference))
        is_rent     = control.upper().startswith('R-')
        is_sweep    = control.upper().startswith('J-') and 'sweep' in description.lower()
        is_mortgage = 'mortgage' in description.lower()

        transactions.append({
            'date':        date_str,
            'period':      period,
            'description': description,
            'vendor':      vendor,
            'control':     control,
            'reference':   reference,
            'debit':       debit,
            'credit':      credit,
            'remarks':     remarks,
            'is_check':    is_check,
            'is_sweep':    is_sweep,
            'is_mortgage': is_mortgage,
            'is_rent':     is_rent,
        })

    return transactions


# ── Utility ───────────────────────────────────────────────────────────────────

def _f(s: str) -> float:
    """Convert a currency string like '1,234,567.89' or '-778,571.45' to float."""
    try:
        return float(str(s).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0
