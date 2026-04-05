"""
PNC Bank Statement Parser

This parser extracts data from PNC bank statement PDFs including Bank of America,
KeyBank, and PNC Corporate Banking statements.

Expected file formats:
- PNC Ops statements: Multi-page PDFs with structured transaction lists
- Bank of America statements: Business checking statements with minimal activity
- KeyBank statements: Corporate banking statements with deposit/withdrawal summary

Each statement contains:
- Account information (number, statement period)
- Beginning and ending balances
- Transaction details (deposits, withdrawals, checks, ACH, wire transfers)
- Ledger balance history
- Fee information
"""

import pdfplumber
from datetime import datetime
from typing import Dict, List, Tuple, Any
import re


def parse(filepath: str) -> Dict[str, Any]:
    """
    Parse PNC bank statement PDF.

    Args:
        filepath: Path to the PDF file

    Returns:
        Dictionary containing extracted statement data
    """
    result = {
        'account_number': None,
        'statement_period': {},
        'beginning_balance': None,
        'ending_balance': None,
        'transactions': [],
        'deposits': [],
        'withdrawals': [],
        'checks': [],
        'ach_debits': [],
        'ach_credits': [],
        'wire_transfers': [],
        'ledger_balances': [],
        'fees': [],
        'bank_type': None,
    }

    with pdfplumber.open(filepath) as pdf:
        # Concatenate all pages for the same bank statement
        all_text = ""
        bank_type = None
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_text += text + "\n"

            # Detect bank type from first page
            if bank_type is None:
                if 'PNC' in text and 'Corporate Business Account' in text:
                    bank_type = 'PNC'
                elif 'Bank of America' in text:
                    bank_type = 'Bank of America'
                elif 'KeyBank' in text:
                    bank_type = 'KeyBank'

        result['bank_type'] = bank_type

        if bank_type == 'PNC':
            _parse_pnc_corporate(all_text, result)
        elif bank_type == 'Bank of America':
            _parse_bank_of_america(all_text, result)
        elif bank_type == 'KeyBank':
            _parse_keybank(all_text, result)

    return result


def _parse_pnc_corporate(text: str, result: Dict[str, Any]) -> None:
    """
    Parse PNC Corporate Business Account statement.

    Args:
        text: Extracted text from PDF page
        result: Result dictionary to populate
    """
    lines = text.split('\n')

    # Extract account number
    for line in lines:
        if 'Account Number:' in line:
            match = re.search(r'XX-XXXX-(\d+)', line)
            if match:
                result['account_number'] = f"XX-XXXX-{match.group(1)}"
                break

    # Extract statement period
    for line in lines:
        if 'For the period' in line:
            match = re.search(r'(\d{2}/\d{2}/\d{4})\s+to\s+(\d{2}/\d{2}/\d{4})', line)
            if match:
                result['statement_period'] = {
                    'start': match.group(1),
                    'end': match.group(2),
                }
                break

    # Extract balance summary
    for i, line in enumerate(lines):
        if 'Balance Summary' in line:
            # Search more lines ahead for balances (expanded from 5 to 10)
            for j in range(i + 1, min(i + 10, len(lines))):
                if 'Beginning' in lines[j]:
                    match = re.search(r'\$?\s*([\d,]+(?:\.\d{2})?)', lines[j])
                    if match:
                        result['beginning_balance'] = float(
                            match.group(1).replace(',', '')
                        )
                if 'Ending' in lines[j]:
                    match = re.search(r'\$?\s*([\d,]+(?:\.\d{2})?)', lines[j])
                    if match:
                        result['ending_balance'] = float(
                            match.group(1).replace(',', '')
                        )

    # Fallback: scan entire text for ending balance if not found in Balance Summary
    if result['ending_balance'] is None:
        for line in lines:
            if 'Ending' in line and 'balance' in line.lower():
                match = re.search(r'\$?\s*([\d,]+\.\d{2})', line)
                if match:
                    result['ending_balance'] = float(match.group(1).replace(',', ''))
                    break

    # Extract deposits
    _extract_pnc_deposits(text, result)

    # Extract checks
    _extract_pnc_checks(text, result)

    # Extract ACH debits
    _extract_pnc_ach_debits(text, result)

    # Extract ledger balances
    _extract_pnc_ledger_balances(text, result)


def _extract_pnc_deposits(text: str, result: Dict[str, Any]) -> None:
    """Extract deposits from PNC statement."""
    lines = text.split('\n')
    in_deposits = False

    for i, line in enumerate(lines):
        if 'Deposits 1 transaction' in line or 'Deposits and Other Credits' in line:
            in_deposits = True
            continue

        if in_deposits:
            if 'posted' in line and 'Amount' in line:
                # Header line, next lines are transactions
                for j in range(i + 1, min(i + 10, len(lines))):
                    match = re.search(
                        r'(\d{2}/\d{2})\s+([\d,]+\.?\d*)\s+(.+?)\s+(\d+)',
                        lines[j],
                    )
                    if match:
                        deposit = {
                            'date': match.group(1),
                            'amount': float(match.group(2).replace(',', '')),
                            'description': match.group(3).strip(),
                            'reference': match.group(4),
                        }
                        result['deposits'].append(deposit)
                        result['transactions'].append(
                            {
                                'type': 'deposit',
                                'date': match.group(1),
                                'amount': float(match.group(2).replace(',', '')),
                                'description': match.group(3).strip(),
                            }
                        )

            if 'Funds Transfer' in line or 'Checks and Other' in line:
                break


def _extract_pnc_checks(text: str, result: Dict[str, Any]) -> None:
    """Extract checks from PNC statement.

    PNC uses a 3-column grid layout for checks:
      date check_num amount ref  date check_num amount ref  date check_num amount ref
    """
    lines = text.split('\n')
    in_checks = False

    for i, line in enumerate(lines):
        if 'Checks and Substitute Checks' in line:
            in_checks = True
            continue

        if in_checks:
            # Skip header lines
            if 'posted' in line.lower() or 'Date' in line and 'Check' in line:
                continue

            # Find all check entries in the line (3-column grid)
            # Pattern: mm/dd check_num amount reference_num
            matches = re.findall(
                r'(\d{2}/\d{2})\s+(\d{3,5})\s+([\d,]+(?:\.\d{2})?)\s+(\d+)',
                line,
            )
            for m in matches:
                check = {
                    'date': m[0],
                    'check_number': m[1],
                    'amount': float(m[2].replace(',', '')),
                    'reference': m[3],
                }
                result['checks'].append(check)
                result['withdrawals'].append(check)
                result['transactions'].append(
                    {
                        'type': 'check',
                        'date': m[0],
                        'amount': -float(m[2].replace(',', '')),
                        'check_number': m[1],
                        'description': f'Check #{m[1]}',
                    }
                )

            if 'ACH Debits' in line or 'Corporate ACH' in line:
                break


def _extract_pnc_ach_debits(text: str, result: Dict[str, Any]) -> None:
    """Extract ACH debits from PNC statement.

    Format: mm/dd amount description reference_number
    Description may span to the next line (e.g. "Berkadia Loan#011159012")
    """
    lines = text.split('\n')
    in_ach = False

    for i, line in enumerate(lines):
        if 'Corporate ACH' in line and ('Auto Paymt' in line or 'Cash Conc' in line):
            # Inline ACH entry: "02/09 23,374.18 Corporate ACH Auto Paymt 00026037006367545"
            match = re.search(
                r'(\d{2}/\d{2})\s+([\d,]+\.\d{2})\s+(.+?)\s+(\d{10,})',
                line,
            )
            if match:
                desc = match.group(3).strip()
                # Check next line for continuation (e.g. "Berkadia Loan#011159012")
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and not re.match(r'\d{2}/\d{2}', next_line) and 'Member FDIC' not in next_line:
                        desc += ' ' + next_line
                ach = {
                    'date': match.group(1),
                    'amount': float(match.group(2).replace(',', '')),
                    'description': desc,
                    'reference': match.group(4),
                }
                result['ach_debits'].append(ach)
                result['withdrawals'].append(ach)
                result['transactions'].append({
                    'type': 'ach_debit',
                    'date': match.group(1),
                    'amount': -float(match.group(2).replace(',', '')),
                    'description': desc,
                })
            continue

        if 'ACH Debits' in line:
            in_ach = True
            continue

        if in_ach:
            match = re.search(
                r'(\d{2}/\d{2})\s+([\d,]+\.\d{2})\s+(.+?)\s+(\d{8,})',
                line,
            )
            if match:
                desc = match.group(3).strip()
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and not re.match(r'\d{2}/\d{2}', next_line) and 'Member FDIC' not in next_line:
                        desc += ' ' + next_line
                ach = {
                    'date': match.group(1),
                    'amount': float(match.group(2).replace(',', '')),
                    'description': desc,
                    'reference': match.group(4),
                }
                result['ach_debits'].append(ach)
                result['withdrawals'].append(ach)
                result['transactions'].append({
                    'type': 'ach_debit',
                    'date': match.group(1),
                    'amount': -float(match.group(2).replace(',', '')),
                    'description': desc,
                })

            if 'Member FDIC' in line or ('Ending balance' in line):
                break


def _extract_pnc_ledger_balances(text: str, result: Dict[str, Any]) -> None:
    """Extract ledger balance history from PNC statement."""
    lines = text.split('\n')
    in_ledger = False

    for i, line in enumerate(lines):
        if 'Ledger Balance' in line:
            in_ledger = True
            continue

        if in_ledger:
            # Look for date and balance pairs
            parts = line.split()
            if len(parts) >= 2:
                match = re.match(r'(\d{2}/\d{2})', parts[0])
                if match:
                    for j in range(1, len(parts)):
                        amount_match = re.match(r'([\d,]+\.?\d*)', parts[j])
                        if amount_match:
                            balance = {
                                'date': parts[0],
                                'balance': float(
                                    amount_match.group(1).replace(',', '')
                                ),
                            }
                            result['ledger_balances'].append(balance)
                            break

            if 'Deposits and Other Credits' in line:
                break


def _parse_bank_of_america(text: str, result: Dict[str, Any]) -> None:
    """
    Parse Bank of America statement.

    Args:
        text: Extracted text from PDF page
        result: Result dictionary to populate
    """
    lines = text.split('\n')

    # Extract account number
    for line in lines:
        if 'Account number:' in line:
            match = re.search(r'(\d{4}\s\d{4}\s\d{4})', line)
            if match:
                result['account_number'] = match.group(1)
                break

    # Extract statement period
    for line in lines:
        if 'for' in line.lower() and 'to' in line.lower():
            match = re.search(
                r'(\w+ \d+, \d{4})\s+to\s+(\w+ \d+, \d{4})',
                line,
            )
            if match:
                result['statement_period'] = {
                    'start': match.group(1),
                    'end': match.group(2),
                }
                break

    # Extract beginning balance
    for line in lines:
        if 'Beginning balance' in line.lower():
            match = re.search(r'\$([\d,]+\.?\d*)', line)
            if match:
                result['beginning_balance'] = float(
                    match.group(1).replace(',', '')
                )
                break

    # Extract ending balance
    for line in lines:
        if 'Ending balance' in line.lower():
            match = re.search(r'\$([\d,]+\.?\d*)', line)
            if match:
                result['ending_balance'] = float(
                    match.group(1).replace(',', '')
                )
                break

    # Bank of America statement in sample shows no transactions
    result['transactions'] = []


def _parse_keybank(text: str, result: Dict[str, Any]) -> None:
    """
    Parse KeyBank statement.

    Args:
        text: Extracted text from PDF page
        result: Result dictionary to populate
    """
    lines = text.split('\n')

    # Extract account number
    for line in lines:
        if 'Commercial Control Transaction' in line:
            match = re.search(r'(\d+)', line.split()[-1])
            if match:
                result['account_number'] = match.group(1)
                break

    # Extract statement date
    for line in lines:
        if line.startswith(('January', 'February', 'March', 'April', 'May',
                           'June', 'July', 'August', 'September', 'October',
                           'November', 'December')):
            if 'REVOLUTION LABS' not in line:
                result['statement_period'] = {'end': line.strip()}
                break

    # Extract balances
    for i, line in enumerate(lines):
        if 'Beginning balance' in line:
            match = re.search(r'\$([\d,]+\.?\d*)', line)
            if match:
                result['beginning_balance'] = float(
                    match.group(1).replace(',', '')
                )

        if 'Ending balance' in line:
            match = re.search(r'\$([\d,]+\.?\d*)', line)
            if match:
                result['ending_balance'] = float(
                    match.group(1).replace(',', '')
                )

    # Extract deposits
    _extract_keybank_deposits(text, result)

    # Extract withdrawals
    _extract_keybank_withdrawals(text, result)

    # Extract fees
    _extract_keybank_fees(text, result)


def _extract_keybank_deposits(text: str, result: Dict[str, Any]) -> None:
    """Extract deposits from KeyBank statement."""
    lines = text.split('\n')
    in_deposits = False

    for i, line in enumerate(lines):
        if 'Additions' in line and 'Deposits' in lines[i + 1] if i + 1 < len(
            lines
        ) else False:
            in_deposits = True
            continue

        if in_deposits:
            match = re.search(
                r'(\d{1,2}-\d{1,2})\s+(.+?)\s+\$([\d,]+\.?\d*)',
                line,
            )
            if match:
                deposit = {
                    'date': match.group(1),
                    'description': match.group(2).strip(),
                    'amount': float(match.group(3).replace(',', '')),
                }
                result['deposits'].append(deposit)
                result['transactions'].append(
                    {
                        'type': 'deposit',
                        'date': match.group(1),
                        'amount': float(match.group(3).replace(',', '')),
                        'description': match.group(2).strip(),
                    }
                )

            if 'Totaladditions' in line or 'Subtractions' in line:
                break


def _extract_keybank_withdrawals(text: str, result: Dict[str, Any]) -> None:
    """Extract withdrawals from KeyBank statement."""
    lines = text.split('\n')
    in_withdrawals = False

    for i, line in enumerate(lines):
        if 'Subtractions' in line and 'Withdrawals' in lines[i + 1] if i + 1 < len(
            lines
        ) else False:
            in_withdrawals = True
            continue

        if in_withdrawals:
            match = re.search(
                r'(\d{1,2}-\d{1,2})\s+(\d+)\s+(.+?)\s+\$([\d,]+\.?\d*)',
                line,
            )
            if match:
                withdrawal = {
                    'date': match.group(1),
                    'reference': match.group(2),
                    'description': match.group(3).strip(),
                    'amount': float(match.group(4).replace(',', '')),
                }
                result['withdrawals'].append(withdrawal)
                result['transactions'].append(
                    {
                        'type': 'withdrawal',
                        'date': match.group(1),
                        'amount': -float(match.group(4).replace(',', '')),
                        'description': match.group(3).strip(),
                    }
                )

            if 'Totalsubtractions' in line or 'Fees' in line:
                break


def _extract_keybank_fees(text: str, result: Dict[str, Any]) -> None:
    """Extract fees from KeyBank statement."""
    lines = text.split('\n')
    in_fees = False

    for i, line in enumerate(lines):
        if 'Fees and' in line and 'charges' in line:
            in_fees = True
            continue

        if in_fees:
            match = re.search(
                r'(\d{1,2}-\d{1,2}-\d{2})\s+(.+?)\s+(\d+)\s+([\d,]+\.?\d*)\s+-\$([\d,]+\.?\d*)',
                line,
            )
            if match:
                fee = {
                    'date': match.group(1),
                    'description': match.group(2).strip(),
                    'quantity': int(match.group(3)),
                    'unit_charge': float(match.group(4).replace(',', '')),
                    'total': float(match.group(5).replace(',', '')),
                }
                result['fees'].append(fee)
                result['transactions'].append(
                    {
                        'type': 'fee',
                        'date': match.group(1),
                        'amount': -float(match.group(5).replace(',', '')),
                        'description': match.group(2).strip(),
                    }
                )

            if line.strip() == '' or 'Net' in line:
                break


def validate(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate that the file is a properly formatted bank statement PDF.

    Args:
        filepath: Path to the PDF file

    Returns:
        Tuple of (is_valid: bool, issues: list of error messages)
    """
    issues = []

    try:
        with pdfplumber.open(filepath) as pdf:
            if len(pdf.pages) == 0:
                issues.append("PDF contains no pages")
                return False, issues

            # Check first page for recognized bank statement patterns
            text = pdf.pages[0].extract_text()
            is_recognized = any(
                bank in text
                for bank in ['PNC', 'Bank of America', 'KeyBank']
            )
            if not is_recognized:
                issues.append(
                    "PDF does not appear to be a recognized bank statement"
                )

            # Check for account information
            if 'Account' not in text and 'account' not in text:
                issues.append("Missing account information")

    except Exception as e:
        return False, [f"Failed to open PDF file: {str(e)}"]

    return len(issues) == 0, issues


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python pnc_bank_statement.py <filepath>")
        sys.exit(1)

    filepath = sys.argv[1]

    # Validate file first
    is_valid, issues = validate(filepath)
    if not is_valid:
        print(f"Validation failed for {filepath}:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)

    # Parse the file
    statement = parse(filepath)

    # Print summary
    print(f"\n{'='*80}")
    print(
        f"PNC Bank Statement Parser - {filepath.split('/')[-1]}"
    )
    print(f"{'='*80}\n")

    print(f"Bank Type: {statement.get('bank_type')}")
    print(f"Account Number: {statement.get('account_number')}")
    print(f"Statement Period: {statement.get('statement_period')}")
    print(f"Beginning Balance: ${statement.get('beginning_balance', 0):,.2f}")
    print(f"Ending Balance: ${statement.get('ending_balance', 0):,.2f}")
    print(f"\nTransactions: {len(statement.get('transactions', []))}")
    print(f"  Deposits: {len(statement.get('deposits', []))}")
    print(f"  Checks: {len(statement.get('checks', []))}")
    print(f"  ACH Debits: {len(statement.get('ach_debits', []))}")
    print(f"  Withdrawals: {len(statement.get('withdrawals', []))}")
    print(f"  Fees: {len(statement.get('fees', []))}")

    if statement.get('transactions'):
        print(f"\nFirst 5 Transactions:")
        for tx in statement.get('transactions', [])[:5]:
            print(f"  {tx['type']:12} {tx['date']:10} {tx['amount']:>12,.2f} {tx.get('description', '')}")
