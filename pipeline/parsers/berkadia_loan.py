"""
Berkadia Loan Servicer Statement Parser

This parser extracts data from Berkadia loan servicer statements in Excel format.
The statements are formatted as PDF-like layouts (not tables) with scattered data
across specific cells.

Expected file formats:
- Loan.xlsx: Contains 3 sheets (Note A1, Note B1, Mezz)
- Loan2.xlsx: Contains 1 sheet (Note B1)
- Loan3.xlsx: Contains 1 sheet (Mezz/loan 1159012)

Each statement contains:
- Property name and loan number
- Interest rate
- Balance information (principal, escrow balances)
- Payment information (breakdown by component)
- Account activity (transactions with dates and amounts)
- Payment due date and amount due
"""

from openpyxl import load_workbook
from datetime import datetime
from typing import Dict, List, Tuple, Any


def parse(filepath: str) -> List[Dict[str, Any]]:
    """
    Parse Berkadia loan servicer statements from Excel file.

    Args:
        filepath: Path to the Excel file

    Returns:
        List of dictionaries, one per loan/sheet, containing extracted data
    """
    results = []
    wb = load_workbook(filepath)

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        loan_data = _parse_sheet(ws)
        if loan_data:
            results.append(loan_data)

    return results


def _parse_sheet(ws) -> Dict[str, Any]:
    """
    Parse a single sheet from the workbook.

    Args:
        ws: Worksheet object

    Returns:
        Dictionary containing extracted loan data
    """
    data = {}

    # Extract basic loan information
    data['property_name'] = ws['D1'].value or ws['D3'].value
    data['loan_number'] = ws['P5'].value or ws['Q3'].value
    data['interest_rate'] = ws['X5'].value or ws['V3'].value

    # Extract balance information (as of date)
    data['as_of_date'] = _extract_date_from_cell(ws['A7'].value)
    data['principal_balance'] = ws['G8'].value
    data['interest_paid_ytd'] = ws['G9'].value
    data['outstanding_deferred_int'] = ws['A10'].value  # May be in different location
    data['tax_escrow_balance'] = ws['G11'].value
    data['insurance_escrow_balance'] = ws['G12'].value
    data['reserve_balance'] = ws['G13'].value

    # Extract payment information (for date)
    data['payment_due_date'] = _extract_date_from_cell(ws['K7'].value)

    # Extract payment breakdown
    data['payment_breakdown'] = {}
    data['payment_breakdown']['principal'] = ws['K8'].value
    data['payment_breakdown']['interest'] = ws['T9'].value
    data['payment_breakdown']['taxes'] = ws['T10'].value
    data['payment_breakdown']['insurance'] = ws['K12'].value
    data['payment_breakdown']['reserves'] = ws['K13'].value

    # Total payment due
    total_text = ws['P16'].value
    data['total_payment_due'] = _extract_amount_from_text(total_text)

    # Extract account activity
    data['account_activity'] = _extract_account_activity(ws)

    # Last payment information
    data['last_payment_date'] = ws['A29'].value
    data['due_date'] = ws['I29'].value
    data['amount_due'] = ws['R29'].value

    return data


def _extract_account_activity(ws) -> List[Dict[str, Any]]:
    """
    Extract account activity transactions from the worksheet.

    Args:
        ws: Worksheet object

    Returns:
        List of transaction dictionaries
    """
    transactions = []

    # Account activity typically starts at row 20
    current_row = 20
    while True:
        date_cell = ws[f'A{current_row}'].value
        if date_cell is None or not isinstance(date_cell, (datetime, str)):
            break

        desc_cell = ws[f'C{current_row}'].value
        if desc_cell is None:
            current_row += 1
            continue

        transaction = {
            'date': date_cell if isinstance(date_cell, datetime) else None,
            'description': desc_cell,
            'total': ws[f'F{current_row}'].value,
            'principal': ws[f'G{current_row}'].value,
            'interest': ws[f'O{current_row}'].value,
            'escrow': ws[f'O{current_row}'].value,
            'reserves': ws[f'T{current_row}'].value,
            'late_fee': ws[f'V{current_row}'].value,
            'other': ws[f'X{current_row}'].value,
        }

        # Only add if there's at least a date and description
        if transaction['date'] or transaction['description']:
            transactions.append(transaction)

        current_row += 1
        if current_row > 25:  # Stop searching after reasonable range
            break

    return transactions


def _extract_date_from_cell(cell_value: Any) -> datetime:
    """
    Extract date from cell value (handles various formats).

    Args:
        cell_value: Cell value that may contain date information

    Returns:
        Datetime object or None
    """
    if isinstance(cell_value, datetime):
        return cell_value
    if isinstance(cell_value, str):
        # Try to parse if it contains a date pattern
        if '/' in cell_value or '-' in cell_value:
            try:
                return datetime.fromisoformat(cell_value.split()[0])
            except:
                pass
    return None


def _extract_amount_from_text(text: str) -> float:
    """
    Extract numeric amount from text (handles currency formatting).

    Args:
        text: Text that may contain dollar amount

    Returns:
        Float amount or None
    """
    if text is None:
        return None

    import re
    # Extract all numbers and decimal points
    match = re.search(r'[\$]?\s*([\d,]+\.?\d*)', str(text))
    if match:
        amount_str = match.group(1).replace(',', '')
        try:
            return float(amount_str)
        except:
            pass
    return None


def validate(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate that the file is a properly formatted Berkadia loan statement.

    Args:
        filepath: Path to the Excel file

    Returns:
        Tuple of (is_valid: bool, issues: list of error messages)
    """
    issues = []

    try:
        wb = load_workbook(filepath)
    except Exception as e:
        return False, [f"Failed to open Excel file: {str(e)}"]

    if not wb.sheetnames:
        issues.append("Workbook contains no sheets")
        return False, issues

    # Check first sheet for required fields
    ws = wb[wb.sheetnames[0]]

    # Check for property name
    if not (ws['D1'].value or ws['D3'].value):
        issues.append("Missing property name (expected in D1 or D3)")

    # Check for loan number
    if not (ws['P5'].value or ws['Q3'].value):
        issues.append("Missing loan number (expected in P5 or Q3)")

    # Check for principal balance
    if not ws['G8'].value:
        issues.append("Missing principal balance (expected in G8)")

    # Check for payment due amount
    if not ws['P16'].value and not ws['Q9'].value:
        issues.append("Missing total payment due (expected in P16 or Q9)")

    return len(issues) == 0, issues


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python berkadia_loan.py <filepath>")
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
    loans = parse(filepath)

    # Print summary
    print(f"\n{'='*80}")
    print(f"Berkadia Loan Statement Parser - {filepath.split('/')[-1]}")
    print(f"{'='*80}\n")

    for idx, loan in enumerate(loans, 1):
        principal = loan.get('principal_balance') or 0
        tax_escrow = loan.get('tax_escrow_balance') or 0
        ins_escrow = loan.get('insurance_escrow_balance') or 0
        reserve = loan.get('reserve_balance') or 0
        total_due = loan.get('total_payment_due') or 0

        print(f"Loan {idx}:")
        print(f"  Property: {loan.get('property_name')}")
        print(f"  Loan #: {loan.get('loan_number')}")
        print(f"  Interest Rate: {loan.get('interest_rate')}")
        print(f"  As of Date: {loan.get('as_of_date')}")
        print(f"  Principal Balance: ${principal:,.2f}")
        print(f"  Tax Escrow: ${tax_escrow:,.2f}")
        print(f"  Insurance Escrow: ${ins_escrow:,.2f}")
        print(f"  Reserve Balance: ${reserve:,.2f}")
        print(f"  Payment Due Date: {loan.get('payment_due_date')}")
        print(f"  Total Payment Due: ${total_due:,.2f}")
        print(f"  Transactions: {len(loan.get('account_activity', []))}")
        print()
