"""
Yardi Budget Comparison (Accrual) Parser

Parses Yardi Budget Comparison export files comparing actual vs budgeted amounts:
- Rows 1-4: Meta information (property, report type, period, book/tree)
- Row 5: Column headers with PTD, YTD, and Annual metrics
- Row 6+: Hierarchical account data with actuals and budget comparisons

Expected columns:
  Account Code, Account Name, PTD Actual, PTD Budget, PTD Variance, PTD % Var,
  YTD Actual, YTD Budget, YTD Variance, YTD % Var, Annual

The account hierarchy is indicated by spacing in the Account Name column.

Features:
- Handles hierarchical GL accounts with indentation
- Extracts Period-to-Date (PTD) and Year-to-Date (YTD) comparisons
- Calculates and preserves variance metrics
- Handles 'N/A' values in percentage columns
- Normalizes numeric values
"""

from openpyxl import load_workbook
from datetime import datetime
from typing import List, Dict, Tuple, Optional


def parse(filepath: str) -> List[Dict]:
    """
    Parse a Yardi Budget Comparison export file.

    Args:
        filepath: Path to the Excel file

    Returns:
        List of dictionaries representing budget comparison line items

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If file structure is invalid
    """
    try:
        wb = load_workbook(filepath)
    except Exception as e:
        raise FileNotFoundError(f"Cannot open file: {filepath}") from e

    ws = wb.active
    data = []

    # Meta information is in rows 1-4
    metadata = _extract_metadata(ws)

    # Row 5 contains column headers
    headers = _extract_headers(ws, row=5)

    if not headers:
        raise ValueError("Cannot extract headers from Budget Comparison file")

    # Process data rows starting from row 6
    for row_num in range(6, ws.max_row + 1):
        row = ws[row_num]
        row_values = [cell.value for cell in row]

        # Skip completely empty rows
        if all(v is None for v in row_values):
            continue

        # Account code should be in first column
        account_code = row_values[0]
        account_name = row_values[1] if len(row_values) > 1 else None

        # Skip rows without an account code
        if account_code is None:
            continue

        # Build record
        record = {
            'account_code': _normalize_value(account_code),
            'account_name': _normalize_value(account_name),
        }

        # Extract values in order: PTD Actual, PTD Budget, PTD Variance, PTD % Var,
        #                          YTD Actual, YTD Budget, YTD Variance, YTD % Var, Annual
        column_map = [
            (2, 'ptd_actual'),
            (3, 'ptd_budget'),
            (4, 'ptd_variance'),
            (5, 'ptd_percent_var'),
            (6, 'ytd_actual'),
            (7, 'ytd_budget'),
            (8, 'ytd_variance'),
            (9, 'ytd_percent_var'),
            (10, 'annual'),
        ]

        for col_idx, col_name in column_map:
            if col_idx < len(row_values):
                value = row_values[col_idx]
                # Percentage columns may have 'N/A'
                if col_name.endswith('_var'):
                    record[col_name] = _normalize_flexible_numeric(value)
                else:
                    record[col_name] = _normalize_numeric(value)

        # Add metadata
        record.update(metadata)

        data.append(record)

    return data


def validate(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate that a file has the expected Budget Comparison structure.

    Args:
        filepath: Path to the Excel file

    Returns:
        Tuple of (is_valid: bool, issues: list of error strings)
    """
    issues = []

    try:
        wb = load_workbook(filepath)
    except Exception as e:
        return False, [f"Cannot open file: {e}"]

    ws = wb.active

    # Check for expected meta rows
    if not ws.cell(1, 1).value or "Property" not in str(ws.cell(1, 1).value):
        issues.append("Row 1 missing 'Property' meta information")

    if not ws.cell(2, 1).value or "Budget Comparison" not in str(ws.cell(2, 1).value):
        issues.append("Row 2 missing 'Budget Comparison' title")

    # Check headers
    headers = _extract_headers(ws, row=5)
    if not headers:
        issues.append("Cannot extract headers from row 5")
    else:
        # Should have at least Account Code and Account Name
        if len(headers) < 2:
            issues.append("Expected at least 2 header columns")

    return len(issues) == 0, issues


def _extract_metadata(ws) -> Dict:
    """Extract metadata from rows 1-4."""
    metadata = {}

    # Row 1: Property
    prop_line = ws.cell(1, 1).value
    if prop_line:
        parts = str(prop_line).split('=')
        if len(parts) > 1:
            metadata['property'] = parts[1].strip()

    # Row 2: Report type
    report_line = ws.cell(2, 1).value
    if report_line:
        metadata['report_type'] = str(report_line).strip()

    # Row 3: Period
    period_line = ws.cell(3, 1).value
    if period_line:
        parts = str(period_line).split('=')
        if len(parts) > 1:
            metadata['period'] = parts[1].strip()

    # Row 4: Book/Tree
    book_line = ws.cell(4, 1).value
    if book_line:
        parts = str(book_line).split(';')
        for part in parts:
            part = part.strip()
            if '=' in part:
                key, val = part.split('=', 1)
                metadata[key.strip().lower()] = val.strip()

    return metadata


def _extract_headers(ws, row: int) -> List[str]:
    """Extract and clean headers from a specific row."""
    headers = []
    for cell in ws[row]:
        value = cell.value
        if value:
            headers.append(str(value).strip())
        else:
            headers.append(None)
    return headers


def _normalize_value(value):
    """Normalize values for consistent output."""
    if value is None:
        return None

    # Convert datetime to ISO format string
    if isinstance(value, datetime):
        return value.isoformat()

    # Handle strings - strip whitespace
    if isinstance(value, str):
        return value.strip()

    return value


def _normalize_numeric(value):
    """Normalize numeric values, handling None and strings."""
    if value is None:
        return None

    # Try to convert to float
    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        try:
            # Try int first
            if '.' not in value:
                return int(value)
            return float(value)
        except (ValueError, AttributeError):
            return None

    return value


def _normalize_flexible_numeric(value):
    """
    Normalize numeric values, but allow 'N/A' and other special strings.
    Used for percentage variance columns that may contain 'N/A'.
    """
    if value is None:
        return None

    # Try to convert to float
    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        value_upper = value.upper().strip()
        # Preserve 'N/A' and similar special values
        if value_upper in ('N/A', 'NA', '#DIV/0!', 'ERROR'):
            return value_upper

        try:
            # Try int first
            if '.' not in value:
                return int(value)
            return float(value)
        except (ValueError, AttributeError):
            return None

    return value


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Usage: python yardi_budget_comparison.py <filepath>")
        sys.exit(1)

    filepath = sys.argv[1]

    # Validate
    is_valid, issues = validate(filepath)
    if not is_valid:
        print(f"Validation errors:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)

    # Parse
    data = parse(filepath)
    print(f"Successfully parsed {len(data)} budget comparison line items")
    print(f"\nSample records (first 2 entries):")
    for i, record in enumerate(data[:2]):
        print(f"\nRecord {i+1}:")
        print(json.dumps(record, indent=2, default=str))
