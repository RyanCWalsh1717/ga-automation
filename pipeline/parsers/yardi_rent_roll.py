"""
Yardi Rent Roll Parser

Parses Yardi Rent Roll (Tenancy Schedule) export files with unit-level lease data:
- Row 1: Report title "Tenancy Schedule II"
- Row 2: Property, AsOf date, and notes
- Row 3+: Column headers (spread across multiple rows for multi-level headers)
- Row 7+: Lease and unit data (may have continuation rows for rent steps)

Expected main columns:
  Property, Building, Floor, Unit Code, Unit Type, Unit Area, Lease, Customer,
  Lease From, Lease To, Term, Tenancy, Lease Area, Annual Rent, Annual Rent/Area,
  Lease Type, LOC Amount, Rent, Start Date, Unit, Area Label, Area, Rent Step,
  Monthly, Rent Step, Annual, Management Fee, Annual Gross, Recov. Type, Base Yr,
  Base Amt

Features:
- Handles multi-row header structure
- Parses unit/lease information
- Handles rent step continuation rows
- Extracts dates and converts to ISO format
- Normalizes numeric values
- Groups rent steps by lease unit
"""

from openpyxl import load_workbook
from datetime import datetime
from typing import List, Dict, Tuple, Optional


def parse(filepath: str) -> List[Dict]:
    """
    Parse a Yardi Rent Roll export file.

    Args:
        filepath: Path to the Excel file

    Returns:
        List of dictionaries representing lease/unit records

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

    # Meta information
    metadata = _extract_metadata(ws)

    # Extract headers from row 3 (main headers)
    headers = _extract_headers(ws, row=3)

    if not headers:
        raise ValueError("Cannot extract headers from Rent Roll file")

    # Process data rows starting from row 7
    # Note: row 6 contains property header, so actual unit data starts at 7
    current_unit = None

    for row_num in range(7, ws.max_row + 1):
        row = ws[row_num]
        row_values = [cell.value for cell in row]

        # Skip completely empty rows
        if all(v is None for v in row_values):
            continue

        # Check if this is a new unit record (column 1 has Property info)
        if row_values[0] is not None and str(row_values[0]).startswith("Revolution Labs"):
            # This is a primary unit row
            record = _build_unit_record(headers, row_values, metadata)
            if record:
                data.append(record)
                current_unit = record
        elif row_values[0] is None and any(v is not None for v in row_values):
            # This is a continuation row (rent step)
            # Try to extract rent step data and append to current unit
            if current_unit is not None:
                # Extract rent step info from this row
                rent_step_data = _extract_rent_step_data(headers, row_values)
                if rent_step_data:
                    # Add as separate rent step record linked to the lease
                    step_record = dict(current_unit)
                    step_record.update(rent_step_data)
                    data.append(step_record)

    return data


def validate(filepath: str) -> Tuple[bool, List[str]]:
    """
    Validate that a file has the expected Rent Roll structure.

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

    # Check for expected title row
    if not ws.cell(1, 1).value or "Tenancy Schedule" not in str(ws.cell(1, 1).value):
        issues.append("Row 1 missing 'Tenancy Schedule II' title")

    # Check for property header in row 2
    if not ws.cell(2, 1).value or "Property" not in str(ws.cell(2, 1).value):
        issues.append("Row 2 missing property information")

    # Check headers
    headers = _extract_headers(ws, row=3)
    if not headers:
        issues.append("Cannot extract headers from row 3")
    else:
        # Should have at least Property, Unit Code, Customer
        if len(headers) < 3:
            issues.append("Expected at least 3 header columns")

    return len(issues) == 0, issues