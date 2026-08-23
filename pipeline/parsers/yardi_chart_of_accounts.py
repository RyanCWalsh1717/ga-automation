"""
Yardi Chart of Accounts Parser
=================================
Parses a Yardi Chart of Accounts export (.xlsx) into a code -> name lookup,
used by the QC engine to flag any GL account code that doesn't appear on
the chart on file (a new Yardi account that was never added to the COA).

Report layout (Report1 sheet):
  Row 1:    Title — 'Chart of Accounts'
  Rows 5-6: Column headers (wrapped across two rows) —
            Account | Account Name | Normal Balance | Acct Type | Rpt Type |
            Margin | Line Adv. | Offset Account
  Data rows: one row per account, e.g.
            ('111100', 'Cash - Operating', 'Debit', 'Reg', 'Bal', 2, 1, None)
            ('111000', 'CASH & CASH EQUIVALENTS', 'Debit', 'Head', 'Bal', 1, 0, None)
            ('114999', 'TOTAL CASH & CASH EQUIVALENT', 'Debit', 'Tot', 'Bal', 1, 0, None)

Acct Type column:
  'Reg'  — a real, postable GL account (what actually shows up on a GL export)
  'Head' — section header (e.g. 'CASH & CASH EQUIVALENTS') — not postable
  'Tot'  — subtotal row (e.g. 'TOTAL CASH & CASH EQUIVALENT') — not postable

Only 'Reg' rows are real accounts a GL transaction could reference, so those
are what gets returned as the known-account lookup.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ChartOfAccountsResult:
    """Output of parsing a Yardi Chart of Accounts export."""
    accounts:     Dict[str, str] = field(default_factory=dict)  # code -> name, Acct Type == 'Reg' only
    total_rows:   int = 0
    _parse_error: Optional[str] = None


def parse(filepath: str) -> ChartOfAccountsResult:
    """Parse a Yardi Chart of Accounts .xlsx file."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        accounts: Dict[str, str] = {}
        total = 0
        for row in ws.iter_rows(values_only=True):
            if not row or row[0] is None:
                continue
            code = str(row[0]).strip()
            if not code.isdigit():
                continue   # skip title/header/blank rows
            name = str(row[1] or '').strip() if len(row) > 1 else ''
            acct_type = str(row[3] or '').strip() if len(row) > 3 else ''
            total += 1
            if acct_type == 'Reg':
                accounts[code] = name
        return ChartOfAccountsResult(accounts=accounts, total_rows=total)
    except Exception as exc:
        return ChartOfAccountsResult(_parse_error=str(exc))
