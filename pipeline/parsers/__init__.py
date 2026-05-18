"""
GA Automation Pipeline — Parsers
================================
Each parser reads one source file type, validates it, and produces
a normalized data structure the rest of the pipeline can work with.

Parsers:
  yardi_gl                  – General Ledger Detail (backbone of work papers)
  yardi_income_statement    – Income Statement (Accrual)
  yardi_budget_comparison   – Budget vs. Actual Comparison
  nexus_accrual             – Nexus Invoice / Accrual Detail
  pnc_bank_statement        – PNC Bank Statement (PDF)
  berkadia_loan             – Berkadia Loan Servicer Statements
  kardin_budget             – Kardin Annual Budget (reference)
  monthly_report_template   – 8-tab Singerman Deliverable (target format)
"""

from . import yardi_gl
from . import yardi_income_statement
from . import yardi_budget_comparison
from . import nexus_accrual
from . import pnc_bank_statement
from . import berkadia_loan
from . import kardin_budget
from . import monthly_report_template

__all__ = [
    'yardi_gl',
    'yardi_income_statement',
    'yardi_budget_comparison',
    'nexus_accrual',
    'pnc_bank_statement',
    'berkadia_loan',
    'kardin_budget',
    'monthly_report_template',
]

__version__ = '2.0.0'
