"""
Management Fee Calculator — GRP / Revolution Labs
===================================================
Computes the monthly management fee accrual based on cash received during
the period and the agreed fee rates:

  JLL (current PM):  1.25% of cash received
  GRP (replacement): 1.75% of cash received
  Total:             3.00% of cash received

"Cash received" = gross receipts deposited into the operating bank account
for the period.  The pipeline derives this from one of three sources,
in priority order:

  1. GL operating cash account (111100) — debit transactions for the period
     (debit = cash in, in double-entry terms)
  2. Budget Comparison revenue accounts — PTD Actual of income lines as a proxy
  3. User-supplied override via the Streamlit sidebar

The result is consumed by:
  - qc_engine.py check_7_misc (to verify the accrued fee vs. expected)
  - app.py (to display the fee breakdown in the results dashboard)
  - report_generator.py (to populate the Accruals tab)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ── Account codes ──────────────────────────────────────────────────────────────
_CASH_OPERATING = '111100'   # Cash - Operating (PNC)
_MGMT_FEE_CODE  = '637130'   # Admin-Management Fees (expense line in BC)

# Revenue accounts whose PTD actuals count as gross receipts
_REVENUE_PREFIXES = ('4',)   # 4xxxxx = revenue accounts

# Rate schedule
JLL_RATE = 0.0125   # 1.25%
GRP_RATE = 0.0175   # 1.75%


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ManagementFeeResult:
    """Output of the management fee calculation."""
    cash_received: float          # Gross cash receipts for the period
    cash_source: str              # 'gl_cash_account' | 'revenue_proxy' | 'manual_override'

    jll_rate: float = JLL_RATE
    grp_rate: float = GRP_RATE

    @property
    def jll_fee(self) -> float:
        return self.cash_received * self.jll_rate

    @property
    def grp_fee(self) -> float:
        return self.cash_received * self.grp_rate

    @property
    def total_fee(self) -> float:
        return self.jll_fee + self.grp_fee

    @property
    def total_rate(self) -> float:
        return self.jll_rate + self.grp_rate

    def summary_lines(self) -> list[str]:
        """Human-readable lines for display in dashboard / workbook."""
        return [
            f'Cash Received ({self.cash_source}):  ${self.cash_received:>14,.2f}',
            f'JLL Fee  ({self.jll_rate:.2%}):             ${self.jll_fee:>14,.2f}',
            f'GRP Fee  ({self.grp_rate:.2%}):             ${self.grp_fee:>14,.2f}',
            f'Total Mgmt Fee ({self.total_rate:.2%}):        ${self.total_fee:>14,.2f}',
        ]

    def accrual_description(self) -> str:
        """Short description for the JE accrual entry."""
        return (
            f'Management fee accrual — {self.jll_rate:.2%} JLL + {self.grp_rate:.2%} GRP '
            f'on ${self.cash_received:,.2f} cash received'
        )


# ── Cash-received extraction ───────────────────────────────────────────────────

def _cash_from_gl(gl_parsed) -> Optional[float]:
    """
    Sum debit transactions in the operating cash account (111100).

    In double-entry:  Debit to cash = cash received (money coming in)
                      Credit to cash = cash paid out

    We exclude the beginning-balance entry (Yardi posts it as a debit
    equal to the forward balance on the first row of the account).
    We also exclude same-account transfers (identified by 'transfer' in
    the description) to avoid double-counting.
    """
    if not gl_parsed or not hasattr(gl_parsed, 'accounts'):
        return None

    for acct in gl_parsed.accounts:
        if str(acct.account_code).strip() != _CASH_OPERATING:
            continue

        receipts = 0.0
        for txn in acct.transactions:
            if txn.debit <= 0:
                continue
            desc_lower = (txn.description or '').lower()
            remarks_lower = (txn.remarks or '').lower()
            # Skip internal bank transfers
            if 'transfer' in desc_lower or 'transfer' in remarks_lower:
                continue
            receipts += txn.debit

        return receipts if receipts > 0 else None

    return None


def _cash_from_revenue(budget_rows: list[dict]) -> Optional[float]:
    """
    Sum PTD Actual across all revenue accounts (4xxxxx) as a proxy for
    cash received when GL detail is not available.

    Note: This is an approximation — it treats all accrual-basis revenue as
    cash.  For this property (single-tenant, monthly billing) this is close
    enough for fee verification purposes.
    """
    if not budget_rows:
        return None

    total = 0.0
    found = False
    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        if any(code.startswith(p) for p in _REVENUE_PREFIXES):
            val = row.get('ptd_actual', 0) or 0
            if isinstance(val, (int, float)) and val != 0:
                total += abs(float(val))
                found = True

    return total if found else None


# ── Main entry point ──────────────────────────────────────────────────────────

def calculate(
    gl_parsed=None,
    budget_rows: list[dict] = None,
    manual_override: float = None,
    jll_rate: float = JLL_RATE,
    grp_rate: float = GRP_RATE,
) -> ManagementFeeResult:
    """
    Compute the management fee accrual for the period.

    Priority:
      1. manual_override (if provided and > 0)
      2. GL operating cash account debit total
      3. Revenue account PTD actuals from budget comparison

    Args:
        gl_parsed:       GLParseResult from yardi_gl.parse_gl()
        budget_rows:     List of BC row dicts from yardi_budget_comparison.parse()
        manual_override: If supplied, skip auto-detection and use this number
        jll_rate:        JLL management fee rate (default 1.25%)
        grp_rate:        GRP management fee rate (default 1.75%)

    Returns:
        ManagementFeeResult
    """
    budget_rows = budget_rows or []

    # 1. Manual override
    if manual_override is not None and manual_override > 0:
        return ManagementFeeResult(
            cash_received=float(manual_override),
            cash_source='manual_override',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 2. GL cash account
    gl_cash = _cash_from_gl(gl_parsed)
    if gl_cash is not None:
        return ManagementFeeResult(
            cash_received=gl_cash,
            cash_source='gl_cash_account',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 3. Revenue proxy
    rev_cash = _cash_from_revenue(budget_rows)
    if rev_cash is not None:
        return ManagementFeeResult(
            cash_received=rev_cash,
            cash_source='revenue_proxy',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 4. Nothing available — return $0 with a note
    return ManagementFeeResult(
        cash_received=0.0,
        cash_source='not_available',
        jll_rate=jll_rate,
        grp_rate=grp_rate,
    )


def accrued_fee_from_bc(budget_rows: list[dict]) -> float:
    """
    Read the management fee that was actually accrued in the Budget Comparison.

    Returns the absolute PTD Actual for account 637130.
    """
    for row in budget_rows:
        if str(row.get('account_code', '') or '').strip() == _MGMT_FEE_CODE:
            return abs(float(row.get('ptd_actual', 0) or 0))
    return 0.0
