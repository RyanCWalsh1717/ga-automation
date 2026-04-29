"""
Management Fee Calculator — GRP / Revolution Labs
===================================================
Computes the monthly management fee accrual based on cash received during
the period and the agreed fee rates:

  JLL (current PM):  1.25% of cash received
  GRP (replacement): 1.75% of cash received
  Total:             3.00% of cash received

"Cash received" = gross tenant receipts deposited into the DACA sweep
account for the period.  JLL (the current PM) explicitly uses the DACA
deposit register — not the GL operating cash account — as the management fee
basis.  For March 2026 this was $1,419,011.29 (the "4 Additions" line on the
KeyBank x5132 statement).

The pipeline derives cash received from one of four sources, in priority order:

  1. DACA bank statement additions field — preferred (matches JLL's basis)
  2. GL operating cash account (111100) — debit transactions for the period
     (debit = cash in, in double-entry terms) — fallback when no DACA file
  3. Budget Comparison revenue accounts — PTD Actual of income lines as a proxy
  4. User-supplied override via the Streamlit sidebar

The result is consumed by:
  - qc_engine.py check_7_misc (to verify the accrued fee vs. expected)
  - app.py (to display the fee breakdown in the results dashboard)
  - report_generator.py (to populate the Accruals tab)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from accounting_utils import _round


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

def _cash_from_daca(daca_parsed: dict) -> Optional[float]:
    """
    Read gross additions from the DACA bank statement (KeyBank x5132).

    The DACA parser stores the total deposits as ``additions`` — this is the
    amount JLL uses as the management fee basis (tenant rent receipts swept
    daily from the DACA account into the operating account).

    Returns None if the DACA statement was not parsed or additions is missing.
    """
    if not daca_parsed or not isinstance(daca_parsed, dict):
        return None
    val = daca_parsed.get('additions')
    if val is None or val <= 0:
        return None
    return float(val)


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
    daca_parsed: dict = None,
    jll_rate: float = JLL_RATE,
    grp_rate: float = GRP_RATE,
) -> ManagementFeeResult:
    """
    Compute the management fee accrual for the period.

    Priority:
      1. manual_override (if provided and > 0)
      2. DACA bank statement additions — preferred basis, matches JLL's method
         (tenant rent receipts = gross deposits into KeyBank x5132)
      3. GL operating cash account debit total — fallback when no DACA file
      4. Revenue account PTD actuals from budget comparison — last resort proxy

    Args:
        gl_parsed:       GLParseResult from yardi_gl.parse_gl()
        budget_rows:     List of BC row dicts from yardi_budget_comparison.parse()
        manual_override: If supplied, skip auto-detection and use this number
        daca_parsed:     Parsed KeyBank DACA statement dict (from parsers.keybank_daca.parse)
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

    # 2. DACA additions — preferred (matches JLL's management fee basis)
    daca_cash = _cash_from_daca(daca_parsed)
    if daca_cash is not None:
        return ManagementFeeResult(
            cash_received=daca_cash,
            cash_source='daca_additions',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 3. GL cash account — fallback when DACA statement not uploaded
    gl_cash = _cash_from_gl(gl_parsed)
    if gl_cash is not None:
        return ManagementFeeResult(
            cash_received=gl_cash,
            cash_source='gl_cash_account',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 4. Revenue proxy
    rev_cash = _cash_from_revenue(budget_rows)
    if rev_cash is not None:
        return ManagementFeeResult(
            cash_received=rev_cash,
            cash_source='revenue_proxy',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 5. Nothing available — return $0 with a note
    return ManagementFeeResult(
        cash_received=0.0,
        cash_source='not_available',
        jll_rate=jll_rate,
        grp_rate=grp_rate,
    )


def build_management_fee_je(
    fee_result: ManagementFeeResult,
    period: str = '',
    property_code: str = 'revlabpm',
    ap_account: str = '201000',        # Accrued liabilities — management fees
    ap_account_name: str = 'Accrued Liabilities',
    je_number: str = 'MGT-001',
) -> list[dict]:
    """
    Build the two-line journal entry for the management fee accrual.

    Debit  637130  Admin-Management Fees     (total fee)
    Credit 201000  Accrued Liabilities       (total fee)

    Returns a list of dicts matching the format expected by
    generate_yardi_je_import() in accrual_entry_generator.py.
    """
    if fee_result.cash_received <= 0:
        return []

    desc = fee_result.accrual_description()
    total = fee_result.total_fee

    return [
        {
            'je_number': je_number,
            'line': 1,
            'date': period,
            'account_code': _MGMT_FEE_CODE,
            'account_name': 'Admin-Management Fees',
            'description': desc,
            'reference': 'MGMT-FEE',
            'debit': _round(total),
            'credit': 0.0,
            'vendor': 'Management Fee Accrual',
            'invoice_number': '',
            'source': 'management_fee',
        },
        {
            'je_number': je_number,
            'line': 2,
            'date': period,
            'account_code': ap_account,
            'account_name': ap_account_name,
            'description': desc,
            'reference': 'MGMT-FEE',
            'debit': 0.0,
            'credit': _round(total),
            'vendor': 'Management Fee Accrual',
            'invoice_number': '',
            'source': 'management_fee',
        },
    ]


def accrued_fee_from_bc(budget_rows: list[dict]) -> float:
    """
    Read the management fee that was actually accrued in the Budget Comparison.

    Returns the absolute PTD Actual for account 637130.
    """
    for row in budget_rows:
        if str(row.get('account_code', '') or '').strip() == _MGMT_FEE_CODE:
            return abs(float(row.get('ptd_actual', 0) or 0))
    return 0.0


# ── Prior-period catch-up detection ───────────────────────────────────────────

def detect_prior_period_catchup(gl_data) -> Optional[float]:
    """
    Detect whether the prior month's management fee accrual auto-reversed
    without a matching invoice entry, leaving a net credit in 637130.

    Business context
    ----------------
    GRP's management fee check is cut around the 15th of the month.  If the
    vendor hasn't cashed the check by month-end, the bank close captures it
    as an outstanding item.  Meanwhile, Yardi's accrual cycle runs:

      Month N close  : DR 637130 / CR 211300  (accrual posted)
      Month N+1 Day 1: DR 211300 / CR 637130  (auto-reversal)
      Month N+1      : DR 637130 / CR 211300  (invoice entry) — if check clears
      Month N+1 close: DR 637130 / CR 211300  (current-month new accrual)

    The CURRENT month's new accrual is generated separately by build_management_
    fee_je() and should NOT be factored into this catch-up calculation — it
    represents the current period's fee, not the prior-period shortfall.

    This function looks only at what is ALREADY in the GL:
      - Credits in 637130 = auto-reversals of prior-period accruals
      - Debits  in 637130 = actual invoice postings clearing prior accruals

    If the auto-reversal credit has no matching invoice debit, the net credit
    is the catch-up amount (the prior period's expense was never reinstated).

    Detection
    ---------
    Sum period credits (auto-reversals) and period debits (invoice entries).
    If credits exceed debits by a material amount (> $100) the gap is the
    catch-up amount needed.

    Returns the catch-up amount (positive float) if needed, else None.

    Note: The catch-up JE (MGT-002) and the current-period accrual (MGT-001)
    are independent.  Both will debit 637130: MGT-002 restores the prior-period
    fee; MGT-001 records the current-period fee.  Total DR = catch-up + new fee.
    """
    if not gl_data or not hasattr(gl_data, 'accounts'):
        return None

    for acct in gl_data.accounts:
        if str(acct.account_code).strip() != _MGMT_FEE_CODE:
            continue

        # Sum credits (auto-reversals) and debits (invoice entries) already in GL.
        # The current-period new accrual from build_management_fee_je() is NOT in
        # the GL at this point — it is built and posted as a separate entry (MGT-001).
        # We only look at what Yardi has already recorded.
        period_debits  = sum(float(txn.debit  or 0) for txn in acct.transactions)
        period_credits = sum(float(txn.credit or 0) for txn in acct.transactions)

        # Net credit = auto-reversal exceeded invoice postings → catch-up gap
        net_credit = period_credits - period_debits

        # Return the catch-up amount only when material (> $100)
        if net_credit > 100.0:
            return _round(net_credit)

        return None   # account found but no catch-up needed

    return None  # account not present in GL


def build_catchup_je(
    catchup_amount: float,
    period: str = '',
    property_code: str = 'revlabpm',
    ap_account: str = '211300',
    ap_account_name: str = 'Accrued Expenses',
    je_number: str = 'MGT-002',
) -> list[dict]:
    """
    Build the catch-up journal entry for an unmatched prior-period
    management fee auto-reversal.

    Debit  637130  Admin-Management Fees     (catch-up amount)
    Credit 211300  Accrued Expenses          (catch-up amount)

    This entry offsets the credit left in 637130 by the auto-reversal and
    re-establishes the management fee expense for the prior period.

    Returns list of two JE line dicts in the standard pipeline format.
    """
    if catchup_amount <= 0:
        return []

    desc = (
        f'Management fee catch-up — prior month accrual reversed without '
        f'matching invoice; reinstating ${catchup_amount:,.2f} expense'
    )

    return [
        {
            'je_number':      je_number,
            'line':           1,
            'date':           period,
            'account_code':   _MGMT_FEE_CODE,
            'account_name':   'Admin-Management Fees',
            'description':    desc,
            'reference':      'MGMT-CATCHUP',
            'debit':          _round(catchup_amount),
            'credit':         0.0,
            'vendor':         'Management Fee Catch-up',
            'invoice_number': '',
            'source':         'management_fee_catchup',
        },
        {
            'je_number':      je_number,
            'line':           2,
            'date':           period,
            'account_code':   ap_account,
            'account_name':   ap_account_name,
            'description':    desc,
            'reference':      'MGMT-CATCHUP',
            'debit':          0.0,
            'credit':         _round(catchup_amount),
            'vendor':         'Management Fee Catch-up',
            'invoice_number': '',
            'source':         'management_fee_catchup',
        },
    ]
