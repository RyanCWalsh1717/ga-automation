"""
Management Fee Calculator — GRP / Revolution Labs
===================================================
Computes the monthly management fee accrual based on cash received during
the period and the agreed fee rates:

  JLL (current PM):  1.25% of cash received
  GRP (replacement): 1.75% of cash received
  Total:             3.00% of cash received

"Cash received" = gross tenant receipts per the Yardi Receivable Summary (or
Detail) report, net of Prepayment receipts (advance deposits that are not
earned income).  JLL uses this same basis — both reports are driven by Yardi's
bank reconciliation, which must be completed first.

The pipeline derives cash received from one of six priority tiers, in order:

  1. Yardi Receivable Summary report — preferred (explicit Prepayment row makes
     exclusion unambiguous; no charge-code scanning required)
  2. Yardi Receivable Detail + AR Detail Aging — alternate; JLL's exact method;
     AR Aging Pre-payments column provides the most reliable prepayment exclusion
  3. Yardi Receivable Detail only — AR Aging not uploaded; falls back to
     charge-code scan built into ReceivableDetailResult (less reliable for
     cross-tenant netting scenarios)
  4. DACA bank statement additions — fallback when neither Receivable report
     is uploaded (matches JLL's basis when Yardi bank rec not yet run)
  5. GL operating cash account (111100) — debit transactions for the period
  6. Budget Comparison revenue accounts — PTD Actual of income lines as a proxy

The result is consumed by:
  - qc_engine.py check_7_misc (to verify the accrued fee vs. expected)
  - app.py (to display the fee breakdown in the results dashboard)
  - report_generator.py (to populate the Accruals tab)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from accounting_utils import _round


# ── Period formatting ──────────────────────────────────────────────────────────

_MONTH_ABBR = {
    'jan': 'January', 'feb': 'February', 'mar': 'March',
    'apr': 'April',   'may': 'May',       'jun': 'June',
    'jul': 'July',    'aug': 'August',    'sep': 'September',
    'oct': 'October', 'nov': 'November',  'dec': 'December',
}


def _fmt_period(period: str) -> str:
    """
    Format an accounting period string as 'Month YYYY' for use in descriptions.

    Accepts:
      'Apr-2026'  → 'April 2026'
      '04/2026'   → 'April 2026'
      'April 2026' → 'April 2026' (pass-through)

    Falls back to the raw string if the format is unrecognised.
    """
    if not period:
        return period
    # 'Apr-2026' or 'Apr 2026'
    m = re.match(r'([A-Za-z]{3,})[- ](\d{4})', period.strip())
    if m:
        month_key = m.group(1)[:3].lower()
        year = m.group(2)
        return f"{_MONTH_ABBR.get(month_key, m.group(1).capitalize())} {year}"
    # '04/2026' or '04-2026'
    m = re.match(r'(\d{1,2})[/\-](\d{4})', period.strip())
    if m:
        month_num = int(m.group(1))
        year = m.group(2)
        month_names = list(_MONTH_ABBR.values())
        if 1 <= month_num <= 12:
            return f"{month_names[month_num - 1]} {year}"
    return period


# ── Account codes (defaults — overridden by PropertyConfig when provided) ──────
_CASH_OPERATING = '111100'   # Cash - Operating
_MGMT_FEE_CODE  = '637130'   # Admin-Management Fees

# Revenue accounts whose PTD actuals count as gross receipts
_REVENUE_PREFIXES = ('4',)   # 4xxxxx = revenue accounts

# Legacy rate constants — kept for backward compatibility only.
# New callers should pass a PropertyConfig; compute_management_fee() reads
# rates from cfg.management_fees when cfg is provided.
JLL_RATE = 0.0125   # 1.25%
GRP_RATE = 0.0175   # 1.75%


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ManagementFeeResult:
    """Output of the management fee calculation."""
    cash_received:       float          # Net cash receipts used as fee basis
    cash_source:         str            # 'receivable_detail+ar_aging' | 'receivable_detail' | 'daca_additions' | ...
                                       # Note: when AR Aging is uploaded, prepayment exclusion = max(ar_aging, scan)
    prepayment_excluded: float = 0.0   # Amount subtracted as prepayments (from AR Aging or charge-code scan)

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
        src_label = {
            'receivable_summary':          'Receivable Summary',
            'receivable_detail+ar_aging':  'Receivable Detail (ex-Prepayments via AR Aging)',
            'receivable_detail':           'Receivable Detail (ex-Prepayments)',
            'daca_additions':              'DACA Additions',
            'gl_cash_account':             'GL 111100 Debits',
            'revenue_proxy':               'Revenue Proxy',
            'not_available':               'Not Available',
        }.get(self.cash_source, self.cash_source)
        return [
            f'Cash Received ({src_label}):  ${self.cash_received:>14,.2f}',
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

def _cash_from_receivable_summary(rs_parsed) -> tuple:
    """
    Read net cash received from the Yardi Receivable Summary report.

    Returns (net_cash: Optional[float], prepayment_excluded: float).

    Prepayment exclusion — uses the dedicated Prepayment row in the Summary:
      Negative Prepayment receipt = new cash received as a prepayment → exclude.
      Positive Prepayment receipt = prior credit applied to a charge → already
        washed into Grand Total, nothing to exclude.

    This is the simplest and most reliable prepayment detection method because
    Yardi isolates prepayments in their own charge-code row, so no charge-code
    scanning or cross-tenant netting concerns apply.

    Returns (None, 0.0) if the report was not parsed or total receipts are zero.
    """
    if rs_parsed is None:
        return None, 0.0

    if hasattr(rs_parsed, 'total_receipts'):
        total = float(rs_parsed.total_receipts or 0)
        prepay = float(rs_parsed.prepayment_receipts or 0)
        net = float(rs_parsed.net_receipts or 0)
    elif isinstance(rs_parsed, dict):
        total = float(rs_parsed.get('total_receipts', 0) or 0)
        prepay = float(rs_parsed.get('prepayment_receipts', 0) or 0)
        net = float(rs_parsed.get('net_receipts', 0) or 0)
    else:
        return None, 0.0

    if total <= 0:
        return None, 0.0

    return (net if net > 0 else None), prepay


def _cash_from_receivable_detail(rd_parsed, ar_aging=None) -> tuple:
    """
    Read net cash received from the Yardi Receivable Detail report.

    Returns (net_cash: Optional[float], prepayment_excluded: float).

    Prepayment exclusion — uses the MAXIMUM of two independent sources:

      1. AR Aging Grand Total Pre-payments column (when uploaded)
         Authoritative Yardi balance, but reports the NET of all tenant
         pre-payment column values.  If one tenant's applied prepayment
         (negative) offsets another's new prepayment (positive), the net
         understates the true amount to exclude.
         Example: Santi +$100K new prepayment, Keros −$50K applied Dec
         prepayment → AR Aging Grand Total = $50K (under-excludes $50K).

      2. Charge-code scan built into ReceivableDetailResult.prepayment_receipts
         Scans C-XXXX rows for 'prepay' charge codes and sums ABS per tenant
         independently.  Not affected by cross-tenant netting.
         Example above → scan gives $100K (Santi) + $50K (Keros) = $150K.

      Taking max() ensures we exclude at least as much as either source
      found.  In the normal case (no cross-tenant netting) both sources
      should agree; using max() is conservative and safe.

    Returns (None, 0.0) if the report was not parsed or total receipts are zero.
    """
    if rd_parsed is None:
        return None, 0.0

    # Get total receipts (gross, before prepayment exclusion)
    if hasattr(rd_parsed, 'total_receipts'):
        total = float(rd_parsed.total_receipts or 0)
    elif isinstance(rd_parsed, dict):
        total = float(rd_parsed.get('total_receipts', 0) or 0)
    else:
        return None, 0.0

    if total <= 0:
        return None, 0.0

    # Prepayment exclusion — collect both sources and take the maximum
    ar_prepay = 0.0
    scan_prepay = 0.0

    if ar_aging is not None and hasattr(ar_aging, 'prepayment_balance'):
        ar_prepay = float(ar_aging.prepayment_balance or 0)

    if hasattr(rd_parsed, 'prepayment_receipts'):
        scan_prepay = float(rd_parsed.prepayment_receipts or 0)
    elif isinstance(rd_parsed, dict):
        scan_prepay = float(rd_parsed.get('prepayment_receipts', 0) or 0)

    # max() handles cross-tenant netting: charge-code scan wins when AR Aging
    # net under-states true prepayments; AR Aging wins when scan misses a
    # prepayment not flagged by its charge code.
    prepay = max(ar_prepay, scan_prepay)

    net = max(0.0, total - prepay)
    return (net if net > 0 else None), prepay


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
    receivable_detail=None,
    receivable_summary=None,
    ar_aging=None,
    jll_rate: float = None,
    grp_rate: float = None,
    property_config=None,
) -> ManagementFeeResult:
    """
    Compute the management fee accrual for the period.

    Priority:
      1. Yardi Receivable Summary — preferred; explicit Prepayment row, cleanest exclusion
      2. Yardi Receivable Detail — alternate (JLL's exact method, excludes prepayments via
         AR Aging Pre-payments column if uploaded, else charge-code scan)
      3. DACA bank statement additions — fallback when no Receivable report uploaded
      4. GL operating cash account debit total — fallback when no DACA file
      5. Revenue account PTD actuals from budget comparison — last resort proxy

    Args:
        gl_parsed:          GLParseResult from yardi_gl.parse_gl()
        budget_rows:        List of BC row dicts from yardi_budget_comparison.parse()
        manual_override:    Deprecated — no longer used (kept for signature compatibility)
        daca_parsed:        Parsed KeyBank DACA statement dict
        receivable_detail:  ReceivableDetailResult from parsers.yardi_receivable_detail.parse()
        receivable_summary: ReceivableSummaryResult from parsers.yardi_receivable_summary.parse()
                            When uploaded, takes priority over the Receivable Detail.
                            Prepayment exclusion is read directly from the Prepayment row:
                            negative receipt = new cash to exclude; positive = applied credit.
        ar_aging:           ARAgingResult from parsers.yardi_ar_aging.parse() — used only when
                            falling back to Receivable Detail (Summary makes it unnecessary).
                            Both AR Aging and charge-code scan are evaluated; larger wins.
        jll_rate:           JLL management fee rate (default 1.25%)
        grp_rate:           GRP management fee rate (default 1.75%)

    Returns:
        ManagementFeeResult
    """
    budget_rows = budget_rows or []

    # Resolve rates: explicit args > property_config > module-level defaults
    if jll_rate is None:
        jll_rate = property_config.management_fee_jll_rate if property_config else JLL_RATE
    if grp_rate is None:
        grp_rate = property_config.management_fee_grp_rate if property_config else GRP_RATE

    # Resolve key GL accounts from config when available
    _cash_acct = (
        property_config.gl_account('cash_operating', _CASH_OPERATING)
        if property_config else _CASH_OPERATING
    )
    _fee_acct = (
        property_config.gl_account('mgmt_fee_expense', _MGMT_FEE_CODE)
        if property_config else _MGMT_FEE_CODE
    )

    # 1. Receivable Summary — preferred (explicit Prepayment row, no scanning required)
    rs_cash, rs_prepay = _cash_from_receivable_summary(receivable_summary)
    if rs_cash is not None:
        return ManagementFeeResult(
            cash_received=rs_cash,
            cash_source='receivable_summary',
            prepayment_excluded=rs_prepay,
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 2. Receivable Detail — alternate (JLL's exact method, excludes prepayments)
    rd_cash, prepay_excl = _cash_from_receivable_detail(receivable_detail, ar_aging)
    if rd_cash is not None:
        src = 'receivable_detail+ar_aging' if ar_aging is not None else 'receivable_detail'
        return ManagementFeeResult(
            cash_received=rd_cash,
            cash_source=src,
            prepayment_excluded=prepay_excl,
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 3. DACA additions — fallback (matches JLL's basis when bank rec not yet run)
    daca_cash = _cash_from_daca(daca_parsed)
    if daca_cash is not None:
        return ManagementFeeResult(
            cash_received=daca_cash,
            cash_source='daca_additions',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 4. GL cash account — fallback when neither Receivable report nor DACA uploaded
    gl_cash = _cash_from_gl(gl_parsed)
    if gl_cash is not None:
        return ManagementFeeResult(
            cash_received=gl_cash,
            cash_source='gl_cash_account',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 5. Revenue proxy
    rev_cash = _cash_from_revenue(budget_rows)
    if rev_cash is not None:
        return ManagementFeeResult(
            cash_received=rev_cash,
            cash_source='revenue_proxy',
            jll_rate=jll_rate,
            grp_rate=grp_rate,
        )

    # 6. Nothing available — return $0 with a note
    return ManagementFeeResult(
        cash_received=0.0,
        cash_source='not_available',
        jll_rate=jll_rate,
        grp_rate=grp_rate,
    )


def build_management_fee_je(
    fee_result: ManagementFeeResult,
    period: str = '',
    property_code: str = 'revlabspm',
    ap_account: str = '213100',
    ap_account_name: str = 'Accrued Management Fees',
    je_number: str = 'MGT-001',
    property_config=None,
) -> list[dict]:
    """
    Build the journal entry lines for the management fee accrual.

    When property_config is provided, iterates over cfg.management_fees to
    produce one balanced DR/CR pair per fee line — supporting any number of
    PM arrangements (single PM, JLL+GRP, or future structures).

    Fallback (no config): two pairs — JLL (fee_result.jll_rate) and GRP
    (fee_result.grp_rate) — preserving existing behavior.

    Both/all pairs share je_number so they import as a single Yardi batch.
    """
    if fee_result.cash_received <= 0:
        return []

    cash = fee_result.cash_received
    _period_label = _fmt_period(period)
    lines = []
    line_num = 1

    # ── Config-driven path: iterate fee lines ─────────────────────────────────
    fee_lines_cfg = getattr(property_config, 'management_fees', None) if property_config else None

    if fee_lines_cfg:
        for fl in fee_lines_cfg:
            if fl.rate <= 0:
                continue
            # Apply minimum: fee = max(rate × cash, minimum)
            computed = fee_result.cash_received * fl.rate
            amt = _round(max(computed, fl.minimum) if fl.minimum > 0 else computed)
            if amt <= 0:
                continue
            desc = (
                f'Accrual {_period_label} — {fl.name} Management Fee '
                f'({fl.rate:.2%} on ${cash:,.2f} cash received)'
            )
            ref = fl.ref_prefix or f'MGMT-FEE-{fl.name.upper()}'
            dr_acct = fl.dr_account or _MGMT_FEE_CODE
            cr_acct = fl.cr_account or ap_account
            lines += [
                {
                    'je_number': je_number, 'line': line_num,
                    'date': period, 'account_code': dr_acct,
                    'account_name': 'Admin-Management Fees',
                    'description': desc, 'reference': ref,
                    'debit': amt, 'credit': 0.0,
                    'vendor': f'{fl.name} Management Fee',
                    'invoice_number': '', 'source': 'management_fee',
                },
                {
                    'je_number': je_number, 'line': line_num + 1,
                    'date': period, 'account_code': cr_acct,
                    'account_name': ap_account_name,
                    'description': desc, 'reference': ref,
                    'debit': 0.0, 'credit': amt,
                    'vendor': f'{fl.name} Management Fee',
                    'invoice_number': '', 'source': 'management_fee',
                },
            ]
            line_num += 2
        return lines

    # ── Legacy fallback: JLL + GRP from fee_result rates ─────────────────────
    jll_amt = _round(fee_result.jll_fee)
    grp_amt = _round(fee_result.grp_fee)

    jll_desc = (
        f'Accrual {_period_label} — JLL Management Fee '
        f'({fee_result.jll_rate:.2%} on ${cash:,.2f} cash received)'
    )
    grp_desc = (
        f'Accrual {_period_label} — GRP Management Fee '
        f'({fee_result.grp_rate:.2%} on ${cash:,.2f} cash received)'
    )

    return [
        {'je_number': je_number, 'line': 1, 'date': period,
         'account_code': _MGMT_FEE_CODE, 'account_name': 'Admin-Management Fees',
         'description': jll_desc, 'reference': 'MGMT-FEE-JLL',
         'debit': jll_amt, 'credit': 0.0,
         'vendor': 'JLL Management Fee', 'invoice_number': '', 'source': 'management_fee'},
        {'je_number': je_number, 'line': 2, 'date': period,
         'account_code': ap_account, 'account_name': ap_account_name,
         'description': jll_desc, 'reference': 'MGMT-FEE-JLL',
         'debit': 0.0, 'credit': jll_amt,
         'vendor': 'JLL Management Fee', 'invoice_number': '', 'source': 'management_fee'},
        {'je_number': je_number, 'line': 3, 'date': period,
         'account_code': _MGMT_FEE_CODE, 'account_name': 'Admin-Management Fees',
         'description': grp_desc, 'reference': 'MGMT-FEE-GRP',
         'debit': grp_amt, 'credit': 0.0,
         'vendor': 'GRP Management Fee', 'invoice_number': '', 'source': 'management_fee'},
        {'je_number': je_number, 'line': 4, 'date': period,
         'account_code': ap_account, 'account_name': ap_account_name,
         'description': grp_desc, 'reference': 'MGMT-FEE-GRP',
         'debit': 0.0, 'credit': grp_amt,
         'vendor': 'GRP Management Fee', 'invoice_number': '', 'source': 'management_fee'},
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

      Month N close  : DR 637130 / CR 213100  (accrual posted)
      Month N+1 Day 1: DR 213100 / CR 637130  (auto-reversal)
      Month N+1      : DR 637130 / CR 213100  (invoice entry) — if check clears
      Month N+1 close: DR 637130 / CR 213100  (current-month new accrual)

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
    property_code: str = 'revlabspm',
    ap_account: str = '213100',
    ap_account_name: str = 'Accrued Expenses',
    je_number: str = 'MGT-002',
) -> list[dict]:
    """
    Build the catch-up journal entry for an unmatched prior-period
    management fee auto-reversal.

    Debit  637130  Admin-Management Fees     (catch-up amount)
    Credit 213100  Accrued Expenses          (catch-up amount)

    This entry offsets the credit left in 637130 by the auto-reversal and
    re-establishes the management fee expense for the prior period.

    Returns list of two JE line dicts in the standard pipeline format.
    """
    if catchup_amount <= 0:
        return []

    _period_label = _fmt_period(period)
    desc = (
        f'Accrual {_period_label} — Management Fee Catch-Up '
        f'(prior month auto-reversal; reinstating ${catchup_amount:,.2f})'
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
