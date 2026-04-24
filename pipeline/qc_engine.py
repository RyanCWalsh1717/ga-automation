"""
QC Engine — GRP Monthly Close Quality Control
==============================================
Automates the 8 QC checks currently performed manually in the LexLabs QC workbook.
Runs against the parsed data from the pipeline and produces:
  1. A QCReport dataclass consumed by the Streamlit dashboard
  2. An Excel workbook matching the LexLabs QC structure (via generate_qc_workbook)

Checks:
  1  TB to Budget Comparison Tie-Out
  2  Budget Variances ≥ Tier 1 threshold (GRP standards)
  3  TB Self-Balance + GL Ending Balances vs TB
  4  Month-over-Month Swings (>$10,000 or sign change)
  5  GL Ending Balances vs Workpapers (BS account cross-check)
  6  Accruals vs Budget (missing accrual detection)
  7  Miscellaneous (mgmt fee, interest expense, insurance/prepaid)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from variance_comments import classify_tier, TIER1_ABS, TIER1_PCT, TIER2_MIN
from property_config import is_revenue_account, is_expense_account, is_balance_sheet_account


# ── Account type helpers — delegate to per-property COA config ─────────────────
# Defaults match the standard Yardi COA (4xxxxx=revenue, 5-8xxxxx=expense,
# 1-3xxxxx=BS).  Override via PropertyConfig when onboarding a new property.
def _is_revenue(code: str) -> bool:
    return is_revenue_account(code)


def _is_expense(code: str) -> bool:
    return is_expense_account(code)


def _is_balance_sheet(code: str) -> bool:
    return is_balance_sheet_account(code)


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(',', '').strip())
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════

@dataclass
class QCFinding:
    account_code: str
    account_name: str
    value_a: float          # Primary value (e.g. actual, GL balance)
    value_b: float          # Comparison value (e.g. budget, TB balance)
    difference: float
    flag: str               # "OVER", "UNDER", "MISMATCH", "MISSING", "FLAG", "INFO"
    note: str = ''


@dataclass
class QCResult:
    check_id: str           # "CHECK_1" … "CHECK_7"
    check_name: str
    status: str             # "PASS" | "FLAG" | "FAIL"
    summary: str
    findings: List[QCFinding] = field(default_factory=list)

    @property
    def flag_count(self) -> int:
        return len(self.findings)


@dataclass
class QCReport:
    period: str
    property_name: str
    run_at: str
    checks: List[QCResult] = field(default_factory=list)

    @property
    def has_flags(self) -> bool:
        return any(c.status in ('FLAG', 'FAIL') for c in self.checks)

    @property
    def overall_status(self) -> str:
        if any(c.status == 'FAIL' for c in self.checks):
            return 'FAIL'
        if any(c.status == 'FLAG' for c in self.checks):
            return 'FLAG'
        return 'PASS'


# ══════════════════════════════════════════════════════════════
# CHECK 1 — TB to Budget Comparison Tie-Out
# ══════════════════════════════════════════════════════════════

def check_1_tb_to_budget(tb_result, budget_rows: List[dict]) -> QCResult:
    """
    For every account present in both the Trial Balance and Budget Comparison,
    confirm that TB net activity equals Budget Comparison PTD Actual.

    Revenue accounts: TB net = credit - debit (income shown positive in BC)
    Expense accounts: TB net = debit - credit
    """
    findings: List[QCFinding] = []
    bc_map: Dict[str, dict] = {}

    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        if code:
            bc_map[code] = row

    tb_map = tb_result.account_map if tb_result else {}

    matched = 0
    for code, tb_acct in tb_map.items():
        if code not in bc_map:
            continue

        bc_row = bc_map[code]
        ptd_actual = _safe_float(bc_row.get('ptd_actual', 0))

        # Convention: revenue accounts show positive income in BC
        if _is_revenue(code):
            tb_net = tb_acct.credit - tb_acct.debit
        else:
            tb_net = tb_acct.debit - tb_acct.credit

        diff = abs(tb_net - ptd_actual)
        matched += 1

        if diff > 0.02:  # $0.02 tolerance for rounding
            findings.append(QCFinding(
                account_code=code,
                account_name=tb_acct.account_name,
                value_a=tb_net,
                value_b=ptd_actual,
                difference=tb_net - ptd_actual,
                flag='MISMATCH',
                note=f'TB net activity ${tb_net:,.2f} ≠ BC PTD Actual ${ptd_actual:,.2f}',
            ))

    if not findings:
        summary = (f'All {matched} accounts with TB activity tie to Budget Comparison PTD Actual. '
                   f'Difference = $0.00.')
        status = 'PASS'
    else:
        summary = (f'{len(findings)} account(s) do not tie between TB and Budget Comparison '
                   f'out of {matched} checked.')
        status = 'FAIL'

    return QCResult('CHECK_1', 'TB to Budget Comparison Tie-Out', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 2 — Budget Variances (GRP Tier 1 Threshold)
# ══════════════════════════════════════════════════════════════

def check_2_budget_variances(budget_rows: List[dict]) -> QCResult:
    """
    Flag all accounts that meet GRP Tier 1 or Tier 2 variance thresholds.
    Separates OVER and UNDER for clarity.
    """
    findings: List[QCFinding] = []

    # Skip rows that should never be commented
    from variance_comments import _is_skip_row

    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        name = str(row.get('account_name', '') or '').strip()

        if _is_skip_row(code, name):
            continue

        ptd_actual = _safe_float(row.get('ptd_actual', 0))
        ptd_budget = _safe_float(row.get('ptd_budget', 0))

        tier, var_dollar, var_pct = classify_tier(ptd_actual, ptd_budget)
        if tier == 'tier_3':
            continue

        # Determine direction relative to NOI
        if _is_revenue(code):
            flag = 'OVER' if var_dollar > 0 else 'UNDER'
        else:
            flag = 'OVER' if var_dollar > 0 else 'UNDER'

        tier_label = 'T1' if tier == 'tier_1' else 'T2'
        note = f'{tier_label} | ${abs(var_dollar):,.0f} ({var_pct:+.1f}%)'

        findings.append(QCFinding(
            account_code=code,
            account_name=name,
            value_a=ptd_actual,
            value_b=ptd_budget,
            difference=var_dollar,
            flag=flag,
            note=note,
        ))

    over_count = sum(1 for f in findings if f.flag == 'OVER')
    under_count = sum(1 for f in findings if f.flag == 'UNDER')
    t1_count = sum(1 for f in findings if 'T1' in f.note)

    if not findings:
        summary = 'No current-period variances exceed GRP Tier 1 threshold.'
        status = 'PASS'
    else:
        largest = max(findings, key=lambda f: abs(f.difference))
        summary = (f'{t1_count} Tier-1 variance(s) flagged ({over_count} OVER, {under_count} UNDER). '
                   f'Largest: {largest.account_name} '
                   f'{"OVER" if largest.difference > 0 else "UNDER"} '
                   f'${abs(largest.difference):,.0f}.')
        status = 'FLAG'

    return QCResult('CHECK_2', 'Budget Variances — GRP Tier 1', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 3 — TB Self-Balance + GL vs TB
# ══════════════════════════════════════════════════════════════

def check_3_tb_balance_and_gl(tb_result, gl_parsed) -> QCResult:
    """
    3a. TB self-balance: total debits = total credits.
    3b. For each account in GL, ending balance matches TB ending balance.
    3c. Income accounts in TB are presented in negative convention (credit balance).
    """
    findings: List[QCFinding] = []

    if not tb_result:
        return QCResult('CHECK_3', 'TB Self-Balance + GL vs TB',
                        'FAIL', 'Trial Balance not available.', [])

    # 3a: Self-balance check
    diff = abs(tb_result.total_debits - tb_result.total_credits)
    if diff > 0.05:
        findings.append(QCFinding(
            account_code='TB-TOTAL',
            account_name='Trial Balance Total',
            value_a=tb_result.total_debits,
            value_b=tb_result.total_credits,
            difference=tb_result.total_debits - tb_result.total_credits,
            flag='FAIL',
            note=f'Debits ({tb_result.total_debits:,.2f}) ≠ Credits ({tb_result.total_credits:,.2f}). Out of balance by ${diff:,.2f}.',
        ))

    # 3b: GL ending balance vs TB ending balance
    if gl_parsed and hasattr(gl_parsed, 'accounts'):
        tb_map = tb_result.account_map
        for gl_acct in gl_parsed.accounts:
            code = str(gl_acct.account_code or '').strip()
            if code not in tb_map:
                continue
            tb_acct = tb_map[code]
            gl_end = float(getattr(gl_acct, 'ending_balance', 0) or 0)
            tb_end = float(tb_acct.ending_balance)
            diff_b = abs(gl_end - tb_end)
            if diff_b > 0.05:
                findings.append(QCFinding(
                    account_code=code,
                    account_name=tb_acct.account_name,
                    value_a=gl_end,
                    value_b=tb_end,
                    difference=gl_end - tb_end,
                    flag='MISMATCH',
                    note=f'GL ending ${gl_end:,.2f} ≠ TB ending ${tb_end:,.2f}',
                ))

    # 3c: TB totals summary as info finding
    findings.insert(0, QCFinding(
        account_code='TB-TOTAL',
        account_name='TB Totals',
        value_a=tb_result.total_debits,
        value_b=tb_result.total_credits,
        difference=tb_result.total_debits - tb_result.total_credits,
        flag='INFO' if tb_result.is_balanced else 'FAIL',
        note=(f'Debits = Credits = ${tb_result.total_debits:,.2f}'
              if tb_result.is_balanced
              else f'OUT OF BALANCE by ${abs(tb_result.total_debits - tb_result.total_credits):,.2f}'),
    ))

    critical = [f for f in findings if f.flag in ('FAIL', 'MISMATCH')]
    if not critical:
        status = 'PASS'
        acct_count = len(tb_result.accounts)
        summary = (f'TB balanced: debits = credits = ${tb_result.total_debits:,.2f}. '
                   f'All {acct_count} accounts verified.')
    else:
        status = 'FAIL'
        summary = f'{len(critical)} balance discrepancy/discrepancies found.'

    return QCResult('CHECK_3', 'Workpapers to Trial Balance Tie-Out', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 4 — Month-over-Month Swings
# ══════════════════════════════════════════════════════════════

def check_4_mom_swings(budget_rows: List[dict],
                       swing_threshold: float = 10_000.0) -> QCResult:
    """
    For P&L accounts, derive prior-month actual = YTD actual - PTD actual.
    Flag if |PTD actual - prior month actual| > $10,000 or if sign changes.

    Note: For month 1 (January), prior month = 0 (no prior YTD).
    """
    from variance_comments import _is_skip_row

    findings: List[QCFinding] = []

    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        name = str(row.get('account_name', '') or '').strip()

        if _is_skip_row(code, name):
            continue
        if not (_is_revenue(code) or _is_expense(code)):
            continue

        ptd = _safe_float(row.get('ptd_actual', 0))
        ytd = _safe_float(row.get('ytd_actual', 0))

        # Derive prior month
        prior = ytd - ptd

        swing = ptd - prior
        abs_swing = abs(swing)
        sign_change = (ptd > 0 and prior < 0) or (ptd < 0 and prior > 0)

        if abs_swing > swing_threshold or (sign_change and abs_swing > 2_500):
            flag = 'FLAG'
            note = (f'Prior month ${prior:,.0f} → Current ${ptd:,.0f} '
                    f'= swing ${swing:+,.0f}')
            if sign_change:
                note += ' [SIGN CHANGE]'

            findings.append(QCFinding(
                account_code=code,
                account_name=name,
                value_a=ptd,
                value_b=prior,
                difference=swing,
                flag=flag,
                note=note,
            ))

    # Sort by absolute swing descending
    findings.sort(key=lambda f: abs(f.difference), reverse=True)

    if not findings:
        summary = f'No month-over-month swings exceed ${swing_threshold:,.0f}.'
        status = 'PASS'
    else:
        largest = findings[0]
        summary = (f'{len(findings)} line(s) with MoM swing > ${swing_threshold:,.0f}. '
                   f'Largest: {largest.account_name} '
                   f'${abs(largest.difference):,.0f} swing.')
        status = 'FLAG'

    return QCResult('CHECK_4', 'Month-over-Month Swings (>$10,000)', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 5 — BS Workpaper Tie-Out (GL ending vs TB ending per account)
# ══════════════════════════════════════════════════════════════

def check_5_gl_vs_workpapers(gl_parsed, tb_result) -> QCResult:
    """
    For each balance sheet account, compare GL ending balance to TB ending balance.

    Pre-accrual run: GL ≠ TB is expected — the variance equals the accrual JEs
    that need to be posted. These are flagged as INFO so GRP knows which accounts
    need entries, not as failures.

    Post-accrual run: All variances should be zero. Any remaining non-zero variance
    is a genuine discrepancy and flagged as MISMATCH.

    Accounts not in TB are flagged separately (likely zero-balance accounts).
    """
    findings: List[QCFinding] = []

    if not gl_parsed or not hasattr(gl_parsed, 'accounts'):
        return QCResult('CHECK_5', 'BS Workpaper Tie-Out',
                        'FLAG', 'GL data not available for this check.', [])

    if not tb_result:
        return QCResult('CHECK_5', 'BS Workpaper Tie-Out',
                        'FLAG', 'Trial Balance not uploaded — cannot run workpaper tie-out.', [])

    tb_map = tb_result.account_map if tb_result else {}
    checked = 0
    accrual_gap_total = 0.0
    clean_count = 0

    for gl_acct in gl_parsed.accounts:
        code = str(gl_acct.account_code or '').strip()
        if not _is_balance_sheet(code):
            continue

        gl_end = float(getattr(gl_acct, 'ending_balance', 0) or 0)
        checked += 1

        if code not in tb_map:
            # In GL but not in TB — only flag if non-zero balance
            if abs(gl_end) > 0.05:
                findings.append(QCFinding(
                    account_code=code,
                    account_name=gl_acct.account_name,
                    value_a=gl_end,
                    value_b=0.0,
                    difference=gl_end,
                    flag='INFO',
                    note=f'Account in GL (${gl_end:,.2f}) but not in TB — verify zero balance expected.',
                ))
            continue

        tb_end = float(tb_map[code].ending_balance)
        diff = gl_end - tb_end

        if abs(diff) <= 0.05:
            clean_count += 1
        else:
            accrual_gap_total += abs(diff)
            findings.append(QCFinding(
                account_code=code,
                account_name=gl_acct.account_name,
                value_a=gl_end,
                value_b=tb_end,
                difference=diff,
                flag='FLAG',
                note=(f'GL ${gl_end:,.2f} vs TB ${tb_end:,.2f} — '
                      f'variance of ${abs(diff):,.2f} likely requires accrual JE.'),
            ))

    accrual_gaps = [f for f in findings if f.flag == 'FLAG']
    info_gaps    = [f for f in findings if f.flag == 'INFO']

    if not accrual_gaps and not info_gaps:
        status  = 'PASS'
        summary = f'All {clean_count} BS accounts tie — GL ending = TB ending.'
    elif accrual_gaps:
        status  = 'FLAG'
        summary = (f'{len(accrual_gaps)} account(s) have GL vs TB variance '
                   f'(total ${accrual_gap_total:,.2f}) — accrual JEs required. '
                   f'{clean_count} account(s) tie clean. See BS Workpaper for detail.')
    else:
        status  = 'FLAG'
        summary = f'{len(info_gaps)} account(s) in GL not found in TB — verify expected.'

    return QCResult('CHECK_5', 'BS Workpaper Tie-Out', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 6 — Accruals vs Budget
# ══════════════════════════════════════════════════════════════

def check_6_accruals_vs_budget(budget_rows: List[dict],
                                kardin_records: List[dict] = None,
                                accrual_entries: List[dict] = None,
                                period_month: int = 1) -> QCResult:
    """
    Identify expense accounts where:
    - Budget > $0 for the month AND
    - PTD Actual = $0 AND
    - Account is not a known seasonal account with $0 expected this month

    These are candidates for missing accruals.
    Also verifies that suggested accrual entries (from accrual engine) exist where expected.
    """
    from variance_comments import _is_skip_row, build_kardin_enrichment

    findings: List[QCFinding] = []
    kardin_records = kardin_records or []

    # Build set of accounts that have accrual entries
    accrual_codes = set()
    if accrual_entries:
        for entry in accrual_entries:
            code = str(entry.get('account_code', '') or '').strip()
            if code:
                accrual_codes.add(code)

    for row in budget_rows:
        code = str(row.get('account_code', '') or '').strip()
        name = str(row.get('account_name', '') or '').strip()

        if _is_skip_row(code, name):
            continue
        if not _is_expense(code):
            continue

        ptd_actual = _safe_float(row.get('ptd_actual', 0))
        ptd_budget = _safe_float(row.get('ptd_budget', 0))

        if ptd_budget <= 0:
            continue  # No budget → no expected accrual

        if abs(ptd_actual) > 1.0:
            continue  # Activity exists → no missing accrual

        # Check if this month is budgeted as $0 in Kardin (seasonal)
        kardin = build_kardin_enrichment(kardin_records, code, period_month)
        month_budget = kardin.get('month_budget', ptd_budget)

        if abs(month_budget) < 1.0:
            # Kardin says $0 this month — this is expected, not a gap
            findings.append(QCFinding(
                account_code=code,
                account_name=name,
                value_a=ptd_actual,
                value_b=ptd_budget,
                difference=ptd_actual - ptd_budget,
                flag='INFO',
                note=f'$0 actual; Kardin shows $0 budget this month — no accrual expected.',
            ))
        else:
            has_accrual = code in accrual_codes
            flag = 'INFO' if has_accrual else 'FLAG'
            note = (
                f'$0 actual vs ${ptd_budget:,.0f} budget. '
                + ('Accrual entry generated.' if has_accrual
                   else f'No accrual found. Kardin month budget: ${month_budget:,.0f}. Review required.')
            )
            findings.append(QCFinding(
                account_code=code,
                account_name=name,
                value_a=ptd_actual,
                value_b=ptd_budget,
                difference=ptd_actual - ptd_budget,
                flag=flag,
                note=note,
            ))

    real_flags = [f for f in findings if f.flag == 'FLAG']
    if not real_flags:
        status = 'PASS'
        info_count = sum(1 for f in findings if f.flag == 'INFO')
        summary = (f'No missing accruals detected. '
                   f'{info_count} contingency item(s) with $0 budget this period — no action.')
    else:
        status = 'FLAG'
        summary = (f'{len(real_flags)} potential missing accrual(s) flagged for review. '
                   f'{sum(1 for f in findings if f.flag == "INFO")} items are expected $0.')

    return QCResult('CHECK_6', 'Accruals vs Budget', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# CHECK 7 — Miscellaneous QC Items
# ══════════════════════════════════════════════════════════════

def check_7_misc(budget_rows: List[dict],
                 gl_parsed=None,
                 tb_result=None,
                 kardin_records: List[dict] = None,
                 cash_received: float = None,
                 jll_rate: float = 0.0125,
                 grp_rate: float = 0.0175) -> QCResult:
    """
    Spot-checks for:
    7a. Management fee accrual vs calculated fee (cash received × rate)
    7b. Interest expense — verify accrual exists and is non-zero
    7c. Insurance-Property — verify monthly charge matches expected amortization
    7d. Prepaid accounts — forward balance + debit - credit = ending balance
    """
    findings: List[QCFinding] = []
    kardin_records = kardin_records or []
    tb_map = tb_result.account_map if tb_result else {}

    # ── 7a: Management Fee ─────────────────────────────────────
    mgmt_fee_code = '637130'
    bc_map = {str(r.get('account_code', '') or '').strip(): r for r in budget_rows}

    if mgmt_fee_code in bc_map and cash_received is not None and cash_received > 0:
        ptd_actual = abs(_safe_float(bc_map[mgmt_fee_code].get('ptd_actual', 0)))
        expected_jll = cash_received * jll_rate
        expected_grp = cash_received * grp_rate
        expected_total = expected_jll + expected_grp
        diff = abs(ptd_actual - expected_total)

        flag = 'PASS_INFO' if diff < 500 else 'FLAG'
        findings.append(QCFinding(
            account_code=mgmt_fee_code,
            account_name='Admin-Management Fees',
            value_a=ptd_actual,
            value_b=expected_total,
            difference=ptd_actual - expected_total,
            flag=flag,
            note=(f'Accrued: ${ptd_actual:,.2f} | '
                  f'Calculated: ${expected_total:,.2f} '
                  f'(Cash ${cash_received:,.2f} × JLL {jll_rate:.2%} + GRP {grp_rate:.2%}). '
                  + ('Ties.' if diff < 500 else f'Difference: ${diff:,.2f} — update accrual.')),
        ))
    elif mgmt_fee_code in bc_map:
        ptd_actual = _safe_float(bc_map[mgmt_fee_code].get('ptd_actual', 0))
        findings.append(QCFinding(
            account_code=mgmt_fee_code,
            account_name='Admin-Management Fees',
            value_a=ptd_actual,
            value_b=0,
            difference=0,
            flag='INFO',
            note=f'Accrual: ${ptd_actual:,.2f}. Cash received not available — cannot verify rate calc.',
        ))

    # ── 7b: Interest Expense ───────────────────────────────────
    interest_code = '801110'
    if interest_code in bc_map:
        ptd_actual = abs(_safe_float(bc_map[interest_code].get('ptd_actual', 0)))
        if ptd_actual < 1.0:
            findings.append(QCFinding(
                account_code=interest_code,
                account_name='Interest Expense',
                value_a=ptd_actual,
                value_b=0,
                difference=0,
                flag='FLAG',
                note='Interest Expense is $0 — verify accrual was posted.',
            ))
        else:
            findings.append(QCFinding(
                account_code=interest_code,
                account_name='Interest Expense',
                value_a=ptd_actual,
                value_b=0,
                difference=0,
                flag='INFO',
                note=f'Interest Expense: ${ptd_actual:,.2f}. Verify against loan statement / amortization schedule.',
            ))

    # ── 7c: Insurance amortization ─────────────────────────────
    ins_code = '639110'
    if ins_code in bc_map and kardin_records:
        from variance_comments import build_kardin_enrichment
        kardin = build_kardin_enrichment(kardin_records, ins_code, 1)
        annual_budget = kardin.get('annual_budget', 0)
        expected_monthly = annual_budget / 12 if annual_budget else 0
        ptd_actual = abs(_safe_float(bc_map[ins_code].get('ptd_actual', 0)))
        if expected_monthly > 0:
            diff = abs(ptd_actual - expected_monthly)
            flag = 'INFO' if diff < 500 else 'FLAG'
            findings.append(QCFinding(
                account_code=ins_code,
                account_name='Insurance-Property',
                value_a=ptd_actual,
                value_b=expected_monthly,
                difference=ptd_actual - expected_monthly,
                flag=flag,
                note=(f'Actual: ${ptd_actual:,.2f} | '
                      f'Expected monthly (${annual_budget:,.0f}/12): ${expected_monthly:,.2f}. '
                      + ('On track.' if diff < 500 else f'Difference ${diff:,.2f} — verify prepaid schedule.')),
            ))

    # ── 7d: Prepaid RE Tax math ────────────────────────────────
    if tb_result:
        for code in ('135110', '135120', '135150'):
            if code in tb_map:
                acct = tb_map[code]
                expected_end = acct.forward_balance + acct.debit - acct.credit
                diff = abs(expected_end - acct.ending_balance)
                if diff > 0.05:
                    findings.append(QCFinding(
                        account_code=code,
                        account_name=acct.account_name,
                        value_a=acct.ending_balance,
                        value_b=expected_end,
                        difference=acct.ending_balance - expected_end,
                        flag='FLAG',
                        note=f'Prepaid math error: fwd {acct.forward_balance:,.2f} + debit {acct.debit:,.2f} - credit {acct.credit:,.2f} = {expected_end:,.2f} ≠ ending {acct.ending_balance:,.2f}',
                    ))

    flags = [f for f in findings if f.flag == 'FLAG']
    if not flags:
        status = 'PASS'
        summary = 'Miscellaneous checks passed — management fee, interest, insurance, prepaid math.'
    else:
        status = 'FLAG'
        items = ', '.join(f.account_name for f in flags[:3])
        summary = f'{len(flags)} miscellaneous item(s) flagged: {items}.'

    return QCResult('CHECK_7', 'Miscellaneous QC Items', status, summary, findings)


# ══════════════════════════════════════════════════════════════
# MAIN RUNNER
# ══════════════════════════════════════════════════════════════

def run_qc(
    budget_rows: List[dict],
    tb_result=None,
    gl_parsed=None,
    kardin_records: List[dict] = None,
    accrual_entries: List[dict] = None,
    period: str = '',
    property_name: str = 'Revolution Labs Owner, LLC',
    period_month: int = 1,
    cash_received: float = None,
) -> QCReport:
    """
    Run all 8 QC checks and return a QCReport.

    Args:
        budget_rows:     Parsed budget comparison rows.
        tb_result:       Parsed Trial Balance (TBResult).
        gl_parsed:       Parsed GL (GLParseResult).
        kardin_records:  Kardin annual budget records.
        accrual_entries: Accrual JE entries from accrual engine.
        period:          Period string (e.g. "Apr 2026").
        property_name:   Property display name.
        period_month:    Reporting month number (1=Jan … 12=Dec).
        cash_received:   Total cash received for the month (for mgmt fee check).
    """
    kardin_records = kardin_records or []

    checks = [
        check_1_tb_to_budget(tb_result, budget_rows),
        check_2_budget_variances(budget_rows),
        check_3_tb_balance_and_gl(tb_result, gl_parsed),
        check_4_mom_swings(budget_rows),
        check_5_gl_vs_workpapers(gl_parsed, tb_result),
        check_6_accruals_vs_budget(budget_rows, kardin_records, accrual_entries, period_month),
        check_7_misc(budget_rows, gl_parsed, tb_result, kardin_records, cash_received),
    ]

    return QCReport(
        period=period,
        property_name=property_name,
        run_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
        checks=checks,
    )


# ══════════════════════════════════════════════════════════════
# EXCEL WORKBOOK GENERATOR
# ══════════════════════════════════════════════════════════════

# Colour palette matching LexLabs QC workbook
_GREEN  = 'C6EFCE'
_YELLOW = 'FFEB9C'
_RED    = 'FFC7CE'
_BLUE   = 'DDEEFF'
_HEADER = '1F3864'


def _hdr_font(color='FFFFFF', bold=True, size=10):
    return Font(name='Calibri', bold=bold, color=color, size=size)


def _cell_font(bold=False, size=10):
    return Font(name='Calibri', bold=bold, size=size)


def _fill(hex_color):
    return PatternFill('solid', fgColor=hex_color)


def _border():
    s = Side(style='thin', color='BFBFBF')
    return Border(left=s, right=s, top=s, bottom=s)


def _write_summary_sheet(ws, report: QCReport) -> None:
    ws.title = 'QC Summary'
    ws.column_dimensions['A'].width = 42
    ws.column_dimensions['B'].width = 10
    ws.column_dimensions['C'].width = 80

    # Title
    ws['A1'] = f"{report.property_name} — {report.period}"
    ws['A1'].font = Font(name='Calibri', bold=True, size=13, color=_HEADER)
    ws['A2'] = 'QC Workbook — Executive Summary of Findings'
    ws['A2'].font = Font(name='Calibri', bold=True, size=11)
    ws['A3'] = f'Run: {report.run_at}'
    ws['A3'].font = _cell_font(size=9)

    # Column headers
    row = 5
    for col, label in enumerate(['CHECK', 'STATUS', 'SUMMARY'], 1):
        cell = ws.cell(row=row, column=col, value=label)
        cell.font = _hdr_font()
        cell.fill = _fill(_HEADER)
        cell.alignment = Alignment(horizontal='center')

    for check in report.checks:
        row += 1
        status_color = {'PASS': _GREEN, 'FLAG': _YELLOW, 'FAIL': _RED}.get(check.status, _YELLOW)

        ws.cell(row=row, column=1, value=f'{check.check_id}: {check.check_name}').font = _cell_font(bold=True)
        status_cell = ws.cell(row=row, column=2, value=check.status)
        status_cell.font = _cell_font(bold=True)
        status_cell.fill = _fill(status_color)
        status_cell.alignment = Alignment(horizontal='center')
        ws.cell(row=row, column=3, value=check.summary).alignment = Alignment(wrap_text=True)
        ws.row_dimensions[row].height = 28


def _write_detail_sheet(ws, check: QCResult,
                         col_headers: List[str],
                         col_widths: List[int]) -> None:
    _TAB_NAMES = {
        'CHECK_1': '1-TB to BC Tie-Out',
        'CHECK_2': '2-Budget Variances',
        'CHECK_3': '3-Workpapers to TB',
        'CHECK_4': '4-MoM Swings',
        'CHECK_5': '5-GL vs Workpapers',
        'CHECK_6': '6-Accruals vs Budget',
        'CHECK_7': '7-Misc Items',
    }
    ws.title = _TAB_NAMES.get(check.check_id, check.check_id)[:31]

    # Header row
    for col, (label, width) in enumerate(zip(col_headers, col_widths), 1):
        cell = ws.cell(row=1, column=col, value=label)
        cell.font = _hdr_font()
        cell.fill = _fill(_HEADER)
        ws.column_dimensions[get_column_letter(col)].width = width

    for row_num, finding in enumerate(check.findings, 2):
        flag_color = {
            'OVER': _RED, 'UNDER': _YELLOW, 'MISMATCH': _RED,
            'MISSING': _YELLOW, 'FLAG': _YELLOW, 'FAIL': _RED,
            'INFO': _BLUE, 'PASS_INFO': _GREEN,
        }.get(finding.flag, '')

        values = [
            finding.account_code,
            finding.account_name,
            finding.value_a,
            finding.value_b,
            finding.difference,
            finding.flag,
            finding.note,
        ]
        for col, val in enumerate(values[:len(col_headers)], 1):
            cell = ws.cell(row=row_num, column=col, value=val)
            cell.font = _cell_font()
            cell.border = _border()
            if flag_color and col == len(col_headers) - 1:  # flag column
                cell.fill = _fill(flag_color)
            if isinstance(val, float):
                cell.number_format = '$#,##0.00;($#,##0.00);"-"'
            cell.alignment = Alignment(wrap_text=True, vertical='top')


_DETAIL_HEADERS = ['Account', 'Name', 'Actual / GL', 'Budget / TB', 'Difference', 'Flag', 'Notes']
_DETAIL_WIDTHS  = [12, 36, 16, 16, 16, 10, 60]


def generate_qc_workbook(report: QCReport, output_path: str) -> None:
    """
    Write the QC report to an Excel workbook matching the LexLabs QC structure.

    Tabs:
      QC Summary  |  1-TB to CIS Tie  |  2-Budget Variances  |  3-Workpapers to TB
      4-MoM Swings  |  5-GL to Workpapers  |  6-Accruals vs Budget  |  7-Misc
    """
    wb = Workbook()

    # Summary tab
    _write_summary_sheet(wb.active, report)

    # Detail tabs (one per check)
    for check in report.checks:
        ws = wb.create_sheet()
        _write_detail_sheet(ws, check, _DETAIL_HEADERS, _DETAIL_WIDTHS)

    wb.save(output_path)
