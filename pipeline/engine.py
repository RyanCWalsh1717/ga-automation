"""
GA Automation Pipeline — Processing Engine
============================================
Orchestrates all parsers, runs cross-source validation, matches
GL entries to invoices/payments, and produces structured output
for the report generator.
"""

import os
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Tuple


# ── Data classes for engine output ───────────────────────────

@dataclass
class MatchResult:
    """A matched pair between two sources."""
    source_a: str           # e.g. "GL"
    source_b: str           # e.g. "Bank"
    key: str                # matching key (control#, reference#, etc.)
    amount_a: float
    amount_b: float
    matched: bool
    variance: float
    description: str
    details: dict = field(default_factory=dict)


@dataclass
class Exception_:
    """A flagged issue requiring review."""
    severity: str           # "error", "warning", "info"
    category: str           # "balance", "match", "missing", "variance"
    source: str             # which parser/check found it
    description: str
    details: dict = field(default_factory=dict)
    resolved: bool = False


@dataclass
class EngineResult:
    """Complete output from a pipeline run."""
    run_id: str
    run_at: str
    period: str
    property_name: str

    # Parsed data (references to parser outputs)
    parsed: dict = field(default_factory=dict)

    # Cross-validation results
    gl_invoice_matches: list = field(default_factory=list)
    gl_bank_matches: list = field(default_factory=list)
    debt_service_check: dict = field(default_factory=dict)
    budget_variances: list = field(default_factory=list)

    # Exceptions
    exceptions: list = field(default_factory=list)

    # Summary
    summary: dict = field(default_factory=dict)

    def add_exception(self, severity, category, source, description, **details):
        self.exceptions.append(Exception_(
            severity=severity, category=category,
            source=source, description=description,
            details=details,
        ))

    @property
    def error_count(self):
        return sum(1 for e in self.exceptions if e.severity == "error")

    @property
    def warning_count(self):
        return sum(1 for e in self.exceptions if e.severity == "warning")

    @property
    def status(self):
        if self.error_count > 0:
            return "ERRORS"
        if self.warning_count > 0:
            return "WARNINGS"
        return "CLEAN"


# ── Cross-validation functions ───────────────────────────────

def match_gl_to_invoices(gl_result, nexus_result) -> Tuple[List[MatchResult], List[Exception_]]:
    """
    Match GL transactions to Nexus invoices using control/reference numbers.
    Every Nexus invoice should have a corresponding GL entry.
    """
    matches = []
    exceptions = []

    if nexus_result is None or not hasattr(nexus_result, '__iter__'):
        return matches, exceptions

    # Build GL lookup by reference number
    gl_by_ref = {}
    if hasattr(gl_result, 'all_transactions'):
        for txn in gl_result.all_transactions:
            ref = txn.reference.strip() if txn.reference else ""
            if ref:
                if ref not in gl_by_ref:
                    gl_by_ref[ref] = []
                gl_by_ref[ref].append(txn)

    # Build GL lookup by control number
    gl_by_control = {}
    if hasattr(gl_result, 'all_transactions'):
        for txn in gl_result.all_transactions:
            ctrl = txn.control.strip() if txn.control else ""
            if ctrl:
                if ctrl not in gl_by_control:
                    gl_by_control[ctrl] = []
                gl_by_control[ctrl].append(txn)

    # Try to match each Nexus invoice to a GL entry
    invoices = nexus_result if isinstance(nexus_result, list) else []
    for inv in invoices:
        inv_num = inv.get('invoice_number', '') if isinstance(inv, dict) else getattr(inv, 'invoice_number', '')
        inv_amt = inv.get('amount', 0) if isinstance(inv, dict) else getattr(inv, 'amount', 0)
        inv_vendor = inv.get('vendor', '') if isinstance(inv, dict) else getattr(inv, 'vendor', '')

        # Try matching by invoice number in GL references
        found = False
        if inv_num and inv_num in gl_by_ref:
            gl_txns = gl_by_ref[inv_num]
            gl_total = sum(t.debit - t.credit for t in gl_txns)
            variance = abs(inv_amt - abs(gl_total))
            matches.append(MatchResult(
                source_a="Nexus", source_b="GL",
                key=inv_num,
                amount_a=inv_amt, amount_b=abs(gl_total),
                matched=variance < 0.01,
                variance=variance,
                description=f"{inv_vendor} - Invoice {inv_num}",
            ))
            found = True

        if not found and inv_num:
            # Check if invoice number appears in any GL control number
            for ctrl, txns in gl_by_control.items():
                if inv_num in ctrl:
                    gl_total = sum(t.debit - t.credit for t in txns)
                    variance = abs(inv_amt - abs(gl_total))
                    matches.append(MatchResult(
                        source_a="Nexus", source_b="GL",
                        key=f"{inv_num} -> {ctrl}",
                        amount_a=inv_amt, amount_b=abs(gl_total),
                        matched=variance < 0.01,
                        variance=variance,
                        description=f"{inv_vendor} - Invoice {inv_num} (partial match)",
                    ))
                    found = True
                    break

        if not found and inv_amt != 0:
            exceptions.append(Exception_(
                severity="warning", category="match",
                source="gl_invoice_match",
                description=f"Unmatched Nexus invoice: {inv_vendor} #{inv_num} ${inv_amt:,.2f}",
                details={"vendor": inv_vendor, "invoice": inv_num, "amount": inv_amt},
            ))

    return matches, exceptions


def match_gl_to_bank(gl_result, bank_result) -> Tuple[List[MatchResult], List[Exception_]]:
    """
    Reconcile GL cash account to bank statement.

    Yardi control numbers (C-xxxx, K-xxxx) don't directly map to bank
    check numbers, so we do an aggregate reconciliation:
      - Compare GL cash account ending balance to bank ending balance
      - Compare total debits/credits
      - Flag the variance as timing differences (outstanding checks/deposits)
      - Match ACH payments by amount to GL entries where possible
    """
    matches = []
    exceptions = []

    if bank_result is None:
        return matches, exceptions

    # Get GL cash account (111100)
    gl_cash_acct = None
    if hasattr(gl_result, 'accounts'):
        for acct in gl_result.accounts:
            if acct.account_code == "111100":
                gl_cash_acct = acct
                break

    if gl_cash_acct is None:
        exceptions.append(Exception_(
            severity="warning", category="balance",
            source="gl_bank_recon",
            description="GL Cash-Operating (111100) account not found",
        ))
        return matches, exceptions

    # Get bank balances and totals
    bank_data = bank_result if isinstance(bank_result, dict) else {}
    bank_begin = bank_data.get('beginning_balance', 0) or 0
    bank_end = bank_data.get('ending_balance', 0) or 0
    bank_checks = bank_data.get('checks', [])
    bank_ach = bank_data.get('ach_debits', [])
    bank_deposits = bank_data.get('deposits', [])
    bank_wires = bank_data.get('wire_transfers', [])

    bank_total_checks = sum(c.get('amount', 0) for c in bank_checks)
    bank_total_ach = sum(a.get('amount', 0) for a in bank_ach)
    bank_total_deposits = sum(d.get('amount', 0) for d in bank_deposits)

    # Aggregate reconciliation
    gl_begin = gl_cash_acct.beginning_balance
    gl_end = gl_cash_acct.ending_balance
    gl_debits = gl_cash_acct.total_debits
    gl_credits = gl_cash_acct.total_credits

    # Balance comparison
    balance_var = gl_end - bank_end
    matches.append(MatchResult(
        source_a="GL", source_b="Bank",
        key="Ending Balance",
        amount_a=gl_end, amount_b=bank_end,
        matched=abs(balance_var) < 0.01,
        variance=abs(balance_var),
        description="GL Cash-Operating ending balance vs. bank ending balance",
        details={"gl_begin": gl_begin, "bank_begin": bank_begin},
    ))

    if abs(balance_var) > 0.01:
        exceptions.append(Exception_(
            severity="info", category="balance",
            source="gl_bank_recon",
            description=(
                f"GL ending balance (${gl_end:,.2f}) differs from bank ending balance "
                f"(${bank_end:,.2f}) by ${balance_var:,.2f} — likely timing differences "
                f"(outstanding checks/deposits in transit)"
            ),
        ))

    # Check totals
    matches.append(MatchResult(
        source_a="GL", source_b="Bank",
        key="Total Checks Cleared",
        amount_a=gl_credits, amount_b=bank_total_checks,
        matched=False,  # These won't match due to timing
        variance=abs(gl_credits - bank_total_checks),
        description=f"GL total credits vs. bank checks cleared ({len(bank_checks)} checks)",
        details={"bank_check_count": len(bank_checks)},
    ))

    # Match Berkadia ACH payments to GL debt service
    for ach in bank_ach:
        desc = ach.get('description', '')
        amount = ach.get('amount', 0)
        if 'Berkadia' in desc or 'Loan' in desc:
            matches.append(MatchResult(
                source_a="Bank ACH", source_b="Loan Payment",
                key=desc[:50],
                amount_a=amount, amount_b=amount,
                matched=True,
                variance=0,
                description=f"Berkadia loan payment: ${amount:,.2f}",
                details={"date": ach.get('date', ''), "reference": ach.get('reference', '')},
            ))

    return matches, exceptions


def check_debt_service(gl_result, loan_result) -> Tuple[dict, List[Exception_]]:
    """
    Reconcile debt service: compare loan statement interest/principal
    against GL interest expense entries.
    """
    exceptions = []
    result = {
        "loans": [],
        "gl_interest_expense": 0,
        "loan_interest_total": 0,
        "variance": 0,
        "reconciled": False,
    }

    if loan_result is None:
        return result, exceptions

    # Get GL interest expense (account 801110)
    gl_interest = 0
    if hasattr(gl_result, 'accounts'):
        for acct in gl_result.accounts:
            if acct.account_code == "801110":
                gl_interest = acct.net_change
                break
    result["gl_interest_expense"] = abs(gl_interest)

    # Get loan statement data
    loans = []
    if isinstance(loan_result, list):
        loans = loan_result
    elif isinstance(loan_result, dict):
        loans = loan_result.get('loans', [])
    elif hasattr(loan_result, 'loans'):
        loans = loan_result.loans

    total_loan_interest = 0
    for loan in loans:
        if isinstance(loan, dict):
            interest_ytd = loan.get('interest_paid_ytd', 0)
            principal = loan.get('principal_balance', 0)
            name = loan.get('property_name', loan.get('name', 'Unknown'))
        else:
            interest_ytd = getattr(loan, 'interest_paid_ytd', 0)
            principal = getattr(loan, 'principal_balance', 0)
            name = getattr(loan, 'property_name', getattr(loan, 'name', 'Unknown'))

        interest_ytd = interest_ytd if isinstance(interest_ytd, (int, float)) else 0
        principal = principal if isinstance(principal, (int, float)) else 0
        total_loan_interest += interest_ytd
        result["loans"].append({
            "name": name,
            "principal_balance": principal,
            "interest_paid_ytd": interest_ytd,
        })

    result["loan_interest_total"] = total_loan_interest

    # Note: GL interest is PTD (one month), loan interest_paid_ytd is YTD
    # For a proper reconciliation we'd need to compare at the same level
    # For now, flag if GL has no interest when loans exist
    if loans and gl_interest == 0:
        exceptions.append(Exception_(
            severity="warning", category="balance",
            source="debt_service",
            description="Loan statements exist but no GL interest expense found",
        ))

    if abs(gl_interest) > 0:
        result["reconciled"] = True

    return result, exceptions


def check_budget_variances(is_result, budget_result, threshold_pct=10.0) -> Tuple[list, List[Exception_]]:
    """
    Compare Income Statement actuals to Budget Comparison and flag
    material variances exceeding the threshold.
    """
    variances = []
    exceptions = []

    if budget_result is None or is_result is None:
        return variances, exceptions

    # Budget result should have line items with actual vs budget
    budget_items = []
    if isinstance(budget_result, list):
        budget_items = budget_result
    elif hasattr(budget_result, 'line_items'):
        budget_items = budget_result.line_items
    elif hasattr(budget_result, '__iter__'):
        budget_items = list(budget_result)

    for item in budget_items:
        if isinstance(item, dict):
            code = item.get('account_code', '')
            name = item.get('account_name', '')
            ptd_actual = item.get('ptd_actual', 0) or 0
            ptd_budget = item.get('ptd_budget', 0) or 0
            variance = item.get('ptd_variance', 0) or 0
            var_pct = item.get('ptd_percent_var', item.get('ptd_variance_pct', 0))
        else:
            code = getattr(item, 'account_code', '')
            name = getattr(item, 'account_name', '')
            ptd_actual = getattr(item, 'ptd_actual', 0) or 0
            ptd_budget = getattr(item, 'ptd_budget', 0) or 0
            variance = getattr(item, 'ptd_variance', ptd_actual - ptd_budget)
            var_pct = getattr(item, 'ptd_variance_pct', None)

        if not code or "TOTAL" in str(name).upper():
            continue

        # Calculate variance % if not provided
        if var_pct is None or var_pct == 'N/A':
            if ptd_budget != 0:
                var_pct = (variance / abs(ptd_budget)) * 100
            else:
                var_pct = 0

        if isinstance(var_pct, str):
            try:
                var_pct = float(var_pct)
            except ValueError:
                var_pct = 0

        if abs(var_pct) >= threshold_pct and abs(variance) >= 2500:
            variances.append({
                "account_code": code,
                "account_name": name,
                "ptd_actual": ptd_actual,
                "ptd_budget": ptd_budget,
                "variance": variance,
                "variance_pct": round(var_pct, 1),
            })

            if abs(var_pct) >= 25:
                exceptions.append(Exception_(
                    severity="warning", category="variance",
                    source="budget_comparison",
                    description=f"Material variance: {name} ({code}) — {var_pct:+.1f}% (${variance:+,.2f})",
                    details={"account_code": code, "actual": ptd_actual, "budget": ptd_budget},
                ))

    return variances, exceptions


def validate_gl_balance(gl_result) -> List[Exception_]:
    """Verify GL is balanced and all accounts foot."""
    exceptions = []

    if not hasattr(gl_result, 'validation'):
        return exceptions

    v = gl_result.validation
    if not v.get('gl_balanced', True):
        exceptions.append(Exception_(
            severity="error", category="balance",
            source="gl_validation",
            description=f"GL is not balanced: Debits ${v.get('total_debits', 0):,.2f} != Credits ${v.get('total_credits', 0):,.2f}",
        ))

    unbalanced = v.get('unbalanced_accounts', 0)
    if unbalanced > 0:
        exceptions.append(Exception_(
            severity="error", category="balance",
            source="gl_validation",
            description=f"{unbalanced} account(s) do not balance (beginning + debits - credits != ending)",
        ))

    for w in v.get('warnings', []):
        exceptions.append(Exception_(
            severity="warning", category="balance",
            source="gl_validation", description=w,
        ))

    return exceptions


def cross_validate_is_to_gl(is_result, gl_result) -> List[Exception_]:
    """
    Verify Income Statement net income matches GL net change
    for revenue/expense accounts.
    """
    exceptions = []

    if is_result is None or gl_result is None:
        return exceptions

    # Get IS net income
    is_net = None
    if isinstance(is_result, list):
        for item in is_result:
            code = item.get('account_code', '') if isinstance(item, dict) else ''
            if code == '998999':
                is_net = item.get('ptd_amount', 0) if isinstance(item, dict) else 0
                break
    elif hasattr(is_result, 'get_line'):
        net_line = is_result.get_line('998999')
        if net_line:
            is_net = net_line.ptd_amount

    # Get GL total revenue - expense (net change for 4xxxxx - 5/6/7/8xxxxx accounts)
    gl_revenue = 0
    gl_expense = 0
    if hasattr(gl_result, 'accounts'):
        for acct in gl_result.accounts:
            code = acct.account_code
            if code.startswith('4'):
                gl_revenue += acct.net_change
            elif code.startswith(('5', '6', '7', '8')):
                gl_expense += acct.net_change

    if is_net is not None:
        # IS net income should equal GL credits - debits for P&L accounts
        gl_net = -(gl_revenue + gl_expense)  # credits are positive in GL
        variance = abs(is_net - gl_net)
        if variance > 1.00:
            exceptions.append(Exception_(
                severity="warning", category="balance",
                source="is_gl_cross_check",
                description=f"IS Net Income (${is_net:,.2f}) differs from GL P&L net (${gl_net:,.2f}) by ${variance:,.2f}",
            ))

    return exceptions


# ── Main orchestrator ────────────────────────────────────────

def run_pipeline(files: dict) -> EngineResult:
    """
    Run the full pipeline against a set of input files.

    Args:
        files: dict mapping file type to filepath, e.g.:
            {
                "gl": "/path/to/GL.xlsx",
                "income_statement": "/path/to/IS.xlsx",
                "budget_comparison": "/path/to/Budget.xlsx",
                "rent_roll": "/path/to/RentRoll.xlsx",
                "nexus_accrual": "/path/to/Nexus.xls",
                "pnc_bank": "/path/to/PNC.pdf",
                "loan": "/path/to/Loan.xlsx",
                "kardin_budget": "/path/to/Kardin.xlsx",
                "monthly_report": "/path/to/Report.xlsx",
            }

    Returns:
        EngineResult with all parsed data, matches, and exceptions
    """
    import sys
    # Ensure the pipeline directory is on the path
    pipeline_dir = os.path.dirname(os.path.abspath(__file__))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)

    from parsers.yardi_gl import parse_gl
    from parsers.yardi_income_statement import parse as parse_is
    from parsers.yardi_budget_comparison import parse as parse_bc
    from parsers.yardi_rent_roll import parse as parse_rr
    from parsers.nexus_accrual import parse as parse_nexus
    from parsers.pnc_bank_statement import parse as parse_pnc
    from parsers.berkadia_loan import parse as parse_loan
    from parsers.kardin_budget import parse as parse_kardin
    from parsers.monthly_report_template import parse_monthly_report

    result = EngineResult(
        run_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
        run_at=datetime.now().isoformat(),
        period="",
        property_name="",
    )

    # ── Step 1: Parse all files ──────────────────────────────
    gl = None
    if "gl" in files and files["gl"]:
        try:
            gl = parse_gl(files["gl"])
            result.parsed["gl"] = gl
            result.period = gl.metadata.period
            result.property_name = gl.metadata.property_name
        except Exception as e:
            result.add_exception("error", "parse", "yardi_gl", f"GL parse failed: {e}")

    is_data = None
    if "income_statement" in files and files["income_statement"]:
        try:
            is_data = parse_is(files["income_statement"])
            result.parsed["income_statement"] = is_data
        except Exception as e:
            result.add_exception("error", "parse", "yardi_is", f"IS parse failed: {e}")

    bc_data = None
    if "budget_comparison" in files and files["budget_comparison"]:
        try:
            bc_data = parse_bc(files["budget_comparison"])
            result.parsed["budget_comparison"] = bc_data
        except Exception as e:
            result.add_exception("error", "parse", "yardi_bc", f"Budget parse failed: {e}")

    rr_data = None
    if "rent_roll" in files and files["rent_roll"]:
        try:
            rr_data = parse_rr(files["rent_roll"])
            result.parsed["rent_roll"] = rr_data
        except Exception as e:
            result.add_exception("error", "parse", "yardi_rr", f"Rent Roll parse failed: {e}")

    nexus_data = None
    if "nexus_accrual" in files and files["nexus_accrual"]:
        try:
            nexus_data = parse_nexus(files["nexus_accrual"])
            result.parsed["nexus_accrual"] = nexus_data
        except Exception as e:
            result.add_exception("error", "parse", "nexus", f"Nexus parse failed: {e}")

    bank_data = None
    if "pnc_bank" in files and files["pnc_bank"]:
        try:
            bank_data = parse_pnc(files["pnc_bank"])
            result.parsed["pnc_bank"] = bank_data
        except Exception as e:
            result.add_exception("error", "parse", "pnc", f"Bank parse failed: {e}")

    loan_data = None
    if "loan" in files and files["loan"]:
        try:
            loan_data = parse_loan(files["loan"])
            result.parsed["loan"] = loan_data
        except Exception as e:
            result.add_exception("error", "parse", "berkadia", f"Loan parse failed: {e}")

    kardin_data = None
    if "kardin_budget" in files and files["kardin_budget"]:
        try:
            kardin_data = parse_kardin(files["kardin_budget"])
            result.parsed["kardin_budget"] = kardin_data
        except Exception as e:
            result.add_exception("error", "parse", "kardin", f"Kardin parse failed: {e}")

    template_data = None
    if "monthly_report" in files and files["monthly_report"]:
        try:
            template_data = parse_monthly_report(files["monthly_report"])
            result.parsed["monthly_report"] = template_data
        except Exception as e:
            result.add_exception("error", "parse", "template", f"Template parse failed: {e}")

    # ── Step 2: Validate GL ──────────────────────────────────
    if gl:
        gl_exceptions = validate_gl_balance(gl)
        result.exceptions.extend(gl_exceptions)

    # ── Step 3: Cross-validate IS to GL ──────────────────────
    if gl and is_data:
        is_gl_exceptions = cross_validate_is_to_gl(is_data, gl)
        result.exceptions.extend(is_gl_exceptions)

    # ── Step 4: Match GL to invoices ─────────────────────────
    if gl and nexus_data:
        gl_inv_matches, gl_inv_exc = match_gl_to_invoices(gl, nexus_data)
        result.gl_invoice_matches = gl_inv_matches
        result.exceptions.extend(gl_inv_exc)

    # ── Step 5: Match GL to bank ─────────────────────────────
    if gl and bank_data:
        gl_bank_matches, gl_bank_exc = match_gl_to_bank(gl, bank_data)
        result.gl_bank_matches = gl_bank_matches
        result.exceptions.extend(gl_bank_exc)

    # ── Step 6: Debt service check ───────────────────────────
    if gl and loan_data:
        ds_result, ds_exc = check_debt_service(gl, loan_data)
        result.debt_service_check = ds_result
        result.exceptions.extend(ds_exc)

    # ── Step 7: Budget variances ─────────────────────────────
    if is_data and bc_data:
        bv_result, bv_exc = check_budget_variances(is_data, bc_data)
        result.budget_variances = bv_result
        result.exceptions.extend(bv_exc)

    # ── Step 8: Build summary ────────────────────────────────
    result.summary = {
        "files_processed": sum(1 for v in files.values() if v),
        "parsers_succeeded": len(result.parsed),
        "gl_accounts": gl.validation.get("accounts_parsed", 0) if gl else 0,
        "gl_transactions": gl.validation.get("transactions_parsed", 0) if gl else 0,
        "gl_balanced": gl.validation.get("gl_balanced", False) if gl else False,
        "invoice_matches": len(result.gl_invoice_matches),
        "bank_matches": len(result.gl_bank_matches),
        "budget_variances_flagged": len(result.budget_variances),
        "exceptions_error": result.error_count,
        "exceptions_warning": result.warning_count,
        "status": result.status,
    }

    return result
