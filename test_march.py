"""
March 2026 pipeline test — runs full pipeline and reports results.
"""
import sys, os, tempfile, traceback
sys.path.insert(0, r'C:\Users\RyanCWalsh\.claude\ga-automation\pipeline')

OUT = tempfile.mkdtemp(prefix='revlabs_mar26_')
print(f'Output dir: {OUT}')

FILES = {
    'bank_rec':          r'C:\Users\RyanCWalsh\AppData\Local\Temp\Bank_Rec (41) (1).pdf',
    'gl':                r'C:\Users\RyanCWalsh\Downloads\GeneralLedger_revlabpm_Accrual (2).xlsx',
    'trial_balance':     r'C:\Users\RyanCWalsh\Downloads\Trial_Balance_revlabpm_Accrual (2).xlsx',
    'budget_comparison': r'C:\Users\RyanCWalsh\Downloads\Budget_Comparison_revlabpm_Accrual (2).xlsx',
    'loan': [
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159010__2026__Apr.PDF',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159011__2026__Apr.pdf',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159012__2026__Apr.PDF',
    ],
    'daca_bank':  r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Bank Statements\2026\2026.3 Revolution Labs Owner LLC x5132.pdf',
    'pnc_bank':   r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Claude\Raw PNC Data - Mar.pdf',
    'kardin_budget': r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.02\Beta\Kardin 2026 Budget.xlsx',
}

# ── Engine ────────────────────────────────────────────────────
print('\n=== ENGINE ===')
from engine import run_pipeline
result = run_pipeline(FILES)
gl_parsed = result.parsed.get('gl')
bc_parsed = result.parsed.get('budget_comparison') or []
period    = result.period
prop      = result.property_name
print(f'Period: {period}   Property: {prop}')
print(f'Parsers: {list(result.parsed.keys())}')
print(f'GL: {result.summary.get("gl_accounts")} accounts, {result.summary.get("gl_transactions")} txns, balanced={result.summary.get("gl_balanced")}')
print(f'Exceptions: {result.summary.get("exceptions_error")} errors, {result.summary.get("exceptions_warning")} warnings')
for e in result.exceptions:
    print(f'  [{e.severity.upper()}] {e.source}: {e.description[:100]}')

# ── Trial Balance ─────────────────────────────────────────────
print('\n=== TRIAL BALANCE ===')
try:
    from parsers.yardi_trial_balance import parse as parse_tb
    tb_result = parse_tb(FILES['trial_balance'])
    print(f'TB accounts: {len(tb_result.accounts)}')
    print(f'TB period: {tb_result.metadata.period}  book: {tb_result.metadata.book}')
except Exception as e:
    tb_result = None
    print(f'TB FAILED: {e}')
    traceback.print_exc()

# ── Yardi Bank Rec ────────────────────────────────────────────
print('\n=== YARDI BANK REC ===')
try:
    from parsers.yardi_bank_rec import parse as parse_bank_rec
    bank_rec = parse_bank_rec(FILES['bank_rec'])
    print(f'Bank balance:       ${bank_rec.get("bank_statement_balance", 0):>14,.2f}')
    print(f'Outstanding checks: ${bank_rec.get("total_outstanding_checks", 0):>14,.2f}  ({len(bank_rec.get("outstanding_checks", []))} items)')
    print(f'Reconciled balance: ${bank_rec.get("reconciled_bank_balance", 0):>14,.2f}')
    print(f'GL balance (111100):${bank_rec.get("gl_balance", 0):>14,.2f}')
    print(f'Difference:         ${bank_rec.get("reconciling_difference", 0):>14,.2f}')
    if bank_rec.get('_parse_error'):
        print(f'Parse error: {bank_rec["_parse_error"]}')
except Exception as e:
    bank_rec = None
    print(f'Bank rec FAILED: {e}')
    traceback.print_exc()

# ── DACA ──────────────────────────────────────────────────────
print('\n=== DACA BANK STATEMENT ===')
try:
    from parsers.keybank_daca import parse as parse_daca
    daca = parse_daca(FILES['daca_bank'])
    print(f'Account:            {daca.get("account_number")}')
    print(f'Period:             {daca.get("statement_period")}')
    print(f'Beginning balance:  ${(daca.get("beginning_balance") or 0):>14,.2f}')
    print(f'Ending balance:     ${(daca.get("ending_balance") or 0):>14,.2f}')
    if daca.get('_parse_error'):
        print(f'Parse error: {daca["_parse_error"]}')
    # GL 115100 ending
    gl_115100 = 0.0
    if gl_parsed:
        for a in (gl_parsed.accounts or []):
            if a.account_code == '115100':
                gl_115100 = a.ending_balance
                break
    print(f'GL balance (115100):${gl_115100:>14,.2f}')
    print(f'Difference:         ${(daca.get("ending_balance") or 0) - gl_115100:>14,.2f}')
except Exception as e:
    daca = None
    print(f'DACA FAILED: {e}')
    traceback.print_exc()

# ── Accruals ──────────────────────────────────────────────────
print('\n=== ACCRUAL ENTRIES ===')
try:
    from accrual_entry_generator import build_accrual_entries, generate_yardi_je_csv
    # Manual overrides — amounts not derivable from GL (semi-annual billing, etc.)
    # For March 2026 test: use JLL's known water/sewer semi-annual amount
    manual_accruals = [
        {
            'account_code': '613310',
            'account_name': 'Utilities-Water/Sewer',
            'amount':        round(99814.50 / 6, 2),   # $16,635.75/month
            'description':   'Water/Sewer semi-annual accrual: invoice $99,814.50 / 6 months = $16,635.75/month',
        },
    ]
    je_lines = build_accrual_entries(
        [],
        period=period, property_name=prop,
        gl_data=gl_parsed, budget_data=bc_parsed,
        manual_accruals=manual_accruals,
    )
    dr_lines = [l for l in je_lines if (l.get('debit') or 0) > 0]
    print(f'Total JE lines: {len(je_lines)}  ({len(dr_lines)} debit entries)')
    for l in dr_lines:
        src  = l.get('source', '')
        acct = l.get('account_code', '')
        amt  = l.get('debit') or 0
        desc = (l.get('description') or '')[:65]
        print(f'  {l.get("je_number",""):10s}  {acct:8s}  DR ${amt:>10,.2f}  [{src}]  {desc}')
except Exception as e:
    je_lines = []
    print(f'ACCRUALS FAILED: {e}')
    traceback.print_exc()

# ── Management fee ────────────────────────────────────────────
print('\n=== MANAGEMENT FEE ===')
try:
    from management_fee import calculate as calc_fee, build_management_fee_je
    fee = calc_fee(gl_parsed=gl_parsed, budget_rows=bc_parsed, daca_parsed=daca)
    print(f'Cash received:  ${fee.cash_received:>12,.2f}  (source: {fee.cash_source})')
    print(f'JLL ({fee.jll_rate:.2%}):    ${fee.jll_fee:>12,.2f}')
    print(f'GRP ({fee.grp_rate:.2%}):    ${fee.grp_fee:>12,.2f}')
    print(f'Total ({fee.total_rate:.2%}): ${fee.total_fee:>12,.2f}')
    fee_je = build_management_fee_je(fee, period=period, property_code='revlabpm', je_number='MGT-001')
    fee_dr = [l for l in fee_je if (l.get('debit') or 0) > 0]
    for l in fee_dr:
        print(f'  {l.get("account_code",""):8s}  DR ${(l.get("debit") or 0):>10,.2f}  {(l.get("description") or "")[:60]}')
except Exception as e:
    fee = None
    fee_je = []
    print(f'MGMT FEE FAILED: {e}')
    traceback.print_exc()

# ── Loan statements ───────────────────────────────────────────
print('\n=== LOAN STATEMENTS ===')
loan_data = result.parsed.get('loan') or []
for loan in loan_data:
    if isinstance(loan, dict):
        name = loan.get('loan_number') or loan.get('property') or 'Unknown'
        bal  = loan.get('principal_balance') or loan.get('ending_balance') or 0
        pmt  = loan.get('payment_total') or loan.get('total_payment') or loan.get('payment_amount') or 0
        print(f'  Loan {name}: balance=${bal:,.2f}  payment=${pmt:,.2f}')
    else:
        print(f'  Loan: {type(loan)}')
if not loan_data:
    print('  No loan data parsed')

# ── BS Workpaper ──────────────────────────────────────────────
print('\n=== BS WORKPAPER ===')
try:
    import bs_workpaper_generator
    bs_path = os.path.join(OUT, 'RevLabs_Mar2026_BS_Workpaper.xlsx')
    # GL cash balance for bank rec tab
    gl_cash = 0.0
    gl_daca = 0.0
    if gl_parsed:
        for a in (gl_parsed.accounts or []):
            if a.account_code == '111100': gl_cash = a.ending_balance
            if a.account_code == '115100': gl_daca = a.ending_balance

    bs_workpaper_generator.generate(
        gl_result=gl_parsed,
        tb_result=tb_result,
        output_path=bs_path,
        period=period,
        property_name=prop,
        prepaid_ledger_active=[],
        bank_rec_data=bank_rec,
        gl_cash_balance=gl_cash,
        daca_bank_data=daca,
        daca_gl_balance=gl_daca,
    )
    print(f'BS Workpaper: {bs_path}')
    # Report GL vs TB variances
    if tb_result:
        tb_map = {a.account_code: a for a in tb_result.accounts}
        print('\n  Account        GL Ending        TB Ending       Variance  Status')
        print('  ' + '-'*75)
        bs_accts = [a for a in gl_parsed.accounts if '100000' <= a.account_code <= '399999']
        for a in bs_accts:
            tb_a = tb_map.get(a.account_code)
            tb_end = tb_a.ending_balance if tb_a else None
            var = (a.ending_balance - tb_end) if tb_end is not None else None
            status = 'CLEAN' if (var is not None and abs(var) < 0.02) else ('NO TB' if tb_end is None else 'VARIANCE')
            tb_str = f'{tb_end:>14,.2f}' if tb_end is not None else '       N/A in TB'
            var_str = f'{var:>12,.2f}' if var is not None else '            N/A'
            print(f'  {a.account_code}  {a.account_name[:22]:22s}  {a.ending_balance:>14,.2f}  {tb_str}  {var_str}  {status}')
except Exception as e:
    print(f'BS WORKPAPER FAILED: {e}')
    traceback.print_exc()

# ── JE CSVs ───────────────────────────────────────────────────
print('\n=== JE CSVs ===')
try:
    from accrual_entry_generator import generate_yardi_je_csv
    all_je = je_lines + fee_je
    accrual_sources = {'nexus', 'budget_gap', 'historical', 'management_fee'}
    accrual_lines = [l for l in all_je if l.get('source') in accrual_sources]
    if accrual_lines:
        path = os.path.join(OUT, 'RevLabs_Mar2026_Accruals_JE.csv')
        generate_yardi_je_csv(accrual_lines, path, period=period, property_code='revlabpm')
        print(f'Accruals JE CSV: {path}  ({len(accrual_lines)} lines)')
    else:
        print('Accruals JE: no lines')
except Exception as e:
    print(f'JE CSV FAILED: {e}')
    traceback.print_exc()

# ── QC ────────────────────────────────────────────────────────
print('\n=== QC ===')
try:
    import re
    from qc_engine import run_qc, generate_qc_workbook
    month_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
    m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', period or '')
    period_month = month_map.get(m.group(1), 1) if m else 1

    qc = run_qc(
        budget_rows=bc_parsed,
        tb_result=tb_result,
        gl_parsed=gl_parsed,
        kardin_records=result.parsed.get('kardin_budget') or [],
        accrual_entries=je_lines,
        period=period,
        property_name=prop,
        period_month=period_month,
    )
    print(f'QC Overall: {qc.overall_status}')
    for chk in qc.checks:
        icon = 'PASS' if chk.status == 'PASS' else ('FLAG' if chk.status == 'FLAG' else 'FAIL')
        print(f'  [{icon}] {chk.check_id}: {chk.check_name} — {chk.summary[:80]}')
    qc_path = os.path.join(OUT, 'RevLabs_Mar2026_QC.xlsx')
    generate_qc_workbook(qc, qc_path)
    print(f'QC Workbook: {qc_path}')
except Exception as e:
    print(f'QC FAILED: {e}')
    traceback.print_exc()

print(f'\n=== DONE — all outputs in {OUT} ===')
