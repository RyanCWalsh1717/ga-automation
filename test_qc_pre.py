"""
Pre-test QC — validates all pipeline components before JLL comparison run.
"""
import sys, re
sys.path.insert(0, r'C:\Users\RyanCWalsh\.claude\ga-automation\pipeline')
from collections import Counter, defaultdict
from engine import run_pipeline
from accrual_entry_generator import build_accrual_entries
from parsers.yardi_trial_balance import parse as parse_tb
from parsers.yardi_bank_rec import parse as parse_bank_rec
from parsers.berkadia_loan import parse as parse_berkadia
from parsers.keybank_daca import parse as parse_daca
from management_fee import calculate as calc_fee, build_management_fee_je

FILES = {
    'gl':                r'C:\Users\RyanCWalsh\Downloads\GeneralLedger_revlabpm_Accrual (2).xlsx',
    'trial_balance':     r'C:\Users\RyanCWalsh\Downloads\Trial_Balance_revlabpm_Accrual (2).xlsx',
    'budget_comparison': r'C:\Users\RyanCWalsh\Downloads\Budget_Comparison_revlabpm_Accrual (2).xlsx',
    'bank_rec':          r'C:\Users\RyanCWalsh\AppData\Local\Temp\Bank_Rec (41) (1).pdf',
    'daca_bank':         r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Bank Statements\2026\2026.3 Revolution Labs Owner LLC x5132.pdf',
    'loan': [
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159010__2026__Apr.PDF',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159011__2026__Apr.pdf',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159012__2026__Apr.PDF',
    ],
    'kardin_budget': r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.02\Beta\Kardin 2026 Budget.xlsx',
}

result   = run_pipeline(FILES)
gl       = result.parsed.get('gl')
bc       = result.parsed.get('budget_comparison') or []
period   = result.period
prop     = result.property_name
tb       = parse_tb(FILES['trial_balance'])
bank_rec = parse_bank_rec(FILES['bank_rec'])
daca     = parse_daca(FILES['daca_bank'])
loans    = result.parsed.get('loan') or []

manual = [{
    'account_code': '613310',
    'account_name': 'Utilities-Water/Sewer',
    'amount':       round(99814.50 / 6, 2),
    'description':  'Water/Sewer semi-annual accrual $99,814.50/6',
}]
je_lines = build_accrual_entries([], period=period, property_name=prop,
    gl_data=gl, budget_data=bc, manual_accruals=manual)
fee     = calc_fee(gl_parsed=gl, budget_rows=bc, daca_parsed=daca)
fee_je  = build_management_fee_je(fee, period=period,
              property_code='revlabpm', je_number='MGT-001')
je_lines = je_lines + fee_je

dr = [l for l in je_lines if (l.get('debit') or 0) > 0]
cr = [l for l in je_lines if (l.get('credit') or 0) > 0]

PASS = 'PASS'
FAIL = 'FAIL'
WARN = 'WARN'

results = []

def chk(label, ok, detail=''):
    status = PASS if ok else FAIL
    results.append((status, label, detail))

def chk_warn(label, ok, detail=''):
    status = PASS if ok else WARN
    results.append((status, label, detail))

# ── 1. Accrual entry balance ──────────────────────────────────
dr_total = sum(l.get('debit', 0) for l in dr)
cr_total = sum(l.get('credit', 0) for l in cr)
chk('JE balanced (DR = CR)', abs(dr_total - cr_total) < 0.02,
    f'DR ${dr_total:,.2f}  CR ${cr_total:,.2f}  diff=${abs(dr_total-cr_total):.2f}')

# No duplicate DR accounts
acct_counts = Counter(l.get('account_code') for l in dr)
dupes = {k: v for k, v in acct_counts.items() if v > 1}
chk('No duplicate DR accounts', not dupes,
    f'Duplicates: {dupes}' if dupes else f'{len(dr)} entries, all unique')

# ── 2. Source breakdown present ───────────────────────────────
by_source = defaultdict(list)
for l in dr:
    by_source[l.get('source', '?')].append(l)

expected_sources = ['manual', 'prepaid_amortization', 'invoice_proration',
                    'budget_gap', 'management_fee']
for src in expected_sources:
    lines = by_source.get(src, [])
    total = sum(l.get('debit', 0) for l in lines)
    chk(f'Source present: {src}', len(lines) > 0,
        f'{len(lines)} entries  ${total:,.2f}')

# ── 3. Key fixed amounts ──────────────────────────────────────
dr_map = {l.get('account_code'): l for l in dr}

def get_dr(acct):
    return dr_map.get(acct, {}).get('debit', 0.0)

def get_dr_src(acct, src):
    return next((l.get('debit', 0) for l in dr
                 if l.get('account_code') == acct and l.get('source') == src), 0.0)

checks_amt = [
    ('Management fee',       fee.total_fee,                                   42570.34, 1.0),
    ('Bank rec diff',        float(bank_rec.get('reconciling_difference', 0) or 0), 0.00, 0.02),
    ('DACA diff',            float(daca.get('ending_balance') or 0) -
                             next((a.ending_balance for a in gl.accounts
                                   if a.account_code == '115100'), 0),         0.00, 0.02),
    ('Water/Sewer monthly',  get_dr_src('613310', 'manual'),                 16635.75, 1.0),
    ('Insurance amort total',sum(get_dr(a) for a in ('639110','639120')),     5576.00, 2.0),
    ('RE Tax amort',         get_dr('641110'),                               249375.40, 1.0),
    ('Casella trash',        get_dr('610160'),                                 3328.98, 0.02),
    ('HVAC contract flag',   get_dr('617110'),                                 1000.00, 0.02),
    ('Electricity proration',get_dr('613110'),                                67554.94, 1.0),
    ('Gas proration',        get_dr('613210'),                                70462.91, 1.0),
]
for label, actual, expected, tol in checks_amt:
    diff = abs(actual - expected)
    chk(label, diff <= tol,
        f'actual=${actual:,.2f}  expected=${expected:,.2f}  diff=${diff:,.2f}')

# ── 4. Loan payments ──────────────────────────────────────────
loan_expected = {'11159010': 606743.95, '11159011': 210260.57, '11159012': 23362.29}
for loan in loans:
    if not isinstance(loan, dict):
        continue
    ln  = loan.get('loan_number', '')
    pmt = float(loan.get('payment_total') or 0)
    exp = loan_expected.get(ln, -1)
    if exp < 0:
        continue
    chk(f'Loan {ln} payment', abs(pmt - exp) < 1.0,
        f'${pmt:,.2f}  (expected ${exp:,.2f})')
    chk(f'Loan {ln} balance > 0', float(loan.get('principal_balance') or 0) > 0,
        f'${float(loan.get("principal_balance", 0)):,.2f}')

# ── 5. BS workpaper tie-out ───────────────────────────────────
tb_map = {a.account_code: a for a in tb.accounts}
bs_accts = [a for a in gl.accounts if '100000' <= a.account_code <= '399999']
chk('BS accounts count', len(bs_accts) == 18, f'{len(bs_accts)} accounts')
for a in bs_accts:
    tb_a = tb_map.get(a.account_code)
    if tb_a:
        var = abs(a.ending_balance - tb_a.ending_balance)
        chk(f'BS tie {a.account_code}', var < 0.02,
            f'GL=${a.ending_balance:,.2f}  TB=${tb_a.ending_balance:,.2f}  var=${var:,.2f}')

# ── 6. GL balanced ────────────────────────────────────────────
chk('GL balanced', result.summary.get('gl_balanced', False),
    f'{result.summary.get("gl_transactions")} txns, '
    f'{result.summary.get("gl_accounts")} accounts')

# ── 7. Engine errors ──────────────────────────────────────────
n_err = result.summary.get('exceptions_error', 0)
chk('No engine errors', n_err == 0, f'{n_err} errors')

# ── Coverage: key accounts present ───────────────────────────
key_coverage = {
    '613310': 'manual',
    '639110': 'prepaid_amortization',
    '641110': 'prepaid_amortization',
    '613110': 'invoice_proration',
    '613210': 'invoice_proration',
    '615110': 'invoice_proration',
    '637110': 'invoice_proration',
    '610160': 'invoice_proration',
    '617110': 'budget_gap',
    '637130': 'management_fee',
}
for acct, src in key_coverage.items():
    entry = next((l for l in dr if l.get('account_code') == acct
                  and l.get('source') == src), None)
    chk(f'Coverage {acct} [{src[:8]}]', entry is not None,
        f'${entry["debit"]:,.2f}' if entry else 'MISSING')

# ── Print results ─────────────────────────────────────────────
print()
print('=' * 68)
print('  REVOLUTION LABS — MARCH 2026  |  PRE-TEST QC REPORT')
print('=' * 68)

passes = sum(1 for s, _, _ in results if s == PASS)
fails  = sum(1 for s, _, _ in results if s == FAIL)
warns  = sum(1 for s, _, _ in results if s == WARN)

for status, label, detail in results:
    icon = {'PASS': 'PASS', 'FAIL': '*** FAIL ***', 'WARN': 'WARN'}[status]
    print(f'  [{icon:12s}]  {label:<38s}  {detail}')

print()
print('=' * 68)
print(f'  {passes} PASS  |  {warns} WARN  |  {fails} FAIL')
overall = 'ALL CLEAR — ready for JLL comparison' if fails == 0 else 'FAILURES DETECTED — fix before proceeding'
print(f'  {overall}')
print('=' * 68)
