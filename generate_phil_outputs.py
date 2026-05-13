"""
Generate March 2026 bake-off outputs for Phil Dorman.

Produces three files in C:\\Users\\RyanCWalsh\\Desktop\\RevLabs_Mar2026_Phil\\ :
  1. RevLabs_Mar2026_Monthly_Report.xlsx  — 8-tab Singerman workbook
  2. RevLabs_Mar2026_Exception_Report.xlsx — 5-tab exception/QC report
  3. RevLabs_Mar2026_Accruals_JE.csv       — Yardi-import accrual JE CSV
"""
import sys, os, traceback

sys.path.insert(0, r'C:\Users\RyanCWalsh\.claude\ga-automation\pipeline')

OUT = r'C:\Users\RyanCWalsh\Desktop\RevLabs_Mar2026_Phil'
os.makedirs(OUT, exist_ok=True)
print(f'Output folder: {OUT}\n')

FILES = {
    'bank_rec':          r'C:\Users\RyanCWalsh\AppData\Local\Temp\Bank_Rec (41) (1).pdf',
    'gl':                r'C:\Users\RyanCWalsh\Downloads\GeneralLedger_revlabspm_Accrual (2).xlsx',
    'trial_balance':     r'C:\Users\RyanCWalsh\Downloads\Trial_Balance_revlabspm_Accrual (2).xlsx',
    'budget_comparison': r'C:\Users\RyanCWalsh\Downloads\Budget_Comparison_revlabspm_Accrual (2).xlsx',
    'loan': [
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159010__2026__Apr.PDF',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159011__2026__Apr.pdf',
        r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.03\Billing__011159012__2026__Apr.PDF',
    ],
    'daca_bank':     r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Bank Statements\2026\2026.3 Revolution Labs Owner LLC x5132.pdf',
    'pnc_bank':      r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Bank Statements\2026\Raw PNC Data - Mar.pdf',
    'kardin_budget': r'C:\Users\RyanCWalsh\Greatland Realty Partners\Greatland Partners - Documents\Portfolio\Revolution Labs\10 - Finance\Accounting\Workpapers + Financials\2026.02\Beta\Kardin 2026 Budget.xlsx',
}

# ── 1. Run engine ─────────────────────────────────────────────
print('Running pipeline...')
from engine import run_pipeline
from parsers.keybank_daca import parse as parse_daca
from accrual_entry_generator import (
    build_accrual_entries, generate_yardi_je_csv,
)
from management_fee import calculate as calc_fee, build_management_fee_je
from report_generator import generate_report
from parsers.yardi_trial_balance import parse as parse_tb
import bs_workpaper_generator

result = run_pipeline(FILES)
gl     = result.parsed.get('gl')
bc     = result.parsed.get('budget_comparison') or []
period = result.period
prop   = result.property_name
print(f'  Period: {period}   Property: {prop}')
print(f'  GL: {result.summary.get("gl_accounts")} accounts, balanced={result.summary.get("gl_balanced")}')
print(f'  Exceptions: {result.summary.get("exceptions_error")} errors, {result.summary.get("exceptions_warning")} warnings')

# ── 2. Build accrual JEs ──────────────────────────────────────
print('\nBuilding accrual entries...')
manual_accruals = [
    {
        'account_code': '613310',
        'account_name': 'Utilities-Water/Sewer',
        'amount':        round(99814.50 / 6, 2),
        'description':   'Water/Sewer semi-annual accrual: invoice $99,814.50 / 6 months = $16,635.75/month',
    },
]
je_lines = build_accrual_entries(
    [],
    period=period, property_name=prop,
    gl_data=gl, budget_data=bc,
    manual_accruals=manual_accruals,
)
dr_count = sum(1 for l in je_lines if (l.get('debit') or 0) > 0)
print(f'  {len(je_lines)} JE lines ({dr_count} debit entries)')

# ── 3. Management fee JE ──────────────────────────────────────
print('Building management fee...')
try:
    daca = parse_daca(FILES['daca_bank'])
except Exception:
    daca = None
fee    = calc_fee(gl_parsed=gl, budget_rows=bc, daca_parsed=daca)
fee_je = build_management_fee_je(fee, period=period, property_code='revlabspm', je_number='MGT-001')
print(f'  Total fee: ${fee.total_fee:,.2f}  (source: {fee.cash_source})')

# ── 5. Assemble all JE lines ──────────────────────────────────
# Note: prepaid_amortization entries (INS/TAX) are already inside je_lines
# from build_accrual_entries — no separate prepaid_release call needed here.
all_je = je_lines + fee_je

_accrual_sources = {
    'nexus', 'budget_gap', 'historical', 'management_fee', 'management_fee_catchup',
    'invoice_proration', 'prepaid_amortization', 'contract_supplement',
    'tenant_utility_billing', 'bonus_accrual', 'manual',
}
accrual_lines = [l for l in all_je if l.get('source') in _accrual_sources]

# ── 6. Inject accruals into engine_result so reports see them ─
result.parsed['accrual_entries'] = accrual_lines

# ── 7. Generate 8-tab Singerman workbook ──────────────────────
print('\nGenerating Singerman workbook...')
report_path = os.path.join(OUT, 'RevLabs_Mar2026_Monthly_Report.xlsx')
try:
    generate_report(result, report_path)
    print(f'  [OK] {report_path}')
except Exception as e:
    print(f'  [FAIL] FAILED: {e}')
    traceback.print_exc()

# ── 8. Generate BS workpaper (Balance Sheet account reconciliation) ───────────
print('Generating BS workpaper...')
workpaper_path = os.path.join(OUT, 'RevLabs_Mar2026_BS_Workpaper.xlsx')
try:
    tb_result = parse_tb(FILES['trial_balance'])
    bank_rec  = result.parsed.get('bank_rec')
    daca      = result.parsed.get('daca_bank')
    gl_cash   = next((a.ending_balance for a in gl.accounts if a.account_code == '111100'), 0.0)
    gl_daca   = next((a.ending_balance for a in gl.accounts if a.account_code == '115100'), 0.0)

    # Build BS pro-forma adjustments — project ending balances after pipeline JEs are posted.
    # Sign convention:
    #   Asset (1xxxxx):           DR increases balance → delta = debit - credit
    #   Liability/Equity (2-3x):  CR increases balance → delta = credit - debit
    bs_je_adjustments = {}
    for _line in all_je:
        _code = str(_line.get('account_code', '') or '').strip()
        if not _code or not ('100000' <= _code <= '399999'):
            continue
        _net_debit = float(_line.get('debit', 0) or 0) - float(_line.get('credit', 0) or 0)
        _delta = _net_debit if _code.startswith('1') else -_net_debit
        bs_je_adjustments[_code] = bs_je_adjustments.get(_code, 0.0) + _delta

    print(f'  BS accounts with pipeline JE adjustments: '
          f'{[f"{k}: {v:+,.2f}" for k, v in sorted(bs_je_adjustments.items()) if abs(v) > 0.01]}')

    bs_workpaper_generator.generate(
        gl_result=gl,
        tb_result=tb_result,
        output_path=workpaper_path,
        period=period,
        property_name=prop,
        prepaid_ledger_active=[],
        bank_rec_data=bank_rec,
        gl_cash_balance=gl_cash,
        daca_bank_data=daca,
        daca_gl_balance=gl_daca,
        je_adjustments=bs_je_adjustments,
    )
    print(f'  [OK] {workpaper_path}')
except Exception as e:
    print(f'  [FAIL] FAILED: {e}')
    traceback.print_exc()

# ── 9. Generate accrual JE CSV ────────────────────────────────
print('Generating accrual JE CSV...')
csv_path = os.path.join(OUT, 'RevLabs_Mar2026_Accruals_JE.csv')
try:
    generate_yardi_je_csv(accrual_lines, csv_path, period=period, property_code='revlabspm')
    print(f'  [OK] {csv_path}  ({len(accrual_lines)} lines)')
except Exception as e:
    print(f'  [FAIL] FAILED: {e}')
    traceback.print_exc()

# ── Summary ───────────────────────────────────────────────────
print(f'\n{"="*60}')
print('FILES READY FOR PHIL:')
for f in [report_path, workpaper_path, csv_path]:
    exists = '[OK]' if os.path.exists(f) else '[FAIL] MISSING'
    print(f'  {exists}  {os.path.basename(f)}')
print(f'\nFolder: {OUT}')
