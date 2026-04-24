# GA Automation Pipeline

## What This Is

Monthly accounting close automation for **Greatland Realty Partners** (GRP), a CRE firm managing 2M+ SF in Greater Boston. The beta property is **Revolution Labs** at 275 Grove Street, Newton, MA (Yardi property code: `revlabpm`). The capital partner is **Singerman Real Estate Partners**, who receives institutional-quality deliverables each month.

Deployed as a Streamlit web app at: `https://ga-automation-v2ebmljbewawn5pj9ivvbd.streamlit.app/`
GitHub repo: `https://github.com/RyanCWalsh1717/ga-automation`

---

## Architecture

```
app.py                          ← Streamlit UI: file uploads, two manual-entry tables,
                                  run button, results dashboard, download buttons.

pipeline/
  engine.py                     ← Core orchestrator. Parses all files, runs cross-source
                                  validation, bank reconciliation. Returns EngineResult.
                                  Prefers Yardi Bank Rec PDF over raw PNC for reconciliation.

  management_fee.py             ← Computes monthly management fee accrual.
                                  JLL 1.25% + GRP 1.75% = 3.00% on cash received.
                                  Cash-received priority: DACA additions → GL 111100 debits
                                  → revenue proxy → manual override.
                                  Also detects prior-period catch-up (unmatched auto-reversals
                                  in 637130) and generates MGT-002 catch-up JE.

  accrual_entry_generator.py    ← 5-layer accrual entry detection (deduped against GL):
                                  Layer 1: Nexus open invoices
                                  Layer 2: Invoice proration (cost-per-day × days remaining
                                           for recurring vendors already in GL)
                                  Layer 3: Budget gap detection (with seasonality filters,
                                           escrow exclusions, materiality floor $500)
                                  Layer 4: Historical recurring pattern detection
                                  Layer 5: Payroll bonus accruals (Kardin-driven:
                                           annual ÷ 12 − min monthly; suppressed on
                                           payment months when GL ≥ monthly avg)

  qc_engine.py                  ← 7-point QC checklist run after accruals are built:
                                  CHECK_1: TB → BC tie-out
                                  CHECK_2: Budget variances (Tier 1/2 flags)
                                  CHECK_3: Workpapers → Trial Balance tie-out
                                  CHECK_4: Month-over-month swings (> $10K)
                                  CHECK_5: BS workpaper tie-out
                                  CHECK_6: Accruals vs budget coverage
                                  CHECK_7: Miscellaneous (insurance, mgmt fee, etc.)

  prepaid_ledger.py             ← Prepaid amortization schedule. Tracks insurance and
                                  other prepaid items; generates monthly amortization JEs
                                  and release entries (separate from accruals).

  report_generator.py           ← Generates 8-tab Singerman deliverable workbook:
                                  BS, IS, T12, TB-MTD, TB-YTD, GL-MTD, GL-YTD, Tenancy.
                                  Also generates 5-tab exception/QC report.

  workpaper_generator.py        ← Generates institutional workpapers: Bank Rec, Debt
                                  Service, Rent Roll Tie-Out, Accruals summary.
                                  Consumes BankReconDetail from engine.

  bs_workpaper_generator.py     ← Generates Balance Sheet workpaper: GL vs TB tie-out
                                  for all BS accounts, DACA reconciliation.

  variance_comments.py          ← Variance commentary engine. Data-driven mode (no API)
                                  or Claude API mode (claude-3-5-haiku) with enriched
                                  context: NOI impact, one-time detection, seasonality,
                                  reversal identification. Tier 1 ≥ $5K or 5% of budget;
                                  Tier 2 $2.5K–$5K and ≥ 5%.

  parsers/
    yardi_gl.py                 ← Yardi General Ledger detail (.xlsx) — REQUIRED
    yardi_trial_balance.py      ← Yardi Trial Balance (.xlsx)
    yardi_budget_comparison.py  ← Yardi Budget Comparison (.xlsx)
    yardi_income_statement.py   ← Yardi Income Statement (.xlsx)
    yardi_rent_roll.py          ← Yardi Tenancy Schedule (.xlsx)
    yardi_bank_rec.py           ← Yardi Bank Reconciliation PDF — preferred bank source.
                                  Reads pre-computed reconciled balance, outstanding checks,
                                  and reconciling difference directly from Yardi's output.
    pnc_bank_statement.py       ← PNC Bank Statement PDF — fallback when no Yardi rec.
                                  Parsed with pdfplumber; PNC format only.
    keybank_daca.py             ← KeyBank DACA statement (x5132). Parses "additions"
                                  field — used as management fee cash-received basis.
    nexus_accrual.py            ← Nexus AP Accrual Detail (.xls) — open invoices for
                                  Layer 1 accrual detection.
    berkadia_loan.py            ← Berkadia Loan Statement (.xlsx) — 3 loans.
    kardin_budget.py            ← Kardin Annual Budget (.xlsx) — used for QC tie-out
                                  and Kardin-driven payroll bonus accruals.
    monthly_report_template.py  ← Monthly Report Template (.xlsx) — Singerman format map.
```

---

## UI Layout (app.py)

The Streamlit sidebar contains only:
- File upload widgets (GL required; all others optional)
- Bonus accrual overrides (615110 R&M, 637110 Admin) — override Kardin auto-detection

The main area contains, in order:
1. **One-Off Accruals table** (`st.data_editor`) — user enters DR account + vendor + amount;
   pipeline auto-generates DR expense / CR 211200 JE pair. Pre-seeded with common monthly
   items (637150 Tenant Relations, 617110 HVAC quarterly, 619120 PPM, 627230 Fire Life Safety,
   635110 Snow & Ice, 610140 Durkin, 610160 Casella extra, 637230 BlueTriton, 613310 Water/Sewer).
   Leave Amount $0 to skip a row this month; add rows for anything new.

2. **Manual JEs & Reclasses table** (`st.data_editor`) — fully balanced JEs entered by user
   (positive = DR, negative = CR, must net to $0 per JE#). For reclasses, true-ups, and
   anything where the offset is not 211200.

3. **Run Pipeline** button

---

## Key Data Flow

1. User uploads files (GL required; all others optional)
2. User fills in One-Off Accruals and/or Manual JEs tables if needed
3. `engine.run_pipeline()` parses all files, runs cross-source validation
4. Bank reconciliation: Yardi Bank Rec PDF preferred (reads pre-computed values);
   falls back to 3-pass PNC matching (check number+amount → amount+date → amount-only)
5. Accrual entries built via 5-layer detection (deduped against GL); $500 materiality floor
6. Management fee calculated (DACA → GL 111100 → revenue proxy → manual); catch-up detected
7. One-Off Accrual table rows converted to supplement JEs (DR expense / CR 211200)
8. Manual JE table rows converted to balanced Yardi-import JEs
9. Prepaid amortization schedule runs; release JEs generated
10. QC engine runs 7 checks across GL, TB, BC, accruals, bank rec
11. Variance comments generated (API or data-driven)
12. Three Yardi-import CSVs exported:
    - `GA_Accruals_JE.csv` — all accrual entries (invoice proration, budget gap, historical,
      management fee, catch-up, contract supplements, bonus accruals, tenant utility billings)
    - `GA_Prepaid_JE.csv` — prepaid amortization entries
    - `GA_Manual_JE.csv` — manual JEs and reclasses
13. Singerman workbook, workpapers, BS workpaper, QC workbook, annotated BC exported

---

## Key Dataclasses (engine.py)

- **`EngineResult`** — Top-level output. Contains `parsed` (dict of all parser outputs),
  `matches`, `exceptions`, `bank_recon_detail`, `period`, `property_name`, summary stats.
- **`BankReconDetail`** — Bank reconciliation detail: matched checks/ACH/deposits, outstanding
  items, adjusted balances, reconciling difference. Single source of truth for workpaper generator.
- **`MatchResult`** — A matched pair between two sources (GL↔Bank, GL↔Nexus, etc.)
- **`Exception_`** — A flagged issue with severity (`error`/`warning`/`info`), category, source, description.

---

## Management Fee (management_fee.py)

- **JLL rate**: 1.25% | **GRP rate**: 1.75% | **Total**: 3.00%
- **Base**: Cash received — DACA KeyBank x5132 additions (matches JLL's own basis)
- **JE**: DR 637130 Admin-Management Fees / CR 201000 Accrued Liabilities (JE# MGT-001)
- **Catch-up**: If 637130 has net credit from prior-period auto-reversal with no matching
  invoice debit, a catch-up JE is generated (DR 637130 / CR 211200, JE# MGT-002)

---

## Accrual Layers (accrual_entry_generator.py)

| Layer | Source | Method |
|-------|--------|--------|
| 1 | Nexus AP Accrual Detail | Open invoices not yet in GL (deduped by invoice number) |
| 2 | GL recurring invoices | Cost-per-day × days remaining in period |
| 3 | Yardi Budget Comparison | Budget − YTD Actual gap, with escrow/seasonal exclusions |
| 4 | GL historical patterns | Average of prior months for accounts with no current activity |
| 5 | Kardin budget | Annual bonus ÷ 12 − min monthly; suppressed on payment months |

Materiality floor: **$500** — entries below this are suppressed.

Escrow-funded accounts excluded from budget-gap (e.g., 641110 RE Tax via 115200, insurance via 115300).

---

## Account Code Conventions

- Account codes are **6-digit strings** with no dashes (e.g., `"111100"`, `"637130"`)
- P&L accounts: `4xxxxx` (revenue) through `8xxxxx` (expense)
- Balance Sheet accounts: `1xxxxx` (assets) through `3xxxxx` (equity/liabilities)
- Key accounts: `111100` Operating Cash, `115100` DACA, `115200` RE Tax Escrow,
  `115300` Insurance Escrow, `201000` Accrued Liabilities, `211200` Accrued Expenses,
  `637130` Admin-Management Fees

---

## Development Notes

- **Imports**: `app.py` adds `pipeline/` to `sys.path`. Standalone scripts need
  `sys.path.insert(0, 'pipeline')` or `sys.path.insert(0, '/path/to/pipeline')`.
- **Parser returns**: Each parser returns a dataclass or dict. Keys vary by parser;
  GL returns a `GLParseResult` with `.accounts` list and `.transactions` rollup.
- **Financial data types**: Parsers sometimes return strings for numeric fields.
  Use defensive float conversion (see `_safe_num()` in workpaper_generator.py).
- **No circular imports**: GL lookup utilities live in `accrual_entry_generator.py`, not `engine.py`.
- **Excel output**: Uses `openpyxl` for .xlsx generation. Styles defined per generator module.
- **PDF parsing**: `pdfplumber` for PNC and Yardi Bank Rec PDFs.
- **Session state**: `manual_accruals_df` (One-Off Accruals table) and `manual_je_df`
  (Manual JEs table) persist in `st.session_state` across reruns. Reset button zeros
  accruals amounts but preserves pre-seeded rows.

---

## Testing

```bash
# Full March 2026 integration test (requires local file paths in test_march.py)
python test_march.py

# Unit tests (if present)
python -m pytest pipeline/tests/ -v
```

---

## Deployment

Push to `main` branch on GitHub (`RyanCWalsh1717/ga-automation`).
Streamlit Community Cloud auto-deploys within 1–2 minutes.
No CI/CD pipeline — Streamlit builds directly from `requirements.txt`.

---

## People

- **Ryan Walsh** — GRP accountant, primary pipeline operator. Reviews all outputs before sign-off.
- **Natasha** — GRP accountant, co-reviewer. Reviews pipeline outputs alongside Ryan.
- **Lauren Sullivan** — CFO. Final reviewer only — sees outputs after Ryan/Natasha complete review.
- **Philip Dorman** — Separate pipeline implementation (bake-off comparison). Not involved in
  the monthly GRP accounting review process.

---

## Exception Severity Levels

- `error` — Must fix before outputs are valid
- `warning` — Needs review; output may still be usable
- `info` — Informational only; no action required

---

## Output Files

| File | Contents |
|------|----------|
| `GA_Monthly_Report.xlsx` | 8-tab Singerman deliverable (BS, IS, T12, TB-MTD, TB-YTD, GL-MTD, GL-YTD, Tenancy) |
| `GA_Exception_Report.xlsx` | 5-tab QC/exception workbook (Summary, Exceptions, Budget Variances, Bank Rec, Debt Service) |
| `GA_Workpapers.xlsx` | Institutional workpapers (Bank Rec, Debt Service, Rent Roll, Accruals) |
| `GA_BS_Workpaper.xlsx` | Balance Sheet workpaper (GL vs TB tie-out, DACA rec) |
| `GA_QC_Workbook.xlsx` | 7-point QC checklist with pass/flag/fail status |
| `GA_Accruals_JE.csv` | Yardi-import CSV: all accrual JEs |
| `GA_Prepaid_JE.csv` | Yardi-import CSV: prepaid amortization JEs |
| `GA_Manual_JE.csv` | Yardi-import CSV: manual JEs and reclasses |
| `GA_Budget_Comparison_WithComments.xlsx` | Annotated BC with variance commentary |
