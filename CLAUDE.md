# GA Automation Pipeline

## What This Is

Monthly accounting close automation for **Greatland Realty Partners** (CRE firm, 2M+ SF, Greater Boston). The beta property is **GA (Grove Arts / Revolution Labs)** at 275 Grove Street, Newton. The capital partner is **Singerman**, who receives institutional-quality deliverables each month.

Deployed as a Streamlit web app at: `https://ga-automation-v2ebmljbewawn5pj9ivvbd.streamlit.app/`
GitHub repo: `https://github.com/Pdorman76/ga-automation`

## Architecture

```
app.py                          ← Streamlit UI (file upload, run button, results dashboard, downloads)
pipeline/
  engine.py                     ← Core processing engine. Orchestrates parsers, cross-validation, bank recon.
                                  Returns EngineResult dataclass with all parsed data + exceptions.
  report_generator.py           ← Generates 8-tab Singerman deliverable workbook (BS, IS, T12, TB-MTD,
                                  TB-YTD, GL-MTD, GL-YTD, Tenancy) + exception report.
  workpaper_generator.py        ← Generates 4-tab financial workpapers (Bank Recon, Debt Service,
                                  Rent Roll Tie-Out, Accruals). Consumes BankReconDetail from engine.
  accrual_entry_generator.py    ← 3-layer accrual entry detection:
                                  Layer 1: Nexus open invoices (deduped against GL)
                                  Layer 2: Budget gap detection (with seasonality filters)
                                  Layer 3: Historical recurring pattern detection
  variance_comments.py          ← Generates variance commentary. Data-driven mode (no API) or
                                  Claude API mode with enriched context (NOI impact, seasonality, etc.)
  parsers/
    yardi_gl.py                 ← Yardi General Ledger detail (.xlsx) — REQUIRED file
    yardi_income_statement.py   ← Yardi Income Statement (.xlsx)
    yardi_budget_comparison.py  ← Yardi Budget Comparison (.xlsx)
    yardi_rent_roll.py          ← Yardi Tenancy Schedule (.xlsx) — dynamic property prefix detection
    nexus_accrual.py            ← Nexus AP Accrual Detail (.xls)
    pnc_bank_statement.py       ← PNC Bank Statement (.pdf)
    berkadia_loan.py            ← Berkadia Loan Statement (.xlsx)
    kardin_budget.py            ← Kardin Annual Budget (.xlsx)
    monthly_report_template.py  ← Monthly Report Template (.xlsx) — maps to Singerman format
```

## Key Data Flow

1. User uploads up to 9 files via Streamlit sidebar (only GL is required)
2. `engine.run_pipeline()` parses all files, runs cross-source validation
3. Bank reconciliation uses 3-pass matching: check number+amount → amount+date proximity → amount-only
4. Accrual entries are built from Nexus invoices, budget gaps, and historical patterns (deduped against GL)
5. Report generator builds the Singerman workbook from parsed data
6. Workpaper generator builds institutional workpapers from engine results
7. Variance comments enrich budget variances with NOI impact, one-time detection, seasonality

## Key Dataclasses (engine.py)

- **`EngineResult`** — Top-level output. Contains `parsed` (dict of all parser outputs), `matches`, `exceptions`, `bank_recon_detail`, summary stats.
- **`BankReconDetail`** — Detailed bank reconciliation: matched checks/ACH/deposits, outstanding items, adjusted balances, reconciling difference. Single source of truth consumed by workpaper generator.
- **`MatchResult`** — A matched pair between two sources (GL↔Bank, GL↔Nexus, etc.)
- **`Exception_`** — A flagged issue with severity (error/warning/info), category, source, description.

## Development Notes

- **Imports**: `app.py` adds `pipeline/` to `sys.path`. When running files standalone or in tests, you may need `sys.path.insert(0, 'pipeline')`.
- **Parser returns**: Each parser returns a dict. Keys vary by parser but typically include lists of transaction dicts and metadata.
- **Financial data types**: Parsers sometimes return strings for numeric fields. Use defensive float conversion (see `_safe_num()` in workpaper_generator.py).
- **No circular imports**: GL lookup utilities live in `accrual_entry_generator.py`, not `engine.py`.
- **Excel output**: Uses `openpyxl` for .xlsx generation. Styles are defined in each generator module.
- **PDF parsing**: `pdfplumber` for PNC bank statements. Bank statement format is PNC-specific.

## Testing

```bash
python -m pytest pipeline/tests/ -v
```

## Deployment

Push to `main` branch on GitHub. Streamlit Community Cloud auto-deploys within 1-2 minutes. No CI/CD pipeline — the Streamlit service builds from `requirements.txt` directly.

## People

- **Philip Dorman** — Project owner, Greatland Realty Partners
- **Lauren Sullivan** — CFO, primary end user of the pipeline outputs
- **Ryan Walsh** — Building API prompts for variance commentary (Phase 2)

## Conventions

- All monetary values should be floats, not strings, by the time they reach generators
- Account numbers are strings (e.g., "1000-000", "4000-100")
- P&L accounts: 4000+ series. BS accounts: 1000-3999 series.
- Exception severity levels: "error" (must fix), "warning" (review), "info" (FYI)
- Output filenames: `GA_[Type]_Report_YYYYMMDD.xlsx`
