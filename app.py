"""
GA Automation — Monthly Report Pipeline (Two-Pass)
====================================================
Pass 1 (Pre-Close):  Upload pre-close Yardi GL + supporting files → detect
                     accruals → export 3 JE CSVs for Yardi upload.

Pass 2 (Post-Close): After JEs are posted to Yardi, upload final GL →
                     generate BS workpaper, QC, variance comments, exception
                     report. (Singerman workbook is downloaded directly from Yardi.)
"""

import streamlit as st
import sys
import os
import re
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

# ── Setup paths ──────────────────────────────────────────────
pipeline_dir = Path(__file__).parent / "pipeline"
if str(pipeline_dir) not in sys.path:
    sys.path.insert(0, str(pipeline_dir))

from engine import run_pipeline, EngineResult, Exception_
from property_config import is_revenue_account, is_income_statement_account
from report_generator import generate_exception_report
import traceback
from accrual_entry_generator import (
    build_accrual_entries, generate_yardi_je_csv,
    build_prepaid_amortization, build_prepaid_release_je,
)
import prepaid_ledger
import bs_workpaper_generator
from variance_comments import (
    generate_variance_comments,
    generate_variance_comments_grp,
    write_comments_to_budget_comparison,
)
from qc_engine import run_qc, generate_qc_workbook
from management_fee import (
    calculate as calculate_mgmt_fee,
    accrued_fee_from_bc,
    build_management_fee_je,
    detect_prior_period_catchup,
    build_catchup_je,
)


# ── Page configuration ───────────────────────────────────────
st.set_page_config(
    page_title="GA Automation",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    :root {
        --primary-color: #1F3864;
        --success-color: #2ecc71;
        --warning-color: #f39c12;
        --error-color: #e74c3c;
        --info-color: #3498db;
    }

    .main-header {
        color: var(--primary-color);
        border-bottom: 3px solid var(--primary-color);
        padding-bottom: 10px;
        margin-bottom: 20px;
    }

    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        padding: 20px;
        border-radius: 8px;
        border-left: 4px solid var(--primary-color);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .status-clean    { color: var(--success-color); font-weight: bold; }
    .status-warnings { color: var(--warning-color); font-weight: bold; }
    .status-errors   { color: var(--error-color);   font-weight: bold; }

    .exception-error {
        background-color: #ffe6e6;
        border-left: 4px solid var(--error-color);
        padding: 10px; margin: 10px 0; border-radius: 4px;
    }
    .exception-warning {
        background-color: #fff3cd;
        border-left: 4px solid var(--warning-color);
        padding: 10px; margin: 10px 0; border-radius: 4px;
    }
    .exception-info {
        background-color: #e7f3ff;
        border-left: 4px solid var(--info-color);
        padding: 10px; margin: 10px 0; border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state initialization ─────────────────────────────
# Shared
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}
if "temp_dir" not in st.session_state:
    st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")

# Pass 1 — JE Generation
if "pass1_complete" not in st.session_state:
    st.session_state.pass1_complete = False
if "pass1_engine_result" not in st.session_state:
    st.session_state.pass1_engine_result = None
if "pass1_output_files" not in st.session_state:
    st.session_state.pass1_output_files = {}
if "pass1_run_count" not in st.session_state:
    st.session_state.pass1_run_count = 0

# Pass 2 — Report Generation
if "pass2_complete" not in st.session_state:
    st.session_state.pass2_complete = False
if "pass2_engine_result" not in st.session_state:
    st.session_state.pass2_engine_result = None
if "pass2_output_files" not in st.session_state:
    st.session_state.pass2_output_files = {}
if "post_close_je_df" not in st.session_state:
    import pandas as _pd_init
    st.session_state.post_close_je_df = _pd_init.DataFrame({
        "JE #": ["PC-001", "PC-001"], "Description": ["", ""],
        "Account Code": ["", ""],
        "Debit ($)": [0.0, 0.0], "Credit ($)": [0.0, 0.0],
        "Line Description": ["", ""],
    })

# Tenant list for TUB sidebar inputs (key suffix, display name)
_TUB_TENANTS = [
    ("accent",  "Accent Therapeutics"),
    ("keros_n", "Keros Therapeutics (N)"),
    ("keros_s", "Keros Therapeutics (S)"),
    ("orum",    "Orum Therapeutics"),
    ("santi",   "Santi Therapeutics"),
]

if "manual_accruals_df" not in st.session_state:
    import pandas as pd
    _n = 11  # number of pre-seeded rows
    st.session_state.manual_accruals_df = pd.DataFrame({
        "Account Code": ["613310", "637150", "637150", "617110", "619120",
                         "627230", "635110", "610140", "610160", "637230", ""],
        "Account Name": ["Utilities-Water/Sewer", "Admin-Tenant Relations",
                         "Admin-Tenant Relations", "HVAC Maint-Contract Svc",
                         "Water Contract Svc", "Fire Life Safety",
                         "Snow & Ice Removal", "Cleaning Mat/Supplies",
                         "Cleaning-Trash Removal (extra)", "Admin-Materials/Supplies", ""],
        "Vendor":       [""] * _n,
        "Amount ($)":   [0.0] * _n,
        "Description":  [""] * _n,
    })

# Drop removed columns from sessions that have them (backward compatibility)
for _col in ("CR Account", "Auto-Reverse"):
    if _col in st.session_state.manual_accruals_df.columns:
        st.session_state.manual_accruals_df = st.session_state.manual_accruals_df.drop(columns=[_col])


# ── Header ───────────────────────────────────────────────────
st.markdown("<h1 class='main-header'>Greatland Realty Partners</h1>", unsafe_allow_html=True)
st.markdown("### GA Automation — Monthly Close Pipeline")
st.markdown("**Revolution Labs — 1050 Waltham St, Lexington, MA**")
st.divider()


# ── Sidebar ──────────────────────────────────────────────────────────────────
prior_period_outstanding = 0.0  # Yardi Bank Rec PDF includes all outstanding items

if st.sidebar.button("🔄 Reset All", use_container_width=True,
                     help="Clear all results and uploaded files"):
    st.session_state.pass1_complete = False
    st.session_state.pass1_engine_result = None
    st.session_state.pass1_output_files = {}
    st.session_state['pass1_gl_activity_log'] = []
    st.session_state.pass2_complete = False
    st.session_state.pass2_engine_result = None
    st.session_state.pass2_output_files = {}
    st.session_state.uploaded_files = {}
    st.session_state.bulk_overrides_p1 = {}
    st.session_state.bulk_overrides_p2 = {}
    shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
    st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")
    import pandas as _pd
    st.session_state.post_close_je_df = _pd.DataFrame({
        "JE #": ["PC-001", "PC-001"], "Description": ["", ""],
        "Account Code": ["", ""],
        "Debit ($)": [0.0, 0.0], "Credit ($)": [0.0, 0.0],
        "Line Description": ["", ""],
    })
    if "manual_accruals_df" in st.session_state:
        st.session_state.manual_accruals_df["Amount ($)"] = 0.0
    for _tkey, _ in _TUB_TENANTS:
        st.session_state[f"tub_elec_{_tkey}"] = 0.0
        st.session_state[f"tub_gas_{_tkey}"]  = 0.0
    st.rerun()


FILE_CONFIG = {
    # ── Core ──────────────────────────────────────────────────
    "gl": (
        "Yardi GL Detail (.xlsx)", "xlsx", True, "core",
        "REQUIRED — source of truth for all accounts and transactions.",
    ),
    "trial_balance": (
        "Yardi Trial Balance (.xlsx)", "xlsx", False, "core",
        "Enables GL↔TB tie-out validation on every BS account and QC Check 5. "
        "Without it: BS workpaper generates but all TB columns show 'N/A'.",
    ),
    "budget_comparison": (
        "Yardi Budget Comparison (.xlsx)", "xlsx", False, "core",
        "Enables budget gap accrual detection (Layer 3) and variance commentary. "
        "Without it: only Nexus invoice and invoice-proration accruals generated; no variance comments.",
    ),
    "kardin_budget": (
        "Kardin 2026 Budget (.xlsx)", "xlsx", False, "core",
        "Enables QC YTD budget vs Kardin cross-check AND monthly payroll bonus accruals (Layer 5 — "
        "annual budget ÷ 12, suppressed in payment months). "
        "Without it: QC budget check and bonus accruals skipped.",
    ),
    "t12_statement": (
        "12-Month Income Statement (.xlsx)", "xlsx", False, "core",
        "Powers MoM Swings tab (Tab 4) in Pass 2 QC workbook with real prior-month actuals "
        "instead of derived YTD-PTD. Critical for January (prior month = December actuals). "
        "Also improves Layer 4 historical accrual detection in Pass 1.",
    ),
    "nexus_accrual": (
        "Nexus Invoice Detail (.xls / .xlsx)", ["xls", "xlsx"], False, "core",
        "Enables AP accrual detection (Layer 1 — open invoices not yet posted to GL). "
        "Without it: invoice-proration (Layer 2), budget gap (Layer 3), and historical (Layer 4) still run.",
    ),
    # ── Bank ──────────────────────────────────────────────────
    "bank_rec": (
        "Yardi Bank Rec PDF — Operating (.pdf)", "pdf", False, "bank",
        "PREFERRED bank source. Reads Yardi's pre-computed reconciliation: bank balance, "
        "outstanding checks, reconciled balance, and $0 difference. Enables Operating bank "
        "rec tab in the BS workpaper (PNC x3993 vs GL 111100). Without it: no bank rec tab.",
    ),
    "receivable_detail": (
        "Yardi Receivable Detail (.xlsx)", "xlsx", False, "bank",
        "PRIMARY management fee basis — JLL's exact method. Export from Yardi after bank rec is complete. "
        "Pair with the AR Detail Aging for accurate prepayment exclusion. "
        "Without it: falls back to DACA additions.",
    ),
    "ar_aging": (
        "Yardi AR Detail Aging (.xlsx)", "xlsx", False, "bank",
        "Prepayment identification for the management fee — the Pre-payments column shows unapplied "
        "tenant credits excluded from the cash-received basis. Upload alongside the Receivable Detail. "
        "Without it: falls back to charge-code scan in the Receivable Detail (less reliable).",
    ),
    "bank_rec_dev": (
        "Development Bank Statement — Bank of America (.pdf)", "pdf", False, "bank",
        "Adds a 'Bank Rec - Development' tab to the Pass 2 workpaper for the revlabs entity. "
        "Upload the BofA Full Analysis Business Checking statement (account x3132). "
        "Without it: development bank rec tab is omitted from the workpaper.",
    ),
    "daca_bank": (
        "DACA Bank Statement — KeyBank x5132 (.pdf)", "pdf", False, "bank",
        "Enables DACA bank rec tab in the BS workpaper (KeyBank x5132 vs GL 115100). "
        "Without it: DACA rec tab is omitted from the workpaper.",
    ),
    # ── Reference ─────────────────────────────────────────────
    "loan": (
        "Berkadia Loan Statements (.pdf)", "pdf", False, "ref",
        "Enables debt service workpaper tab and principal balance tracking. "
        "Without it: debt service section not generated.",
    ),
    "prepaid_ledger": (
        "Prepaid Ledger — prior month (.xlsx)", "xlsx", False, "ref",
        "Carry-forward from prior month for prepaid amortization tracking. "
        "Without it: ledger starts fresh — existing multi-period items won't be carried forward.",
    ),
}

file_config = FILE_CONFIG

from file_classifier import classify_file as _classify_file, FILE_LABELS as _FILE_LABELS, MULTI_FILE_KEYS as _MULTI_FILE_KEYS

# ── Session state: persist override selections across reruns ─────────────────
if "bulk_overrides_p1" not in st.session_state:
    st.session_state.bulk_overrides_p1 = {}

# Options shown in the override dropdown for Pass 1
_P1_SLOT_KEYS = [
    "gl", "trial_balance", "budget_comparison", "kardin_budget", "t12_statement",
    "nexus_accrual", "bank_rec", "receivable_detail", "ar_aging",
    "bank_rec_dev", "daca_bank", "loan", "prepaid_ledger", "unknown",
]
_P1_SLOT_LABELS = [_FILE_LABELS.get(k, k) for k in _P1_SLOT_KEYS]


# ═══════════════════════════════════════════════════════════════
# ── Main content: Two-pass tabs ──────────────────────────────
# ═══════════════════════════════════════════════════════════════
import pandas as pd

tab1, tab2, tab3 = st.tabs([
    "📋 Pass 1 — Generate JEs  (Pre-Close)",
    "📊 Pass 2 — Generate Reports  (Post-Close)",
    "📖 How to Use",
])


# ──────────────────────────────────────────────────────────────
# TAB 1 — PASS 1: JE GENERATION
# ──────────────────────────────────────────────────────────────
with tab1:
    # ── File Upload ──────────────────────────────────────────────────────────
    st.markdown("### 📥 Upload Pass 1 Files")
    st.caption(
        "Select all source files at once — GL, Budget Comparison, Trial Balance, "
        "Bank Rec, Nexus, DACA, Berkadia, etc. The app auto-detects each file type. "
        "Use the dropdown to correct any mismatches."
    )

    _bulk_p1 = st.file_uploader(
        "Drop Pass 1 files here",
        accept_multiple_files=True,
        type=["xlsx", "xls", "pdf"],
        key="bulk_upload_p1",
        label_visibility="collapsed",
    )

    # Clear all Pass 1 slots so stale entries don't persist after a file is removed
    for _clr_key in set(_P1_SLOT_KEYS) - {"unknown"}:
        st.session_state.uploaded_files.pop(_clr_key, None)

    if _bulk_p1:
        _loan_paths_p1: list = []

        for _uf in _bulk_p1:
            _raw = bytes(_uf.getbuffer())
            _det_key, _conf, _det_label = _classify_file(_uf.name, _raw, pass2=False)
            _eff_key = st.session_state.bulk_overrides_p1.get(_uf.name, _det_key)

            _ic, _fn_col, _tp_col = st.columns([0.5, 5, 5])
            if _eff_key == "unknown":
                _ic.markdown("⚠️")
            elif _conf >= 0.85:
                _ic.markdown("✅")
            else:
                _ic.markdown("🟡")

            _short = _uf.name if len(_uf.name) <= 40 else _uf.name[:37] + "…"
            _fn_col.caption(_short)

            if _eff_key == "unknown" or _conf < 0.70:
                _cur_idx = (_P1_SLOT_KEYS.index(_eff_key)
                            if _eff_key in _P1_SLOT_KEYS else len(_P1_SLOT_KEYS) - 1)
                _sel_label = _tp_col.selectbox(
                    "type", _P1_SLOT_LABELS, index=_cur_idx,
                    key=f"ovr_p1_{_uf.name}", label_visibility="collapsed",
                )
                _eff_key = _P1_SLOT_KEYS[_P1_SLOT_LABELS.index(_sel_label)]
                st.session_state.bulk_overrides_p1[_uf.name] = _eff_key
            else:
                _tp_col.caption(_det_label)

            if _eff_key != "unknown":
                _tp = os.path.join(st.session_state.temp_dir, _uf.name)
                if not os.path.exists(_tp) or os.path.getsize(_tp) != _uf.size:
                    with open(_tp, "wb") as _f:
                        _f.write(_raw)
                if _eff_key in _MULTI_FILE_KEYS:
                    _loan_paths_p1.append(_tp)
                else:
                    st.session_state.uploaded_files[_eff_key] = _tp

        if _loan_paths_p1:
            st.session_state.uploaded_files["loan"] = _loan_paths_p1

        _active_names = {_uf.name for _uf in _bulk_p1}
        st.session_state.bulk_overrides_p1 = {
            k: v for k, v in st.session_state.bulk_overrides_p1.items()
            if k in _active_names
        }

    # ── Upload status ─────────────────────────────────────────────────────────
    uploaded_keys = set(st.session_state.uploaded_files.keys())
    missing_impact = []
    if "trial_balance"     not in uploaded_keys: missing_impact.append("No BS tie-out validation (Pass 2)")
    if "budget_comparison" not in uploaded_keys: missing_impact.append("No budget gap accruals or variance comments")
    if "bank_rec"          not in uploaded_keys: missing_impact.append("No Operating bank rec tab (Pass 2)")
    if "daca_bank"         not in uploaded_keys: missing_impact.append("No DACA bank rec tab (Pass 2)")
    if "loan"              not in uploaded_keys: missing_impact.append("No debt service tab (Pass 2)")

    uploaded_count = len(uploaded_keys)
    gl_uploaded = "gl" in uploaded_keys

    _st_c1, _st_c2 = st.columns(2)
    with _st_c1:
        st.caption(f"**{uploaded_count} file(s) uploaded**")
    with _st_c2:
        if missing_impact:
            with st.expander(f"⚠️ {len(missing_impact)} output(s) won't generate", expanded=False):
                for m in missing_impact:
                    st.caption(f"• {m}")

    if not gl_uploaded and uploaded_count > 0:
        st.warning("⚠️ GL Detail is required to run either pass.")

    st.divider()

    # ── Tenant Utility Billing ────────────────────────────────────────────────
    _tenant_utility_rows = []
    _tu_elec_total = 0.0
    _tu_gas_total  = 0.0
    with st.expander("⚡ Tenant Utility Billing — Enter monthly meter read amounts", expanded=False):
        st.caption(
            "Enter electric and gas amounts per tenant from the monthly meter read. "
            "Posts as: DR 133110 / CR 440500 (electric) and CR 440700 (gas). "
            "Leave at $0 to skip — pipeline auto-accrues budget amounts."
        )
        _tub_cols = st.columns(len(_TUB_TENANTS))
        for (_tkey, _tname), _tcol in zip(_TUB_TENANTS, _tub_cols):
            with _tcol:
                st.caption(f"**{_tname}**")
                _telec = st.number_input(
                    "Electric ($)", min_value=0.0, value=0.0, step=1.0, format="%.2f",
                    key=f"tub_elec_{_tkey}",
                )
                _tgas = st.number_input(
                    "Gas ($)", min_value=0.0, value=0.0, step=1.0, format="%.2f",
                    key=f"tub_gas_{_tkey}",
                )
            if _telec > 0 or _tgas > 0:
                _tenant_utility_rows.append({'tenant': _tname, 'electric': _telec, 'gas': _tgas})
                _tu_elec_total += _telec
                _tu_gas_total  += _tgas
        if _tenant_utility_rows:
            st.caption(f"✓ {len(_tenant_utility_rows)} tenant(s) — Electric ${_tu_elec_total:,.2f} / Gas ${_tu_gas_total:,.2f}")
        else:
            st.caption("↳ No entries — pipeline will auto-accrue budget amounts if meter read not in GL")

    # ── RE Tax Bill ───────────────────────────────────────────────────────────
    with st.expander("🏛️ RE Tax Bill — Payment months only (Jan / Apr / Jul / Oct)", expanded=False):
        st.caption(
            "Enter the quarterly RE Tax bill amount from the town. "
            "Posts as: DR 641110 Real Estate Taxes / CR 115200 RE Tax Escrow. "
            "Leave $0 in non-payment months — the monthly DR 641110 / CR 135120 accrual runs automatically."
        )
        _re_tax_bill_amount = st.number_input(
            "Quarterly RE Tax Bill ($)",
            min_value=0.0, value=0.0, step=1000.0, format="%.2f",
            key="widget_re_tax_bill",
        )
        _re_tax_bill_amount = _re_tax_bill_amount if _re_tax_bill_amount > 0 else 0.0

    st.divider()

    st.markdown("""
    **What this does:** Reads your pre-close Yardi GL and detects every accrual entry needed
    to complete the month-end close — invoices in Nexus not yet posted, budget gaps,
    historical patterns, management fee, prepaid amortization, one-off items you enter below.
    Exports two Yardi-import files.

    **Next step after this tab:** Upload the CSVs to Yardi → run final close → switch to **Pass 2**.
    """)
    st.divider()

    # ── One-Off Accruals Table ────────────────────────────────────────────────
    with st.expander("🧾 One-Off Accruals  (DR expense → CR 213100 Accrued Expenses)", expanded=False):
        st.caption(
            "Use this for known invoices not yet in Nexus or Yardi — quarterly contracts, "
            "seasonal items, recurring retainers, semi-annual billings, etc. "
            "All entries debit the expense account and credit **213100 Accrued Expenses** — "
            "they auto-reverse next period. "
            "**Leave Amount at $0** to suppress automated detection for that account without generating a JE — "
            "use this when a JE has already been posted to Yardi to prevent double-counting."
        )
        accruals_edited_df = st.data_editor(
            st.session_state.manual_accruals_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Account Code":  st.column_config.TextColumn("DR Account", width="small",
                                     help="6-digit Yardi GL account code (e.g. 613310)"),
                "Account Name":  st.column_config.TextColumn("Account Name", width="medium"),
                "Vendor":        st.column_config.TextColumn("Vendor", width="medium"),
                "Amount ($)":    st.column_config.NumberColumn("Amount ($)", format="$%,.2f",
                                     width="small", min_value=0.0,
                                     help="Positive amount — debit to expense account"),
                "Description":   st.column_config.TextColumn("Description", width="large",
                                     help="Description for the Yardi JE line"),
            },
            key="manual_accruals_editor",
        )
        st.session_state.manual_accruals_df = accruals_edited_df

        _accrual_active = accruals_edited_df[
            accruals_edited_df["Account Code"].fillna("").str.strip().astype(bool) &
            (accruals_edited_df["Amount ($)"].fillna(0) > 0)
        ]
        if not _accrual_active.empty:
            st.success(
                f"✅ {len(_accrual_active)} accrual(s) queued — "
                f"${_accrual_active['Amount ($)'].sum():,.2f} total debits",
                icon="✅",
            )

    st.divider()

    # ── Pass 1 Run Button ─────────────────────────────────────────────────────
    col_p1a, col_p1b = st.columns([3, 1])
    with col_p1a:
        pass1_button = st.button(
            "🚀 Generate JEs",
            disabled=not gl_uploaded,
            use_container_width=True,
            key="pass1_run_btn",
            help="Parse pre-close GL and generate all accrual JE CSVs for Yardi upload",
        )
    with col_p1b:
        if st.button("🔄 Reset Pass 1", use_container_width=True, key="reset_pass1"):
            st.session_state.pass1_complete = False
            st.session_state.pass1_engine_result = None
            st.session_state.pass1_output_files = {}
            st.session_state['pass1_gl_activity_log'] = []
            st.rerun()

    # ── Pass 1 Processing ─────────────────────────────────────────────────────
    if pass1_button:
        with st.spinner("Building accrual entries..."):
            try:
                files_dict = {key: st.session_state.uploaded_files.get(key)
                              for key in file_config.keys()}

                progress_bar = st.progress(0)
                status_text  = st.empty()

                # Step 1: Parse files (pre-close GL)
                status_text.text("Step 1/6: Parsing files...")
                progress_bar.progress(10)
                engine_result = run_pipeline(
                    files_dict,
                    prior_period_outstanding=prior_period_outstanding,
                )
                st.session_state.pass1_engine_result = engine_result

                gl_parsed  = engine_result.parsed.get('gl')
                bc_parsed  = engine_result.parsed.get('budget_comparison') or []
                nexus_data = engine_result.parsed.get('nexus_accrual')
                close_period = engine_result.period or ''

                # Step 2: Detect accrual entries (4-layer)
                status_text.text("Step 2/6: Detecting accrual entries (4 layers)...")
                progress_bar.progress(25)

                # Build manual exclusion list (account codes with entries in the One-Off table)
                # so Layers 2-4 don't double-accrue those accounts.
                #
                # IMPORTANT: rows with Amount ($) = $0 are treated as pure exclusion flags —
                # they suppress automated layers for that account WITHOUT generating a JE.
                # Use $0 when a JE has already been posted to Yardi (e.g., RE Taxes posted
                # manually before the pipeline ran) to prevent double-counting.
                _accruals_tbl_early = st.session_state.get("manual_accruals_df")
                _manual_accruals_input = []
                if _accruals_tbl_early is not None and not _accruals_tbl_early.empty:
                    _manual_accruals_input = [
                        {
                            'account_code': str(r["Account Code"]).strip(),
                            'account_name': str(r.get("Account Name", "") or "").strip(),
                            # Force amount=0 so Layer 0 registers the account for dedup
                            # suppression WITHOUT generating a MAN-XXXX JE entry.
                            # The actual SUP-XXXX JEs (with custom CR account support)
                            # are built separately via _supplement_je_lines below.
                            'amount': 0,
                            'description': str(r.get("Description", "") or "").strip(),
                        }
                        for _, r in _accruals_tbl_early.iterrows()
                        if str(r.get("Account Code", "") or "").strip()
                    ]

                # Parse T12 for Pass 1 (improves Layer 4 January historical accrual accuracy)
                _t12_file_p1 = st.session_state.uploaded_files.get("t12_statement")
                _t12_result_p1 = None
                if _t12_file_p1 and os.path.exists(_t12_file_p1):
                    try:
                        from parsers.yardi_t12 import parse as parse_t12
                        _t12_result_p1 = parse_t12(_t12_file_p1)
                    except Exception as _e:
                        st.warning(f"Could not parse 12-Month Statement for Pass 1: {_e}")

                _gl_activity_log = []
                je_lines = build_accrual_entries(
                    nexus_data or [],
                    period=close_period,
                    property_name=engine_result.property_name or '',
                    gl_data=gl_parsed,
                    budget_data=bc_parsed,
                    manual_accruals=_manual_accruals_input or [],
                    tenant_utility_rows=_tenant_utility_rows or None,
                    loan_data=engine_result.parsed.get('loan'),
                    re_tax_bill_amount=_re_tax_bill_amount,
                    t12_result=_t12_result_p1,
                    gl_activity_log=_gl_activity_log,
                )
                st.session_state['pass1_gl_activity_log'] = _gl_activity_log

                # Step 3: Prepaid ledger — load → merge → release JEs → advance
                status_text.text("Step 3/6: Processing prepaid ledger...")
                progress_bar.progress(45)

                ledger_path = st.session_state.uploaded_files.get("prepaid_ledger")
                ledger_active, ledger_completed = prepaid_ledger.load(ledger_path)

                # Merge Nexus Invoice Detail into ledger — status filtering in the
                # parser ensures only In Progress / Pending / Submitted / Completed
                # invoices reach this point; Rejected, Void, and On Hold are dropped.
                ledger_active, newly_added = prepaid_ledger.merge_nexus(
                    ledger_active, nexus_data or [], close_period
                )

                # Build visual amortization schedule
                amort_lines = build_prepaid_amortization(nexus_data or [], close_period=close_period)

                # Generate prepaid release JEs (months 2+ from ledger)
                ledger_release_lines = prepaid_ledger.get_current_amortization(ledger_active, close_period)
                prepaid_release_je = build_prepaid_release_je(
                    ledger_release_lines,
                    period=close_period,
                    je_start=len(je_lines) // 2 + 1,
                )

                # Advance ledger (increment months_amortized, expire completed)
                ledger_active, ledger_completed = prepaid_ledger.advance_period(
                    ledger_active, ledger_completed, close_period
                )

                updated_ledger_path = os.path.join(
                    st.session_state.temp_dir, "GA_Prepaid_Ledger_Updated.xlsx"
                )
                prepaid_ledger.save(ledger_active, ledger_completed, updated_ledger_path)

                # Step 4: Management fee (JE included in accruals CSV)
                status_text.text("Step 4/6: Calculating management fee...")
                progress_bar.progress(60)

                _daca_file = st.session_state.uploaded_files.get("daca_bank")
                _daca_parsed = None
                if _daca_file and os.path.exists(_daca_file):
                    try:
                        from parsers.keybank_daca import parse as _parse_daca
                        _daca_parsed = _parse_daca(_daca_file)
                    except Exception:
                        _daca_parsed = None

                _rd_file = st.session_state.uploaded_files.get("receivable_detail")
                _rd_parsed = None
                if _rd_file and os.path.exists(_rd_file):
                    try:
                        from parsers.yardi_receivable_detail import parse as _parse_rd
                        _rd_parsed = _parse_rd(_rd_file)
                    except Exception:
                        _rd_parsed = None

                _ar_aging_file = st.session_state.uploaded_files.get("ar_aging")
                _ar_aging_parsed = None
                if _ar_aging_file and os.path.exists(_ar_aging_file):
                    try:
                        from parsers.yardi_ar_aging import parse as _parse_ar_aging
                        _ar_aging_parsed = _parse_ar_aging(_ar_aging_file)
                    except Exception:
                        _ar_aging_parsed = None

                fee_result = calculate_mgmt_fee(
                    gl_parsed=gl_parsed,
                    budget_rows=bc_parsed or [],
                    daca_parsed=_daca_parsed,
                    receivable_detail=_rd_parsed,
                    ar_aging=_ar_aging_parsed,
                )
                fee_je = build_management_fee_je(
                    fee_result,
                    period=close_period,
                    property_code=engine_result.property_name or 'revlabpm',
                    je_number=f'MGT-{len(je_lines)//2 + 1:03d}',
                )

                _catchup_amount = detect_prior_period_catchup(gl_parsed)
                _catchup_je = []
                if _catchup_amount and _catchup_amount > 0:
                    _catchup_je = build_catchup_je(
                        _catchup_amount,
                        period=close_period,
                        property_code=engine_result.property_name or 'revlabpm',
                        je_number=f'MGT-{len(je_lines)//2 + 2:03d}',
                    )

                # Step 5: One-Off Accrual JEs
                status_text.text("Step 5/6: Building one-off accrual entries...")
                progress_bar.progress(75)

                # One-Off Accruals → DR expense / CR 213100 (or custom CR Account if specified)
                _supplement_je_lines = []
                _periodic_supplement_rows = []
                _sup_base = len(je_lines) // 2 + len(prepaid_release_je) // 2 + len(fee_je) // 2

                _accruals_tbl = st.session_state.get("manual_accruals_df")
                if _accruals_tbl is not None and not _accruals_tbl.empty:
                    _active_accruals = _accruals_tbl[
                        _accruals_tbl["Account Code"].fillna("").str.strip().astype(bool) &
                        (_accruals_tbl["Amount ($)"].fillna(0) > 0)
                    ]
                    _CR_ACCT_NAMES = {
                        '115200': 'RE Tax Escrow',
                        '115300': 'Insurance Escrow',
                        '133110': 'Tenant AR Billback',
                        '135150': 'Prepaids',
                        '213100': 'Accrued Expenses',
                    }
                    for _, _row in _active_accruals.iterrows():
                        _vendor = str(_row.get("Vendor", "") or "").strip()
                        _desc   = str(_row.get("Description", "") or "").strip()
                        _periodic_supplement_rows.append({
                            'account_code':    str(_row["Account Code"]).strip(),
                            'account_name':    str(_row.get("Account Name", "") or "").strip()
                                               or str(_row["Account Code"]).strip(),
                            'amount':          float(_row["Amount ($)"]),
                            'description':     _desc or _vendor or 'one-off accrual',
                            'vendor':          _vendor,
                            'auto_reverse':    True,   # all one-off accruals auto-reverse
                            'cr_account':      '213100',
                            'cr_account_name': 'Accrued Expenses',
                        })

                for _si, _sup in enumerate(_periodic_supplement_rows):
                    _sje_id  = f'SUP-{_sup_base + _si + 1:04d}'
                    _sup_amt = round(float(_sup['amount']), 2)
                    _sup_desc   = _sup.get('description') or f"{_sup['account_name']} — one-off accrual"
                    _sup_vendor = _sup.get('vendor') or _sup['account_name']
                    _sup_cr_acct = _sup.get('cr_account', '213100')
                    _sup_cr_name = _sup.get('cr_account_name', 'Accrued Expenses')
                    _supplement_je_lines.extend([
                        {
                            'je_number': _sje_id, 'line': 1, 'date': close_period,
                            'account_code': _sup['account_code'], 'account_name': _sup['account_name'],
                            'description': _sup_desc, 'reference': 'ONE-OFF-ACCRUAL',
                            'debit': _sup_amt, 'credit': 0, 'vendor': _sup_vendor,
                            'invoice_number': '', 'source': 'contract_supplement', 'confidence': 'high',
                        },
                        {
                            'je_number': _sje_id, 'line': 2, 'date': close_period,
                            'account_code': _sup_cr_acct, 'account_name': _sup_cr_name,
                            'description': _sup_desc, 'reference': 'ONE-OFF-ACCRUAL',
                            'debit': 0, 'credit': _sup_amt, 'vendor': _sup_vendor,
                            'invoice_number': '', 'source': 'contract_supplement', 'confidence': 'high',
                        },
                    ])

                # Step 6: Assemble all JEs and export 3 CSVs
                status_text.text("Step 6/6: Exporting JE CSVs...")
                progress_bar.progress(88)

                all_je_lines = (
                    je_lines
                    + prepaid_release_je
                    + fee_je
                    + _catchup_je
                    + _supplement_je_lines
                )

                _accrual_csv_path = None

                _prop_code = (engine_result.parsed.get('gl') and
                              engine_result.parsed['gl'].metadata.property_code) or 'revlabpm'

                if all_je_lines:
                    _accrual_csv_path = os.path.join(st.session_state.temp_dir, "GA_Accruals_JE.csv")
                    generate_yardi_je_csv(all_je_lines, _accrual_csv_path,
                                          period=close_period, property_code=_prop_code,
                                          book='')

                # Persist Pass 1 outputs
                p1 = st.session_state.pass1_output_files
                p1["all_je_lines"]          = all_je_lines
                p1["accrual_je_csv"]        = _accrual_csv_path
                p1["fee_result"]            = fee_result
                p1["rd_prepayment_amount"]  = getattr(fee_result, 'prepayment_excluded', 0.0)
                p1["catchup_amount"]        = _catchup_amount
                p1["amort_lines"]           = amort_lines
                p1["ledger_active"]         = ledger_active
                p1["ledger_completed"]      = ledger_completed
                p1["newly_added_prepaids"]  = newly_added
                p1["prepaid_ledger_updated"]= updated_ledger_path
                p1["prepaid_released_count"]= len(prepaid_release_je) // 2
                p1["prepaid_release_lines"] = ledger_release_lines

                progress_bar.progress(100)
                status_text.text("✓ JEs ready for Yardi upload!")
                st.session_state.pass1_complete = True
                st.session_state.pass1_run_count = st.session_state.get('pass1_run_count', 0) + 1
                st.success("Pass 1 complete! Download the JE CSVs below.", icon="✅")

            except Exception as e:
                tb = traceback.format_exc()
                st.error(f"Pass 1 error: {str(e)}", icon="❌")
                st.code(tb, language="python")
                st.session_state.pass1_complete = False

    # ── Pass 1 Results Dashboard ──────────────────────────────────────────────
    if st.session_state.pass1_complete and st.session_state.pass1_engine_result:
        result = st.session_state.pass1_engine_result
        p1     = st.session_state.pass1_output_files
        all_je_lines = p1.get("all_je_lines", [])
        fee_result   = p1.get("fee_result")

        st.divider()
        st.markdown(f"## Pass 1 Results — {result.period}  |  {result.property_name}")

        # ── GL Activity Gut-Check ──────────────────────────────────────────
        # Show accounts where the pipeline detected existing GL postings and
        # suppressed automated accruals. Lets Ryan verify these are intentional
        # before uploading the pipeline JEs to Yardi.
        _gl_log = st.session_state.get('pass1_gl_activity_log') or []
        if _gl_log:
            _gl_log_sorted = sorted(_gl_log, key=lambda x: x['account_code'])
            with st.expander(
                f"⚠️  Existing GL Activity — Accruals Suppressed "
                f"({len(_gl_log_sorted)} account{'s' if len(_gl_log_sorted) != 1 else ''})",
                expanded=True,
            ):
                st.markdown(
                    "A journal entry was already posted in the GL for each account below. "
                    "**No pipeline accrual was generated.** Confirm each posting is correct "
                    "before uploading the JE CSVs to Yardi. If a posting is wrong, delete it "
                    "from Yardi and re-run Pass 1."
                )
                import pandas as _pd_gc
                _gc_df = _pd_gc.DataFrame([
                    {'Account': r['account_code'], 'Name': r['account_name']}
                    for r in _gl_log_sorted
                ])
                st.dataframe(_gc_df, use_container_width=True, hide_index=True)
                st.caption(
                    "Need detail on what was posted? Export the GL Activity Log below."
                )

                # ── Optional backup export ─────────────────────────────────
                try:
                    import io as _io
                    import openpyxl as _opxl
                    from openpyxl.styles import (Font as _Font, PatternFill as _PFill,
                                                 Alignment as _Align, Border as _Border,
                                                 Side as _Side)
                    _buf = _io.BytesIO()
                    _wb_gc = _opxl.Workbook()
                    _ws_gc = _wb_gc.active
                    _ws_gc.title = "GL Activity — Verify"

                    # ── Styles ────────────────────────────────────────────
                    _hdr_fill  = _PFill(fill_type='solid', fgColor='1F4E79')
                    _acct_fill = _PFill(fill_type='solid', fgColor='D6E4F0')
                    _thin      = _Side(style='thin', color='BFBFBF')
                    _border    = _Border(bottom=_Side(style='thin', color='BFBFBF'))

                    # ── Column headers ────────────────────────────────────
                    _hdr = ['Account', 'Account Name', 'Type', 'Date',
                            'Description', 'Reference', 'Debit', 'Credit',
                            'Verified? (Y/N)', 'Notes']
                    _ws_gc.append(_hdr)
                    for _ci, _ in enumerate(_hdr, 1):
                        _cell = _ws_gc.cell(row=1, column=_ci)
                        _cell.font      = _Font(bold=True, color='FFFFFF')
                        _cell.fill      = _hdr_fill
                        _cell.alignment = _Align(horizontal='center', wrap_text=True)

                    # ── Data: one account header row + one row per transaction ──
                    for _r in _gl_log_sorted:
                        # Account summary row (shaded)
                        _ws_gc.append([
                            _r['account_code'],
                            _r['account_name'],
                            '', '', '',  # Type / Date / Description blank on summary
                            f"Net PTD: ${_r['ptd_amount']:,.2f}",
                            '', '', '', '',
                        ])
                        _acct_row = _ws_gc.max_row
                        for _ci in range(1, len(_hdr) + 1):
                            _cell = _ws_gc.cell(row=_acct_row, column=_ci)
                            _cell.fill = _acct_fill
                            _cell.font = _Font(bold=True)

                        # Individual transaction rows
                        for _t in _r.get('transactions', []):
                            _ws_gc.append([
                                '',                  # Account (blank — grouped under header)
                                '',                  # Name
                                _t.get('type', ''),
                                _t.get('date', ''),
                                _t.get('description', ''),
                                _t.get('reference', ''),
                                _t['debit']  if _t.get('debit')  else '',
                                _t['credit'] if _t.get('credit') else '',
                                '',  # Verified
                                '',  # Notes
                            ])
                            _txn_row = _ws_gc.max_row
                            # Format debit/credit as currency
                            for _ci in (7, 8):
                                _ws_gc.cell(row=_txn_row, column=_ci).number_format = '$#,##0.00'

                        # Spacer row between accounts
                        _ws_gc.append([''] * len(_hdr))

                    # ── Column widths ─────────────────────────────────────
                    for _col, _w in zip('ABCDEFGHIJ',
                                        [11, 34, 6, 12, 40, 16, 12, 12, 16, 28]):
                        _ws_gc.column_dimensions[_col].width = _w

                    # ── Footer note ───────────────────────────────────────
                    _info_row = _ws_gc.max_row + 1
                    _ws_gc.cell(row=_info_row, column=1,
                                value=(f"GA Automation Pipeline — {result.period} — "
                                       f"{result.property_name}   |   "
                                       "All transactions shown are Journal Entries (J). "
                                       "Accruals suppressed for all accounts above — verify before uploading JEs."))
                    _ws_gc.cell(row=_info_row, column=1).font = _Font(italic=True, color='808080')
                    _ws_gc.merge_cells(start_row=_info_row, start_column=1,
                                       end_row=_info_row, end_column=len(_hdr))

                    _wb_gc.save(_buf)
                    _buf.seek(0)
                    _gc_filename = f"GA_GL_Activity_GutCheck_{(result.period or 'unknown').replace(' ', '_').replace('-', '')}.xlsx"
                    st.download_button(
                        label="⬇️  Export GL Activity Log (.xlsx)",
                        data=_buf.getvalue(),
                        file_name=_gc_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        help="Download a standalone Excel backup with transaction detail.",
                    )
                except Exception:
                    pass  # Export is optional — never block the main UI

        # ── Management Fee ─────────────────────────────────────────────────
        if fee_result and fee_result.cash_received > 0:
            st.markdown("### Management Fee JE")
            _src_labels = {
                'receivable_detail+ar_aging': 'Receivable Detail (ex-Prepayments via AR Aging)',
                'receivable_detail': 'Receivable Detail (ex-Prepayments)',
                'daca_additions':    'DACA Additions',
                'gl_cash_account':   'GL 111100 Debits',
                'revenue_proxy':     'Revenue Proxy',
                'manual_override':   'Manual Override',
            }
            _src_label = _src_labels.get(fee_result.cash_source, fee_result.cash_source)
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                st.metric("Cash Received", f"${fee_result.cash_received:,.0f}",
                          help=f"Source: {_src_label}")
            with col_f2:
                st.metric(f"JLL ({fee_result.jll_rate:.2%})", f"${fee_result.jll_fee:,.0f}")
            with col_f3:
                st.metric(f"GRP ({fee_result.grp_rate:.2%})", f"${fee_result.grp_fee:,.0f}")
            with col_f4:
                st.metric(f"Total ({fee_result.total_rate:.2%})", f"${fee_result.total_fee:,.0f}")
            _prepay_amt = p1.get("rd_prepayment_amount", 0.0) or 0.0
            st.caption(f"Basis: {_src_label}"
                       + (f"  ·  Prepayments excluded: ${_prepay_amt:,.2f}" if _prepay_amt > 0 else ""))

            _catchup_amt = p1.get("catchup_amount")
            if _catchup_amt and _catchup_amt > 0:
                st.warning(
                    f"**Management Fee Catch-up Detected — ${_catchup_amt:,.2f}**\n\n"
                    f"Account 637130 shows a net credit (auto-reversal of prior month accrual with "
                    f"no matching invoice). A catch-up entry **(MGT-002)** has been included in the "
                    f"Accruals CSV. **Verify before posting:** confirm the prior month check is still "
                    f"outstanding in AP (213100) before uploading.",
                )
            st.divider()

        # ── JE Preview — Grouped by Credit (BS) Account ───────────────────
        dr_lines = [l for l in all_je_lines if (l.get('debit') or 0) > 0]
        cr_lines = [l for l in all_je_lines if (l.get('credit') or 0) > 0]

        if dr_lines:
            st.markdown("### Accrual Journal Entries")

            source_labels = {
                'nexus':                  'Nexus AP',
                'invoice_proration':      'Invoice Proration',
                'historical':             'Historical Pattern',
                'budget_gap':             'Budget Gap',
                'prepaid_amortization':   'Prepaid Amort.',
                'prepaid_ledger':         'Prepaid Release',
                'management_fee':         'Management Fee',
                'management_fee_catchup': 'Mgmt Fee Catch-up',
                'contract_supplement':    'One-Off Accrual',
                'tenant_utility_billing': 'Tenant Utility',
            }

            # ── Build CR lookup: je_number → (cr_account_code, cr_description) ──
            _cr_lookup: dict = {}
            for _cl in cr_lines:
                _je = _cl.get('je_number', '')
                if _je and _je not in _cr_lookup:
                    _cr_lookup[_je] = {
                        'code': str(_cl.get('account_code', '') or '').strip(),
                        'desc': str(_cl.get('description') or '').strip(),
                    }

            # Friendly labels for well-known CR accounts
            _CR_LABELS = {
                '115200': 'RE Tax Escrow',
                '115300': 'Insurance Escrow',
                '133110': 'Tenant AR Billback (Utility / Elec Recovery)',
                '135150': 'Prepaids',
                '213100': 'Accrued Expenses',
                '213200': 'Accrued Interest Payable',
            }
            def _cr_section_label(code: str) -> str:
                if code in _CR_LABELS:
                    return f"{code} — {_CR_LABELS[code]}"
                if code.startswith('115'):
                    return f"{code} — Escrow"
                if code.startswith('133'):
                    return f"{code} — Tenant AR Billback"
                if code.startswith('135'):
                    return f"{code} — Prepaids"
                if code == '213100':
                    return f"{code} — Accrued Expenses"
                if code == '213200':
                    return f"{code} — Accrued Interest Payable"
                if code.startswith('213'):
                    return f"{code} — Accrued"
                return f"{code}"

            # ── Sort order for BS sections ──────────────────────────────────
            def _section_sort_key(code: str) -> int:
                if code.startswith('115'):  return 1   # Escrow (RE Tax, Insurance)
                if code.startswith('133'):  return 2   # Tenant AR Billback
                if code.startswith('135'):  return 3   # Prepaids
                if code == '213100':        return 4   # Accrued Expenses
                if code == '213200':        return 5   # Accrued Interest Payable
                if code.startswith('213'):  return 6   # Other accrued
                return 9                                # Other

            # ── Group DR lines by CR account ────────────────────────────────
            _groups: dict = {}   # cr_code → list of DR lines
            for _dl in dr_lines:
                _je = _dl.get('je_number', '')
                _cr_info = _cr_lookup.get(_je, {})
                _cr_code = _cr_info.get('code', 'unknown')
                _groups.setdefault(_cr_code, []).append(_dl)

            _sorted_cr_codes = sorted(_groups.keys(), key=lambda c: (_section_sort_key(c), c))

            # ── Summary metrics row (by source type) ────────────────────────
            source_totals: dict = {}
            for _l in dr_lines:
                _src = _l.get('source', 'other')
                source_totals[_src] = source_totals.get(_src, 0) + (_l.get('debit') or 0)

            _total_je_count = len(set(_l.get('je_number', '') for _l in dr_lines))
            _total_amount   = sum(_l.get('debit') or 0 for _l in dr_lines)

            _metric_items = [('Total JEs', str(_total_je_count)),
                             ('Total Amount', f"${_total_amount:,.0f}")] + \
                            [(source_labels.get(s, s), f"${t:,.0f}") for s, t in source_totals.items()]
            _n_cols = min(len(_metric_items), 6)
            _metric_cols = st.columns(_n_cols)
            for _mi, (_lbl, _val) in enumerate(_metric_items[:_n_cols]):
                with _metric_cols[_mi]:
                    st.metric(_lbl, _val)

            st.write("")  # spacer

            # ── One expander per CR (BS) account ────────────────────────────
            _df_col_cfg = {
                "JE #":        st.column_config.TextColumn(width="small"),
                "Source":      st.column_config.TextColumn(width="small"),
                "Review":      st.column_config.TextColumn(width="medium"),
                "GL Account":  st.column_config.TextColumn(width="small"),
                "Description": st.column_config.TextColumn(width="large"),
                "Amount":      st.column_config.NumberColumn(format="$%,.2f"),
            }

            for _cr_code in _sorted_cr_codes:
                _group_lines = _groups[_cr_code]
                _group_total = sum(_l.get('debit') or 0 for _l in _group_lines)
                _group_count = len(set(_l.get('je_number', '') for _l in _group_lines))
                _section_title = _cr_section_label(_cr_code)
                _expander_label = (
                    f"CR {_section_title}  ·  {_group_count} JE{'s' if _group_count != 1 else ''}  "
                    f"·  ${_group_total:,.0f}"
                )

                with st.expander(_expander_label, expanded=True):
                    # Credit account summary line
                    st.caption(f"Credit account: **{_cr_code}** — all entries below post to this BS account")

                    _rows = []
                    for _l in _group_lines:
                        _src_label = source_labels.get(_l.get('source', ''), _l.get('source', ''))
                        _flag = ''
                        if _l.get('review_flag'):
                            _other = ', '.join(
                                source_labels.get(s, s) for s in (_l.get('review_sources') or [])
                            )
                            _flag = f'⚑ Also: {_other}'
                        _rows.append({
                            "JE #":        _l.get('je_number', ''),
                            "Source":      _src_label,
                            "Review":      _flag,
                            "GL Account":  _l.get('account_code', ''),
                            "Description": (_l.get('description') or '')[:80],
                            "Amount":      _l.get('debit') or 0,
                        })

                    st.dataframe(_rows, use_container_width=True, hide_index=True,
                                 column_config=_df_col_cfg)

                    # Subtotal row
                    _sub_cols = st.columns([4, 1])
                    with _sub_cols[1]:
                        st.markdown(
                            f"<div style='text-align:right; font-weight:bold; "
                            f"padding-top:4px'>Subtotal: ${_group_total:,.2f}</div>",
                            unsafe_allow_html=True,
                        )

            st.divider()
        else:
            st.info("No accrual entries generated. Upload a Nexus file, Budget Comparison, "
                    "or Prepaid Ledger to enable additional accrual detection layers.", icon="ℹ️")

        # ── Prepaid Amortization Panel ─────────────────────────────────────
        amort_lines = p1.get("amort_lines", [])
        if amort_lines:
            st.markdown("### Prepaid Expense Amortization")
            cur_lines = [l for l in amort_lines if l.get('is_current_period')]
            fut_lines = [l for l in amort_lines if not l.get('is_current_period')]
            col_p1, col_p2 = st.columns(2)
            with col_p1:
                st.metric("Current Period Expense", f"${sum(l['monthly_amount'] for l in cur_lines):,.2f}")
            with col_p2:
                st.metric("Future Periods (Prepaid Asset)", f"${sum(l['monthly_amount'] for l in fut_lines):,.2f}")
            amort_rows = [{
                "Vendor": l['vendor'], "Invoice #": l['invoice_number'],
                "Period": l['period_label'], "Month": f"{l['month_index']}/{l['total_months']}",
                "Monthly Amount": l['monthly_amount'], "GL Account": l['gl_account_number'],
                "Current Period": "Yes" if l.get('is_current_period') else "",
            } for l in amort_lines]
            st.dataframe(amort_rows, use_container_width=True, hide_index=True,
                         column_config={
                             "Vendor": st.column_config.TextColumn(width="medium"),
                             "Invoice #": st.column_config.TextColumn(width="small"),
                             "Period": st.column_config.TextColumn(width="small"),
                             "Month": st.column_config.TextColumn(width="small"),
                             "Monthly Amount": st.column_config.NumberColumn(format="$%,.2f"),
                             "GL Account": st.column_config.TextColumn(width="small"),
                             "Current Period": st.column_config.TextColumn(width="small"),
                         })
            st.divider()

        # ── Prepaid Ledger Status ──────────────────────────────────────────
        ledger_active    = p1.get("ledger_active", [])
        ledger_completed = p1.get("ledger_completed", [])
        newly_added      = p1.get("newly_added_prepaids", [])
        released_count   = p1.get("prepaid_released_count", 0)
        release_lines    = p1.get("prepaid_release_lines", [])
        if ledger_active or ledger_completed or newly_added:
            st.markdown("### Prepaid Ledger")
            col_l1, col_l2, col_l3, col_l4 = st.columns(4)
            with col_l1:
                st.metric("Active Prepaid Items", len(ledger_active))
            with col_l2:
                st.metric("Released This Month", released_count)
            with col_l3:
                st.metric("New This Month", len(newly_added))
            with col_l4:
                st.metric("Completed This Month", len(ledger_completed))

            # Diagnostic: active items but nothing released → period mismatch
            if ledger_active and released_count == 0:
                from dateutil.relativedelta import relativedelta as _rdelta
                import re as _re
                next_fires = []
                for _item in ledger_active:
                    _fap = _item.get('first_added_period', '')
                    _ma  = int(_item.get('months_amortized', 0) or 0)
                    _rem = int(_item.get('remaining_months', 0) or 0)
                    if _rem > 0 and _fap:
                        _m = _re.search(
                            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]?(\d{4})',
                            _fap, _re.IGNORECASE)
                        if _m:
                            _mo = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                                   'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}.get(
                                _m.group(1).lower(), 0)
                            if _mo:
                                from datetime import date as _date
                                _anchor = _date(int(_m.group(2)), _mo, 1)
                                _nf = _anchor + _rdelta(months=_ma)
                                next_fires.append(
                                    f"{_item.get('vendor','?')} — next: {_nf.strftime('%b-%Y')}"
                                )
                st.warning(
                    f"⚠️ **{len(ledger_active)} active prepaid item(s) but 0 released for "
                    f"{close_period}.** The ledger's `months_amortized` values don't match "
                    f"the current period — the uploaded ledger may be from a prior month. "
                    f"Upload the **updated Prepaid Ledger** from the previous close.\n\n"
                    + ("\n".join(f"• {f}" for f in next_fires[:8]) if next_fires else "")
                )
            if ledger_active:
                ledger_rows = [{
                    "Vendor": item.get('vendor', ''),
                    "Invoice #": item.get('invoice_number', ''),
                    "GL Account": item.get('gl_account_number', ''),
                    "Monthly Amt": item.get('monthly_amount', 0),
                    "Months Left": int(item.get('remaining_months', 0) or 0),
                    "Service End": str(item.get('service_end')) if item.get('service_end') else '',
                    "First Added": item.get('first_added_period', ''),
                } for item in ledger_active]
                st.dataframe(ledger_rows, use_container_width=True, hide_index=True,
                             column_config={
                                 "Vendor": st.column_config.TextColumn(width="medium"),
                                 "Invoice #": st.column_config.TextColumn(width="small"),
                                 "GL Account": st.column_config.TextColumn(width="small"),
                                 "Monthly Amt": st.column_config.NumberColumn(format="$%,.2f"),
                                 "Months Left": st.column_config.NumberColumn(width="small"),
                                 "Service End": st.column_config.TextColumn(width="small"),
                                 "First Added": st.column_config.TextColumn(width="small"),
                             })
            st.divider()

        # ── Download Section ───────────────────────────────────────────────
        st.markdown("### Download JE Files")
        st.caption("Upload these CSVs to Yardi, run the final close, then switch to **Pass 2** to generate reports.")

        # ── Accruals CSV content breakdown ──────────────────────────────
        _accrual_lines_display = [l for l in all_je_lines
                                  if l.get('source') in {
                                      'nexus', 'budget_gap', 'historical', 'management_fee',
                                      'management_fee_catchup', 'invoice_proration',
                                      'prepaid_amortization', 'contract_supplement',
                                      'tenant_utility_billing', 'bonus_accrual', 'prepaid_ledger',
                                  }]
        _src_label_map = {
            'nexus':                  'Nexus AP',
            'invoice_proration':      'Invoice Proration',
            'historical':             'Historical Pattern',
            'budget_gap':             'Budget Gap',
            'prepaid_amortization':   'Prepaid Amort.',
            'prepaid_ledger':         'Prepaid Release',
            'management_fee':         'Management Fee',
            'management_fee_catchup': 'Mgmt Fee Catch-up',
            'contract_supplement':    'One-Off Accrual',
            'tenant_utility_billing': 'Tenant Utility',
            'bonus_accrual':          'Bonus Accrual',
        }
        # Count unique JEs (not lines) per source — DR lines only to avoid double-count
        _src_je_counts = {}
        for _l in _accrual_lines_display:
            if (_l.get('debit') or 0) > 0:
                _src = _l.get('source', 'other')
                _je_id = _l.get('je_number', '')
                _src_je_counts.setdefault(_src, set()).add(_je_id)
        _src_je_summary = {_src_label_map.get(s, s): len(ids)
                           for s, ids in _src_je_counts.items() if ids}

        if _src_je_summary:
            _ppd_count = _src_je_counts.get('prepaid_ledger', set())
            _total_je_in_csv = sum(len(v) for v in _src_je_counts.values())
            _breakdown_parts = [f"{lbl}: {cnt}" for lbl, cnt in _src_je_summary.items()]
            _breakdown_str = "  ·  ".join(_breakdown_parts)
            if _ppd_count:
                st.success(
                    f"**Accruals CSV contains {_total_je_in_csv} JEs** — {_breakdown_str}",
                    icon="✅",
                )
            else:
                st.info(
                    f"**Accruals CSV contains {_total_je_in_csv} JEs** — {_breakdown_str}  "
                    f"*(No prepaid releases — upload prior-month ledger and re-run if expected)*",
                    icon="📄",
                )

        # Zip of all 3 CSVs + updated ledger
        import zipfile, io
        _run_key = st.session_state.get('pass1_run_count', 0)
        period_label = (result.period or 'Period').replace('-', '_')
        p1_zip_files = {
            f"RevLabs_{period_label}_Accruals_JE.csv":      p1.get("accrual_je_csv"),
            f"RevLabs_{period_label}_Prepaid_Ledger.xlsx":  p1.get("prepaid_ledger_updated"),
        }
        p1_zip_files = {k: v for k, v in p1_zip_files.items() if v and os.path.exists(v)}
        if p1_zip_files:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fname, fpath in p1_zip_files.items():
                    zf.write(fpath, fname)
            zip_buf.seek(0)
            st.download_button(
                label=f"📦 Download All JE Files ({len(p1_zip_files)} files)",
                data=zip_buf,
                file_name=f"RevLabs_{period_label}_JE_Package_{datetime.now().strftime('%Y%m%d')}.zip",
                mime="application/zip",
                key=f"dl_zip_{_run_key}",
                use_container_width=True,
            )

        col_d1, col_d2 = st.columns(2)
        for col, key, label, fname in [
            (col_d1, "accrual_je_csv",        "📄 Accruals JE",    f"GA_Accruals_JE_{datetime.now().strftime('%Y%m%d')}.csv"),
            (col_d2, "prepaid_ledger_updated", "📊 Prepaid Ledger", f"GA_Prepaid_Ledger_{datetime.now().strftime('%Y%m%d')}.xlsx"),
        ]:
            fpath = p1.get(key)
            if fpath and os.path.exists(fpath):
                with col:
                    with open(fpath, "rb") as f:
                        st.download_button(
                            label=label,
                            data=f.read(),
                            file_name=fname,
                            key=f"dl_{key}_{_run_key}",
                            use_container_width=True,
                        )

        st.divider()
        st.info(
            "📌 **Next step:** Upload the JE CSVs to Yardi and run the final close. "
            "Then switch to the **Pass 2 — Generate Reports** tab to produce the "
            "BS workpaper, QC checklist, and variance comments.",
            icon="➡️",
        )


# ──────────────────────────────────────────────────────────────
# TAB 2 — PASS 2: REPORT GENERATION
# ──────────────────────────────────────────────────────────────
with tab2:
    # Recompute upload state (defined in tab1, available via session_state)
    uploaded_keys = set(st.session_state.uploaded_files.keys())
    gl_uploaded = "gl" in uploaded_keys

    st.markdown("""
    **What this does:** Reads the final post-close Yardi GL (after all JEs have been posted)
    and generates the GRP review deliverables — Balance Sheet workpaper, institutional workpapers,
    QC checklist, variance comments, and exception report.

    *(The Singerman monthly report is downloaded directly from Yardi — no need to generate it here.)*
    """)

    if not st.session_state.pass1_complete:
        st.info(
            "ℹ️ Pass 1 hasn't been run yet in this session. If you've already uploaded the "
            "JE CSVs to Yardi and have the final GL ready, you can still run Pass 2 independently.",
            icon="ℹ️",
        )

    st.divider()

    # ── Pass 2: Bulk file upload ──────────────────────────────────────────────
    st.markdown("#### Upload Pass 2 Files")
    st.caption(
        "Drop all post-close files here at once — Final GL, Budget Comparison, "
        "Trial Balance, T12, Berkadia loan statements, and prior workpaper. "
        "The app auto-detects each file type."
    )

    # ── Pass 2 bulk uploader session state ───────────────────────────────────
    if "bulk_overrides_p2" not in st.session_state:
        st.session_state.bulk_overrides_p2 = {}

    _P2_SLOT_KEYS = [
        "gl_pass2", "budget_comparison_pass2", "trial_balance_pass2",
        "t12_statement_pass2", "loan_pass2", "prior_workpaper", "unknown",
    ]
    _P2_SLOT_LABELS = [_FILE_LABELS.get(k, k) for k in _P2_SLOT_KEYS]

    _bulk_p2 = st.file_uploader(
        "Drop all Pass 2 files here",
        accept_multiple_files=True,
        type=["xlsx", "xls", "pdf"],
        key="bulk_upload_p2",
    )

    # Clear Pass 2 slots so stale entries don't persist after file removal
    for _clr_k2 in set(_P2_SLOT_KEYS) - {"unknown"}:
        st.session_state.uploaded_files.pop(_clr_k2, None)

    if _bulk_p2:
        _loan_paths_p2: list = []

        for _uf2 in _bulk_p2:
            _raw2 = bytes(_uf2.getbuffer())
            _det_key2, _conf2, _det_label2 = _classify_file(_uf2.name, _raw2, pass2=True)

            # If classifier returned a base key that isn't in the P2 slot list,
            # keep it as-is (e.g. prior_workpaper has no remap)
            if _det_key2 not in _P2_SLOT_KEYS:
                _det_key2 = "unknown"

            _eff_key2 = st.session_state.bulk_overrides_p2.get(_uf2.name, _det_key2)

            _ic2, _fn2, _tp2 = st.columns([1, 3, 4])
            if _eff_key2 == "unknown":
                _ic2.markdown("⚠️")
            elif _conf2 >= 0.85:
                _ic2.markdown("✅")
            else:
                _ic2.markdown("🟡")

            _short2 = _uf2.name if len(_uf2.name) <= 22 else _uf2.name[:19] + "…"
            _fn2.caption(_short2)

            if _eff_key2 == "unknown" or _conf2 < 0.70:
                _cur_idx2 = (_P2_SLOT_KEYS.index(_eff_key2)
                             if _eff_key2 in _P2_SLOT_KEYS else len(_P2_SLOT_KEYS) - 1)
                _sel_label2 = _tp2.selectbox(
                    "type", _P2_SLOT_LABELS, index=_cur_idx2,
                    key=f"ovr_p2_{_uf2.name}", label_visibility="collapsed",
                )
                _eff_key2 = _P2_SLOT_KEYS[_P2_SLOT_LABELS.index(_sel_label2)]
                st.session_state.bulk_overrides_p2[_uf2.name] = _eff_key2
            else:
                _tp2.caption(_det_label2)

            if _eff_key2 != "unknown":
                _tp2_path = os.path.join(st.session_state.temp_dir, f"p2_{_uf2.name}")
                if not os.path.exists(_tp2_path) or os.path.getsize(_tp2_path) != _uf2.size:
                    with open(_tp2_path, "wb") as _f2:
                        _f2.write(_raw2)
                if _eff_key2 in _MULTI_FILE_KEYS:
                    _loan_paths_p2.append(_tp2_path)
                else:
                    st.session_state.uploaded_files[_eff_key2] = _tp2_path

        if _loan_paths_p2:
            st.session_state.uploaded_files["loan_pass2"] = _loan_paths_p2

        # Show fallback notes for optional slots not yet uploaded
        _p2_loaded = set(st.session_state.uploaded_files.keys())
        if "budget_comparison_pass2" not in _p2_loaded and "budget_comparison" in _p2_loaded:
            st.caption("↳ BC: using Pass 1 version — upload post-close BC above to override")
        if "trial_balance_pass2" not in _p2_loaded and "trial_balance" in _p2_loaded:
            st.caption("↳ TB: using Pass 1 version — upload post-close TB above to override")
        if "t12_statement_pass2" not in _p2_loaded and "t12_statement" in _p2_loaded:
            st.caption("↳ T12: using Pass 1 version — upload post-close T12 above to override")
        if "loan_pass2" not in _p2_loaded and "loan" in _p2_loaded:
            st.caption("↳ Loan: using Pass 1 version — upload post-close PDFs above to override")

        # Clean up overrides for files no longer in uploader
        _active2 = {_uf2.name for _uf2 in _bulk_p2}
        st.session_state.bulk_overrides_p2 = {
            k: v for k, v in st.session_state.bulk_overrides_p2.items()
            if k in _active2
        }

    st.text_input(
        "Prior period label",
        placeholder="e.g. Mar-2026  (leave blank to auto-detect)",
        help=(
            "The period that the uploaded prior workpaper covers. "
            "Required when uploading a workpaper that doesn't have period-prefixed tabs. "
            "Format: Mon-YYYY (e.g. Mar-2026, Feb-2026)."
        ),
        key="prior_period_label_input",
    )

    # Pass 2 requires either a dedicated post-close GL or at minimum the sidebar GL
    _p2_gl_ready = (
        "gl_pass2" in st.session_state.uploaded_files
        or gl_uploaded
    )

    st.divider()

    # ── Pass 2 Run Button ─────────────────────────────────────────────────────
    col_p2a, col_p2b = st.columns([3, 1])
    with col_p2a:
        pass2_button = st.button(
            "📊 Generate Reports",
            disabled=not _p2_gl_ready,
            use_container_width=True,
            key="pass2_run_btn",
            help="Parse final post-close GL and generate all workpapers and reports",
        )
    with col_p2b:
        if st.button("🔄 Reset Pass 2", use_container_width=True, key="reset_pass2"):
            st.session_state.pass2_complete = False
            st.session_state.pass2_engine_result = None
            st.session_state.pass2_output_files = {}
            import pandas as _pd_r2
            st.session_state.post_close_je_df = _pd_r2.DataFrame({
                "JE #": ["PC-001", "PC-001"], "Description": ["", ""],
                "Account Code": ["", ""],
                "Debit ($)": [0.0, 0.0], "Credit ($)": [0.0, 0.0],
                "Line Description": ["", ""],
            })
            for _k in ("gl_pass2", "budget_comparison_pass2", "trial_balance_pass2",
                       "t12_statement_pass2", "loan_pass2", "prior_workpaper"):
                st.session_state.uploaded_files.pop(_k, None)
            st.session_state.bulk_overrides_p2 = {}
            st.rerun()

    # ── Pass 2 Processing ─────────────────────────────────────────────────────
    if pass2_button:
        with st.spinner("Generating reports from final GL..."):
            try:
                # Build files dict from shared sidebar uploads, then override
                # with any Pass 2-specific files that were uploaded above.
                files_dict = {key: st.session_state.uploaded_files.get(key)
                              for key in file_config.keys()}
                if st.session_state.uploaded_files.get("gl_pass2"):
                    files_dict["gl"] = st.session_state.uploaded_files["gl_pass2"]
                if st.session_state.uploaded_files.get("budget_comparison_pass2"):
                    files_dict["budget_comparison"] = st.session_state.uploaded_files["budget_comparison_pass2"]
                if st.session_state.uploaded_files.get("trial_balance_pass2"):
                    files_dict["trial_balance"] = st.session_state.uploaded_files["trial_balance_pass2"]
                if st.session_state.uploaded_files.get("loan_pass2"):
                    files_dict["loan"] = st.session_state.uploaded_files["loan_pass2"]

                progress_bar = st.progress(0)
                status_text  = st.empty()

                # Step 1: Parse final (post-close) GL
                status_text.text("Step 1/6: Parsing final GL...")
                progress_bar.progress(10)
                engine_result = run_pipeline(
                    files_dict,
                    prior_period_outstanding=prior_period_outstanding,
                )
                st.session_state.pass2_engine_result = engine_result

                gl_parsed    = engine_result.parsed.get('gl')
                bc_parsed    = engine_result.parsed.get('budget_comparison') or []
                close_period = engine_result.period or ''

                # Step 2: Parse trial balance + BS workpaper (no je_adjustments — GL is final)
                # Note: Singerman monthly report is downloaded directly from Yardi by Ryan.
                status_text.text("Step 2/6: Generating BS workpaper...")
                progress_bar.progress(25)

                tb_result = None
                # Resolve TB path: Pass 2 upload takes priority over sidebar upload
                _tb_file = (st.session_state.uploaded_files.get("trial_balance_pass2")
                            or st.session_state.uploaded_files.get("trial_balance"))
                if _tb_file:
                    try:
                        from parsers.yardi_trial_balance import parse as parse_tb
                        tb_result = parse_tb(_tb_file)
                    except Exception as _e:
                        st.warning(f"Could not parse Trial Balance: {_e}")

                # Parse 12-Month Statement — Pass 2 upload takes priority over sidebar
                # Post-close T12 has the current period's JEs baked in, so MoM prior month
                # is sourced from T12 and current month from BC PTD Actual (post-close GL).
                t12_result = None
                _t12_file = (st.session_state.uploaded_files.get("t12_statement_pass2")
                             or st.session_state.uploaded_files.get("t12_statement"))
                if _t12_file and os.path.exists(_t12_file):
                    try:
                        from parsers.yardi_t12 import parse as parse_t12
                        t12_result = parse_t12(_t12_file)
                    except Exception as _e:
                        st.warning(f"Could not parse 12-Month Statement: {_e}")

                # Parse bank statements for BS workpaper
                _gl_result = engine_result.parsed.get('gl')
                bank_rec_data = engine_result.parsed.get("bank_rec")
                gl_cash_balance = None
                if bank_rec_data and _gl_result:
                    for _a in (_gl_result.accounts or []):
                        if _a.account_code == '111100':
                            gl_cash_balance = _a.ending_balance
                            break

                _daca_file = st.session_state.uploaded_files.get("daca_bank")
                daca_bank_data = None
                daca_gl_balance = None
                if _daca_file and os.path.exists(_daca_file):
                    try:
                        from parsers.keybank_daca import parse as _parse_daca2
                        daca_bank_data = _parse_daca2(_daca_file)
                    except Exception:
                        daca_bank_data = None
                if daca_bank_data and _gl_result:
                    for _a in (_gl_result.accounts or []):
                        if _a.account_code == '115100':
                            daca_gl_balance = _a.ending_balance
                            break

                _dev_rec_file = st.session_state.uploaded_files.get("bank_rec_dev")
                dev_bank_rec_data = None
                if _dev_rec_file and os.path.exists(_dev_rec_file):
                    try:
                        from parsers.bofa_statement import parse as _parse_bofa
                        dev_bank_rec_data = _parse_bofa(_dev_rec_file)
                    except Exception:
                        dev_bank_rec_data = None

                if tb_result and gl_parsed:
                    try:
                        bs_wp_path = os.path.join(st.session_state.temp_dir, "GA_Workpapers.xlsx")
                        # GL is final — no je_adjustments needed. The GL already reflects
                        # all posted JEs from Pass 1, so the workpaper ties clean.
                        _prior_wp_path = st.session_state.uploaded_files.get("prior_workpaper")
                        # Prior period label: user override → auto-infer from close_period
                        _user_label = st.session_state.get("prior_period_label_input", "").strip()
                        _prior_period = _user_label if _user_label else None
                        if not _prior_period:
                            try:
                                if close_period:
                                    _mo_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,
                                                   Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
                                    _m2 = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ](\d{4})', close_period)
                                    if _m2:
                                        _mo = _mo_map[_m2.group(1)]
                                        _yr = int(_m2.group(2))
                                        _prev_mo = _mo - 1 if _mo > 1 else 12
                                        _prev_yr = _yr if _mo > 1 else _yr - 1
                                        _mo_names = {v: k for k, v in _mo_map.items()}
                                        _prior_period = f"{_mo_names[_prev_mo]}-{_prev_yr}"
                            except Exception:
                                _prior_period = None
                        _berkadia_loan_data = engine_result.parsed.get('loan')
                        _berkadia_loans = []
                        if isinstance(_berkadia_loan_data, list):
                            _berkadia_loans = _berkadia_loan_data
                        elif isinstance(_berkadia_loan_data, dict):
                            _berkadia_loans = _berkadia_loan_data.get('loans', [])
                        elif hasattr(_berkadia_loan_data, 'loans'):
                            _berkadia_loans = _berkadia_loan_data.loans
                        bs_workpaper_generator.generate(
                            gl_result=gl_parsed,
                            tb_result=tb_result,
                            output_path=bs_wp_path,
                            period=close_period,
                            property_name=engine_result.property_name or 'Revolution Labs',
                            prepaid_ledger_active=[],
                            bank_rec_data=bank_rec_data,
                            gl_cash_balance=gl_cash_balance,
                            daca_bank_data=daca_bank_data,
                            daca_gl_balance=daca_gl_balance,
                            prior_workpaper_path=_prior_wp_path,
                            prior_period=_prior_period,
                            berkadia_loans=_berkadia_loans,
                            dev_bank_rec_data=dev_bank_rec_data,
                        )
                        st.session_state.pass2_output_files["bs_workpaper"] = bs_wp_path
                    except Exception as _e:
                        st.warning(f"Workpaper generation skipped: {_e}")
                else:
                    if not tb_result:
                        st.info("Upload a Trial Balance file to enable the Workpaper.", icon="ℹ️")

                # Step 3: (Institutional workpapers removed — not needed)

                # Step 4: Management fee (informational — already in GL)
                status_text.text("Step 4/6: Verifying management fee...")
                progress_bar.progress(58)
                try:
                    _rd_file_p2 = st.session_state.uploaded_files.get("receivable_detail")
                    _rd_parsed_p2 = None
                    if _rd_file_p2 and os.path.exists(_rd_file_p2):
                        try:
                            from parsers.yardi_receivable_detail import parse as _parse_rd2
                            _rd_parsed_p2 = _parse_rd2(_rd_file_p2)
                        except Exception:
                            _rd_parsed_p2 = None

                    _ar_aging_file_p2 = st.session_state.uploaded_files.get("ar_aging")
                    _ar_aging_parsed_p2 = None
                    if _ar_aging_file_p2 and os.path.exists(_ar_aging_file_p2):
                        try:
                            from parsers.yardi_ar_aging import parse as _parse_ar_aging2
                            _ar_aging_parsed_p2 = _parse_ar_aging2(_ar_aging_file_p2)
                        except Exception:
                            _ar_aging_parsed_p2 = None

                    fee_result = calculate_mgmt_fee(
                        gl_parsed=gl_parsed,
                        budget_rows=bc_parsed or [],
                        daca_parsed=daca_bank_data,
                        receivable_detail=_rd_parsed_p2,
                        ar_aging=_ar_aging_parsed_p2,
                    )
                    st.session_state.pass2_output_files["fee_result"] = fee_result
                except Exception:
                    fee_result = None
                    st.session_state.pass2_output_files["fee_result"] = None

                # Step 5: QC engine
                status_text.text("Step 5/6: Running QC checks...")
                progress_bar.progress(72)

                kardin_records = engine_result.parsed.get("kardin_budget") or []
                _period_month = 1
                try:
                    _m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', close_period)
                    _month_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
                    if _m:
                        _period_month = _month_map.get(_m.group(1), 1)
                except Exception:
                    pass

                try:
                    qc_report = run_qc(
                        budget_rows=bc_parsed or [],
                        tb_result=tb_result,
                        gl_parsed=gl_parsed,
                        kardin_records=kardin_records,
                        accrual_entries=[],   # JEs already posted — don't re-evaluate
                        period=close_period,
                        property_name=engine_result.property_name or 'Revolution Labs Owner, LLC',
                        period_month=_period_month,
                        cash_received=fee_result.cash_received if fee_result and fee_result.cash_received > 0 else None,
                        loan_data=engine_result.parsed.get('loan'),
                    )
                    st.session_state.pass2_output_files["qc_report"] = qc_report
                    qc_path = os.path.join(st.session_state.temp_dir, "GA_QC_Workbook.xlsx")
                    generate_qc_workbook(
                        qc_report, qc_path,
                        tb_result=tb_result,
                        budget_rows=bc_parsed or [],
                        gl_parsed=gl_parsed,
                        loan_data=engine_result.parsed.get('loan'),
                        period_month=_period_month,
                        t12_result=t12_result,
                    )
                    st.session_state.pass2_output_files["qc_workbook"] = qc_path
                except Exception as _e:
                    st.warning(f"QC engine skipped: {_e}")
                    st.session_state.pass2_output_files["qc_report"] = None

                # Step 6: Variance comments + annotated BC
                # No je_adjustments — the final GL already reflects all posted JEs.
                status_text.text("Step 6/6: Generating variance comments...")
                progress_bar.progress(87)

                api_key = None
                try:
                    api_key = st.secrets.get("ANTHROPIC_API_KEY")
                except Exception:
                    pass

                _api_fallback_reason = None
                if bc_parsed:
                    try:
                        comments_map = generate_variance_comments_grp(
                            budget_rows=bc_parsed,
                            gl_parsed=gl_parsed,
                            kardin_records=kardin_records,
                            period=close_period,
                            property_name=engine_result.property_name or 'Revolution Labs Owner, LLC',
                            api_key=api_key,
                            # No je_adjustments — GL is the final source of truth
                        )
                        _fallback_reasons = {
                            entry.get('_api_fallback')
                            for entry in comments_map.values()
                            if entry.get('_api_fallback')
                        }
                        _api_fallback_reason = next(iter(_fallback_reasons), None)
                        if api_key and _api_fallback_reason:
                            st.warning(
                                f"⚠️ **Variance commentary fallback:** API was requested but failed. "
                                f"Comments were generated from data-driven templates, not AI. "
                                f"**Do not sign off on commentary until this is resolved.**\n\n"
                                f"Reason: {_api_fallback_reason}"
                            )
                        method = 'api' if (api_key and not _api_fallback_reason) else 'data-driven'

                        var_comments = [
                            {
                                'account_code': code,
                                'account_name': entry['account_name'],
                                'comment': entry.get('mtd_comment', ''),
                                'ytd_comment': entry.get('ytd_comment', ''),
                                'mtd_tier': entry.get('mtd_tier', 'tier_3'),
                                'ytd_tier': entry.get('ytd_tier', 'tier_3'),
                                'method': method,
                            }
                            for code, entry in comments_map.items()
                            if entry.get('mtd_tier') != 'tier_3' or entry.get('ytd_tier') != 'tier_3'
                        ]
                        st.session_state.pass2_output_files["variance_comments"] = var_comments

                        # Annotated BC (GRP internal) — prefer Pass 2 final-close BC over sidebar
                        _bc_file = (
                            st.session_state.uploaded_files.get("budget_comparison_pass2")
                            or st.session_state.uploaded_files.get("budget_comparison")
                        )
                        if _bc_file and os.path.exists(_bc_file):
                            _annotated_bc_path = os.path.join(
                                st.session_state.temp_dir, "GA_Budget_Comparison_Internal.xlsx"
                            )
                            write_comments_to_budget_comparison(
                                input_path=_bc_file,
                                output_path=_annotated_bc_path,
                                comments=comments_map,
                            )
                            st.session_state.pass2_output_files["annotated_bc"] = _annotated_bc_path
                    except Exception as _e:
                        st.warning(f"Variance comments skipped: {_e}")
                        st.session_state.pass2_output_files["variance_comments"] = []
                else:
                    st.session_state.pass2_output_files["variance_comments"] = []

                # Exception report
                if api_key and _api_fallback_reason:
                    engine_result.exceptions.append(Exception_(
                        severity='warning',
                        category='commentary',
                        source='variance_comments',
                        description=(
                            'Variance commentary API fallback: AI commentary was requested but '
                            f'the API call failed. Reason: {_api_fallback_reason}'
                        ),
                        details={'api_fallback_reason': _api_fallback_reason},
                    ))

                exception_path = os.path.join(st.session_state.temp_dir, "GA_Exceptions_Report.xlsx")
                try:
                    generate_exception_report(engine_result, exception_path)
                    st.session_state.pass2_output_files["exception_report"] = exception_path
                except Exception as _e:
                    st.warning(f"Exception report skipped: {_e}")

                # Store auxiliary pass2 data for dashboard
                st.session_state.pass2_output_files["tb_result"]         = tb_result
                st.session_state.pass2_output_files["bank_rec_data"]     = bank_rec_data
                st.session_state.pass2_output_files["daca_bank_data"]    = daca_bank_data
                st.session_state.pass2_output_files["gl_cash_balance"]   = gl_cash_balance
                st.session_state.pass2_output_files["daca_gl_balance"]   = daca_gl_balance
                st.session_state.pass2_output_files["dev_bank_rec_data"] = dev_bank_rec_data

                progress_bar.progress(100)
                status_text.text("✓ Reports complete!")
                st.session_state.pass2_complete = True
                st.success("Pass 2 complete! Download reports below.", icon="✅")

            except Exception as e:
                tb = traceback.format_exc()
                st.error(f"Pass 2 error: {str(e)}", icon="❌")
                st.code(tb, language="python")
                st.session_state.pass2_complete = False

    # ── Pass 2 Results Dashboard ──────────────────────────────────────────────
    if st.session_state.pass2_complete and st.session_state.pass2_engine_result:
        result = st.session_state.pass2_engine_result
        p2     = st.session_state.pass2_output_files

        st.divider()
        st.markdown(f"## Pass 2 Results — {result.period}  |  {result.property_name}")

        # Status banner
        status = result.status
        status_color = {"CLEAN": "#2ecc71", "WARNINGS": "#f39c12"}.get(status, "#e74c3c")
        status_text_str = {"CLEAN": "✅ CLEAN", "WARNINGS": "⚠️ WARNINGS"}.get(status, "❌ ERRORS")
        st.markdown(f"""
        <div style="background-color: {status_color}20; border-left: 5px solid {status_color};
             padding: 15px; border-radius: 5px; margin: 15px 0;">
            <h3 style="color: {status_color}; margin: 0;">{status_text_str}</h3>
        </div>
        """, unsafe_allow_html=True)

        # Period-state indicator
        _ps = result.period_state
        if _ps and _ps.get('state') != 'unknown':
            _state_labels = {
                'pre_close':  ('🟡 Pre-Close', '#f39c12',
                               'GL is for the current period — books still open.'),
                'at_close':   ('🟢 At Close', '#2ecc71', 'Month-end close window.'),
                'post_close': ('🔵 Post-Close', '#3498db',
                               'Running on a closed period. Reports are final.'),
            }
            _label, _color, _desc = _state_labels.get(_ps['state'], ('⚪ Unknown', '#95a5a6', ''))
            _days = _ps.get('days_since_close', 0)
            _day_str = (f'{abs(_days)} day{"s" if abs(_days) != 1 else ""} until close'
                        if _days < 0 else
                        f'{_days} day{"s" if _days != 1 else ""} since close'
                        if _days > 0 else 'closes today')
            _promo = ' _(promoted by 213100 GL signal)_' if _ps.get('promoted') else ''
            with st.expander(f"{_label} — {_day_str}{_promo}", expanded=False):
                st.markdown(_desc)
                if _ps.get('gl_signal_detected'):
                    st.info(
                        f"213100 GL signal detected: net credit ${_ps['gl_signal_amount']:,.2f} "
                        f"— prior-period auto-reversals have posted."
                    )

        # ── QC Summary Panel ───────────────────────────────────────────────
        qc_report = p2.get("qc_report")
        if qc_report:
            st.markdown("### QC Checks")
            qc_overall = qc_report.overall_status
            qc_color = {'PASS': '#2ecc71', 'FLAG': '#f39c12', 'FAIL': '#e74c3c'}.get(qc_overall, '#95a5a6')
            qc_icon  = {'PASS': '✅', 'FLAG': '⚠️', 'FAIL': '❌'}.get(qc_overall, 'ℹ️')
            st.markdown(f"""
            <div style="background:{qc_color}20;border-left:5px solid {qc_color};
                 padding:10px 15px;border-radius:5px;margin:10px 0 5px;">
                <strong style="color:{qc_color};">{qc_icon} QC Overall: {qc_overall}</strong>
                &nbsp;&nbsp;{sum(1 for c in qc_report.checks if c.status=='PASS')} PASS &nbsp;
                {sum(1 for c in qc_report.checks if c.status=='FLAG')} FLAG &nbsp;
                {sum(1 for c in qc_report.checks if c.status=='FAIL')} FAIL
            </div>
            """, unsafe_allow_html=True)
            qc_rows = []
            for chk in qc_report.checks:
                chk_icon = {'PASS': '✅', 'FLAG': '⚠️', 'FAIL': '❌'}.get(chk.status, '')
                qc_rows.append({
                    "Check":    chk.check_id.replace('CHECK_', '') + ' — ' + chk.check_name,
                    "Status":   f"{chk_icon} {chk.status}",
                    "Findings": chk.flag_count,
                    "Summary":  chk.summary,
                })
            st.dataframe(qc_rows, use_container_width=True, hide_index=True,
                         column_config={
                             "Check":    st.column_config.TextColumn(width="medium"),
                             "Status":   st.column_config.TextColumn(width="small"),
                             "Findings": st.column_config.NumberColumn(width="small"),
                             "Summary":  st.column_config.TextColumn(width="large"),
                         })
            st.divider()

        # ── Management Fee Panel ───────────────────────────────────────────
        fee_result = p2.get("fee_result")
        if fee_result and fee_result.cash_received > 0:
            st.markdown("### Management Fee Verification")
            st.caption("Computed from the final GL — used to verify the posted JE is correct.")
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                st.metric("Cash Received", f"${fee_result.cash_received:,.0f}",
                          help=f"Source: {fee_result.cash_source}")
            with col_f2:
                st.metric(f"JLL ({fee_result.jll_rate:.2%})", f"${fee_result.jll_fee:,.0f}")
            with col_f3:
                st.metric(f"GRP ({fee_result.grp_rate:.2%})", f"${fee_result.grp_fee:,.0f}")
            with col_f4:
                st.metric(f"Total ({fee_result.total_rate:.2%})", f"${fee_result.total_fee:,.0f}")
            bc_for_fee = result.parsed.get('budget_comparison') or []
            accrued = accrued_fee_from_bc(bc_for_fee)
            if accrued > 0:
                diff = accrued - fee_result.total_fee
                diff_str = f"${abs(diff):,.0f} {'over' if diff > 0 else 'under'} calculated"
                st.caption(f"BC accrued (637130): ${accrued:,.2f} — {diff_str}")
            st.divider()

        # ── Budget Variances with Comments ─────────────────────────────────
        if result.budget_variances:
            st.markdown("### Budget Variances (Flagged)")
            var_comments = p2.get("variance_comments", [])
            comments_map_disp = {vc['account_code']: vc.get('comment', '') for vc in var_comments}
            comment_method = var_comments[0].get('method', 'none') if var_comments else 'none'
            if comment_method == 'api':
                st.caption("Variance comments generated via Claude API")
            elif comment_method == 'data-driven':
                st.caption(
                    "Variance comments generated from GL transaction detail "
                    "(configure ANTHROPIC_API_KEY in Streamlit secrets for narrative comments)"
                )
            variance_data = []
            for var in result.budget_variances:
                code = var.get("account_code", "")
                variance_data.append({
                    "Account Code": code,
                    "Account Name": var.get("account_name", ""),
                    "Actual":       var.get("ptd_actual", 0),
                    "Budget":       var.get("ptd_budget", 0),
                    "Variance":     var.get("variance", 0),
                    "Variance %":   f"{var.get('variance_pct', 0):.1f}%",
                    "Comment":      comments_map_disp.get(code, ''),
                })
            st.dataframe(variance_data, use_container_width=True, hide_index=True,
                         column_config={
                             "Account Code": st.column_config.TextColumn(width="small"),
                             "Account Name": st.column_config.TextColumn(width="medium"),
                             "Actual":       st.column_config.NumberColumn(format="$%,.2f"),
                             "Budget":       st.column_config.NumberColumn(format="$%,.2f"),
                             "Variance":     st.column_config.NumberColumn(format="$%,.2f"),
                             "Variance %":   st.column_config.TextColumn(width="small"),
                             "Comment":      st.column_config.TextColumn(width="large"),
                         })
            st.divider()

        # ── Bank Rec Summary Panel ─────────────────────────────────────────
        _bank_rec  = p2.get("bank_rec_data")
        _daca_data = p2.get("daca_bank_data")
        _dev_rec   = p2.get("dev_bank_rec_data")
        _gl_111    = float(p2.get("gl_cash_balance") or 0)
        _daca_gl   = float(p2.get("daca_gl_balance") or 0)
        if _bank_rec or _daca_data or _dev_rec:
            st.markdown("### Bank Reconciliation Summary")
            _rec_cols = st.columns(3)
            with _rec_cols[0]:
                if _bank_rec:
                    _bank_bal  = float(_bank_rec.get('bank_statement_balance') or 0)
                    _out_total = float(_bank_rec.get('total_outstanding_checks') or 0)
                    _rec_bal   = float(_bank_rec.get('reconciled_bank_balance') or 0)
                    _diff_111  = _rec_bal - _gl_111
                    _icon_111  = "✅" if abs(_diff_111) < 0.02 else "❌"
                    st.markdown(f"""
**PNC Operating (x3993) — GL 111100** {_icon_111}
| | |
|---|---:|
| Bank Statement Balance | ${_bank_bal:,.2f} |
| Less: Outstanding Checks ({len(_bank_rec.get('outstanding_checks') or [])}) | (${_out_total:,.2f}) |
| Reconciled Bank Balance | **${_rec_bal:,.2f}** |
| GL Balance (111100) | ${_gl_111:,.2f} |
| **Difference** | **${_diff_111:+,.2f}** |
""")
                else:
                    st.caption("Upload Yardi Bank Rec PDF to see Operating account rec summary")
            with _rec_cols[1]:
                if _daca_data:
                    _daca_end  = float(_daca_data.get('ending_balance') or 0)
                    _diff_daca = _daca_end - _daca_gl
                    _icon_daca = "✅" if abs(_diff_daca) < 0.02 else "❌"
                    st.markdown(f"""
**KeyBank DACA (x5132) — GL 115100** {_icon_daca}
| | |
|---|---:|
| Bank Statement Ending Balance | ${_daca_end:,.2f} |
| GL Balance (115100) | ${_daca_gl:,.2f} |
| **Difference** | **${_diff_daca:+,.2f}** |
""")
                else:
                    st.caption("Upload DACA Bank Statement to see DACA account rec summary")
            with _rec_cols[2]:
                if _dev_rec:
                    _dev_bank_bal  = float(_dev_rec.get('bank_statement_balance') or 0)
                    _dev_out_total = float(_dev_rec.get('total_outstanding_checks') or 0)
                    _dev_rec_bal   = float(_dev_rec.get('reconciled_bank_balance') or 0)
                    _dev_gl_bal    = float(_dev_rec.get('gl_balance') or 0)
                    _dev_diff      = _dev_rec_bal - _dev_gl_bal
                    _dev_icon      = "✅" if abs(_dev_diff) < 0.02 else "❌"
                    st.markdown(f"""
**Development Account — revlabs** {_dev_icon}
| | |
|---|---:|
| Bank Statement Balance | ${_dev_bank_bal:,.2f} |
| Less: Outstanding Checks ({len(_dev_rec.get('outstanding_checks') or [])}) | (${_dev_out_total:,.2f}) |
| Reconciled Bank Balance | **${_dev_rec_bal:,.2f}** |
| GL Balance (per Yardi rec) | ${_dev_gl_bal:,.2f} |
| **Difference** | **${_dev_diff:+,.2f}** |
""")
                else:
                    st.caption("Upload revlabs Bank Rec PDF to see Development account rec summary")
            st.divider()

        # ── Engine Bank Match Detail (collapsible) ─────────────────────────
        if result.gl_bank_matches:
            with st.expander("Engine Bank Match Detail"):
                recon_data = [{
                    "Description": match.description,
                    "GL Amount":   match.amount_a,
                    "Bank Amount": match.amount_b,
                    "Matched":     "✅" if match.matched else "⚠️",
                    "Variance":    abs(match.variance),
                } for match in result.gl_bank_matches]
                st.dataframe(recon_data, use_container_width=True, hide_index=True,
                             column_config={
                                 "Description": st.column_config.TextColumn(),
                                 "GL Amount":   st.column_config.NumberColumn(format="$%,.2f"),
                                 "Bank Amount": st.column_config.NumberColumn(format="$%,.2f"),
                                 "Matched":     st.column_config.TextColumn(),
                                 "Variance":    st.column_config.NumberColumn(format="$%,.2f"),
                             })
            st.divider()

        # ── Debt Service ───────────────────────────────────────────────────
        if result.debt_service_check and result.debt_service_check.get("loans"):
            st.markdown("### Debt Service Summary")
            debt_data = [{
                "Loan":               loan.get("name", "Unknown"),
                "Principal Balance":  loan.get("principal_balance", 0),
                "Interest Paid YTD":  loan.get("interest_paid_ytd", 0),
            } for loan in result.debt_service_check["loans"]]
            st.dataframe(debt_data, use_container_width=True, hide_index=True,
                         column_config={
                             "Loan":              st.column_config.TextColumn(),
                             "Principal Balance": st.column_config.NumberColumn(format="$%,.2f"),
                             "Interest Paid YTD": st.column_config.NumberColumn(format="$%,.2f"),
                         })
            st.divider()

        # ── Summary Metrics ────────────────────────────────────────────────
        st.markdown("### Summary Metrics")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Files Processed", result.summary.get("files_processed", 0))
        with col2:
            st.metric("GL Accounts", result.summary.get("gl_accounts", 0))
        with col3:
            st.metric("GL Transactions", result.summary.get("gl_transactions", 0))
        with col4:
            st.metric("GL Balanced", "Yes" if result.summary.get("gl_balanced") else "No")
        with col5:
            st.metric("Exceptions",
                      f"{result.summary.get('exceptions_error', 0)}E / "
                      f"{result.summary.get('exceptions_warning', 0)}W")
        st.divider()

        # ── Parser Status ──────────────────────────────────────────────────
        st.markdown("### Parser Status")
        parser_data = [{"Parser": k.replace("_", " ").title(), "Status": "✅ Success"}
                       for k in result.parsed.keys()]
        if parser_data:
            st.dataframe(parser_data, use_container_width=True, hide_index=True,
                         column_config={
                             "Parser": st.column_config.TextColumn(),
                             "Status": st.column_config.TextColumn(),
                         })
        st.divider()

        # ── Exceptions ─────────────────────────────────────────────────────
        if result.exceptions:
            st.markdown("### Exceptions & Findings")
            for exc in result.exceptions:
                severity_badge = {
                    "error":   "🔴 ERROR",
                    "warning": "🟡 WARNING",
                    "info":    "🔵 INFO",
                }.get(exc.severity, "ℹ️ INFO")
                with st.expander(f"{severity_badge} — {exc.description}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Category:** {exc.category}")
                        st.write(f"**Source:** {exc.source}")
                    with col2:
                        if exc.details:
                            st.write("**Details:**")
                            for key, val in exc.details.items():
                                st.write(f"- {key}: {val}")
        else:
            st.success("No exceptions found! Pipeline validation passed.", icon="✅")

        st.divider()

        # ── Post-Close Adjustments ─────────────────────────────────────────
        # After reviewing the QC workbook the user may identify JEs that still
        # need to be posted (e.g. a correction entry, a missed re-accrual, a
        # reclassification).  Enter them here and download as a Yardi-import CSV.
        # Each JE uses two lines (one DR, one CR).  Debits must equal Credits per JE#.
        st.markdown("### Post-Close Adjustments")
        st.caption(
            "Enter each JE as a Debit line and a Credit line. "
            "Use **Add JE Lines** to append a new pre-numbered pair. "
            "Debits must equal Credits for each **JE #** before export."
        )

        # ── Add JE Lines button ────────────────────────────────────────────
        if st.button("➕ Add JE Lines", key="pcje_add_btn"):
            import pandas as _pd_pcje_add
            _existing_pcje = st.session_state.post_close_je_df
            _used_jes = (
                _existing_pcje["JE #"]
                .str.strip()
                .replace("", None)
                .dropna()
                .unique()
                .tolist()
            )
            _next_n   = len(_used_jes) + 1
            _next_lbl = f"PC-{_next_n:03d}"
            _new_pair = _pd_pcje_add.DataFrame({
                "JE #":           [_next_lbl, _next_lbl],
                "Description":    ["", ""],
                "Account Code":   ["", ""],
                "Debit ($)":      [0.0, 0.0],
                "Credit ($)":     [0.0, 0.0],
                "Line Description": ["", ""],
            })
            st.session_state.post_close_je_df = _pd_pcje_add.concat(
                [_existing_pcje, _new_pair], ignore_index=True
            )
            st.rerun()

        _pcje_edited = st.data_editor(
            st.session_state.post_close_je_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "JE #":             st.column_config.TextColumn("JE #", width="small"),
                "Description":      st.column_config.TextColumn("JE Description", width="medium"),
                "Account Code":     st.column_config.TextColumn("Account Code", width="small"),
                "Debit ($)":        st.column_config.NumberColumn("Debit ($)", format="$%,.2f", width="small"),
                "Credit ($)":       st.column_config.NumberColumn("Credit ($)", format="$%,.2f", width="small"),
                "Line Description": st.column_config.TextColumn("Line Description", width="large"),
            },
            key="post_close_je_editor",
        )
        st.session_state.post_close_je_df = _pcje_edited

        _pcje_valid = _pcje_edited[
            _pcje_edited["Account Code"].fillna("").str.strip().astype(bool) &
            ((_pcje_edited["Debit ($)"] != 0) | (_pcje_edited["Credit ($)"] != 0))
        ]

        if not _pcje_valid.empty:
            # ── Validation: each JE # debits must equal credits ──
            _pcje_errors = []
            for _jn, _grp in _pcje_valid.groupby("JE #"):
                _total_dr = _grp["Debit ($)"].sum()
                _total_cr = _grp["Credit ($)"].sum()
                if abs(_total_dr - _total_cr) > 0.01:
                    _pcje_errors.append(
                        f"JE #{_jn}: Debits ${_total_dr:,.2f} ≠ Credits ${_total_cr:,.2f}"
                    )

            if _pcje_errors:
                for _err in _pcje_errors:
                    st.error(f"❌ {_err}", icon="❌")
            else:
                st.success(
                    f"✅ {len(_pcje_valid)} line{'s' if len(_pcje_valid) != 1 else ''} "
                    f"across {_pcje_valid['JE #'].nunique()} JE(s) — balanced",
                    icon="✅",
                )

                # ── Build JE lines for CSV export ──
                _pcje_lines = []
                _pcje_num   = 1
                for _jn, _grp in _pcje_valid.groupby("JE #"):
                    _line_seq = 1
                    for _, _row in _grp.iterrows():
                        _dr = float(_row["Debit ($)"] or 0)
                        _cr = float(_row["Credit ($)"] or 0)
                        if _dr > 0 or _cr > 0:
                            _pcje_lines.append({
                                'je_number':      f'PCJ-{str(_jn).strip() or _pcje_num:04}',
                                'line':           _line_seq,
                                'date':           '',
                                'account_code':   str(_row["Account Code"]).strip(),
                                'account_name':   str(_row["Line Description"] or _row["Account Code"]).strip(),
                                'description':    str(_row["Description"] or "Post-close adjustment").strip(),
                                'reference':      'POST-CLOSE',
                                'debit':          round(_dr, 2),
                                'credit':         round(_cr, 2),
                                'vendor':         '[Post-Close Adj]',
                                'invoice_number': '',
                                'source':         'post_close',
                                'confidence':     'high',
                            })
                            _line_seq += 1
                    _pcje_num += 1

                # ── Generate CSV ──
                _pcje_csv_path = os.path.join(
                    st.session_state.temp_dir, "GA_PostClose_JE.csv"
                )
                try:
                    from accrual_entry_generator import generate_yardi_je_csv
                    generate_yardi_je_csv(
                        _pcje_lines,
                        _pcje_csv_path,
                        period=result.period,
                        property_code=result.property_name or 'revlabpm',
                        je_number='PCJ-001',
                    )
                    st.session_state.pass2_output_files["post_close_je_csv"] = _pcje_csv_path
                except Exception as _pcje_err:
                    st.warning(f"Post-close JE CSV generation skipped: {_pcje_err}")

                # ── Download button ──
                _pcje_path = st.session_state.pass2_output_files.get("post_close_je_csv")
                if _pcje_path and os.path.exists(_pcje_path):
                    with open(_pcje_path, "rb") as _f:
                        st.download_button(
                            label="⬇️ Download Post-Close JE CSV (Yardi import)",
                            data=_f.read(),
                            file_name=f"GA_PostClose_JE_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            use_container_width=True,
                        )

        st.divider()

        # ── Download Section ───────────────────────────────────────────────
        st.markdown("### Download Reports")

        import zipfile, io
        period_label = (result.period or 'Period').replace('-', '_')

        p2_zip_files = {
            f"RevLabs_{period_label}_Workpapers.xlsx":      p2.get("bs_workpaper"),
            f"RevLabs_{period_label}_QC_Workbook.xlsx":     p2.get("qc_workbook"),
            f"RevLabs_{period_label}_Exceptions.xlsx":      p2.get("exception_report"),
            f"RevLabs_{period_label}_BC_Internal.xlsx":     p2.get("annotated_bc"),
        }
        p2_zip_files = {k: v for k, v in p2_zip_files.items() if v and os.path.exists(v)}

        if p2_zip_files:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for fname, fpath in p2_zip_files.items():
                    zf.write(fpath, fname)
            zip_buf.seek(0)
            st.download_button(
                label=f"📦 Download Full Report Package ({len(p2_zip_files)} files)",
                data=zip_buf,
                file_name=f"RevLabs_{period_label}_Reports_{datetime.now().strftime('%Y%m%d')}.zip",
                mime="application/zip",
                use_container_width=True,
                help="Workpapers, QC Workbook, Exception Report, Annotated BC",
            )

        st.divider()
        st.markdown("##### Individual Downloads")
        col1, col2 = st.columns(2)

        _dl_items = [
            ("bs_workpaper",    "📋 Workpapers",
             f"GA_Workpapers_{datetime.now().strftime('%Y%m%d')}.xlsx"),
            ("qc_workbook",     "✅ QC Workbook",
             f"GA_QC_Workbook_{datetime.now().strftime('%Y%m%d')}.xlsx"),
            ("exception_report","⚠️ Exception Report",
             f"GA_Exceptions_{datetime.now().strftime('%Y%m%d')}.xlsx"),
            ("annotated_bc",    "💬 Budget Comparison (Internal)",
             f"GA_BC_Internal_{datetime.now().strftime('%Y%m%d')}.xlsx"),
        ]

        for i, (key, label, fname) in enumerate(_dl_items):
            fpath = p2.get(key)
            target_col = col1 if i % 2 == 0 else col2
            if fpath and os.path.exists(fpath):
                with target_col:
                    with open(fpath, "rb") as f:
                        st.download_button(
                            label=label,
                            data=f.read(),
                            file_name=fname,
                            use_container_width=True,
                        )


# ──────────────────────────────────────────────────────────────
# TAB 3 — HOW TO USE
# ──────────────────────────────────────────────────────────────
with tab3:
    st.markdown("## 📖 Pipeline User Guide")
    st.markdown(
        "This guide walks through every step of the monthly close — what files to pull from "
        "Yardi, what the pipeline produces, what gets posted to Yardi, and what the final "
        "deliverables look like. No prior knowledge of the pipeline required."
    )

    # ── Quick-reference flow ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### At a Glance")
    st.markdown("""
| Step | Who | Action |
|------|-----|--------|
| 1 | Property Accountant | Export pre-close files from Yardi & pull bank statements |
| 2 | Property Accountant | Upload all Pass 1 files → click **Generate JEs** |
| 3 | Property Accountant | Review outputs, download 2 JE CSVs + Prepaid Ledger |
| 4 | Property Accountant | Import both CSVs into Yardi → run the final close |
| 5 | Property Accountant | Re-export final GL, TB, BC, Bank Rec, Loan Statements from Yardi |
| 6 | Property Accountant | Upload Pass 2 files → click **Generate Reports** |
| 7 | Property Accountant | Review all outputs before sending to Accounting Manager |
| 8 | Accounting Manager | Reviews Workpapers, QC Workbook, and Annotated BC |
""")

    # ── PASS 1 ────────────────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("📥  Step 1 — Pass 1: What to Upload", expanded=True):
        st.markdown(
            "Upload all files into the **Pass 1 upload zone** at the top of the Pass 1 tab. "
            "The pipeline auto-detects each file type — if it guesses wrong, use the "
            "dropdown next to the filename to correct it."
        )
        st.markdown("#### Core Close Files — export from Yardi before running the close")
        st.markdown("""
| File | Where to get it in Yardi | Notes |
|------|--------------------------|-------|
| **Yardi GL Detail** | Reports → General Ledger → by Property, Period = current month, Book = Accrual | Most important file — drives all accrual logic |
| **Yardi Trial Balance** | Reports → Trial Balance → same period & book | Used for GL ↔ TB tie-out |
| **Yardi Budget Comparison** | Reports → Budget Comparison → PTD + YTD columns, same period | Drives budget gap detection |
| **12-Month Income Statement** | Reports → 12-Month Statement → trailing 12 months | Used for historical recurring accruals |
| **Nexus Invoice Detail** | Nexus AP → Export open invoices → .xls format | Open invoices not yet in the GL |
| **Kardin Budget** | Kardin → Export → qryExportData format | Annual budget; used for payroll bonus accruals |
""")
        st.markdown("#### Bank Statements")
        st.markdown("""
| File | Where to get it | Notes |
|------|-----------------|-------|
| **Yardi / PNC Bank Rec** | Yardi Reports → Bank Reconciliation → export as PDF | Preferred source — pipeline reads the pre-computed reconciled balance |
| **KeyBank DACA Statement** | KeyBank online → account x5132 → monthly statement PDF | Used as management fee cash-received basis |
| **BofA Development Statement** | BofA online → development account → monthly PDF | Balance only; development account is dormant |
| **Berkadia Loan Statement(s)** | Berkadia portal → monthly loan statements → PDF (all 3 loans) | Debt service detail + escrow balances |
""")
        st.markdown("#### Reference Files")
        st.markdown("""
| File | Where to get it | Notes |
|------|-----------------|-------|
| **Yardi Receivable Detail** | Reports → Receivable Detail → current period | Used to calculate management fee on cash received |
| **Yardi AR Detail Aging** | Reports → AR Aging Detail → current period | Used alongside Receivable Detail to exclude prepayments |
| **Prior Month Prepaid Ledger** | Downloaded from last month's Pass 1 run | First month (January): use the seed file `GA_Prepaid_Ledger_Seed_Dec2025.xlsx` |
""")
        st.markdown(
            "> **Tip:** You don't need every file every month. The pipeline runs on whatever is "
            "uploaded and flags anything it couldn't calculate. The GL is the only required file."
        )

    # ── One-Off Accruals ──────────────────────────────────────────────────────
    with st.expander("✏️  Step 1b — Fill in the One-Off Accruals Table"):
        st.markdown("""
The **One-Off Accruals** table (Pass 1 tab) is for items the pipeline can't detect automatically —
typically small recurring contracts where no invoice arrives until after close.

The table comes **pre-populated** with common monthly items. Review and adjust amounts each month:

| Pre-seeded item | Account | Typical amount |
|-----------------|---------|---------------|
| Tenant Relations (misc.) | 637150 | ~$1,700 |
| HVAC Quarterly Maintenance | 617110 | ~$8,375 (Q months) |
| PPM Pit Maintenance | 619120 | ~$3,400 |
| Fire Life Safety | 627230 | ~$1,000 |
| Snow & Ice Removal | 635110 | Seasonal — update from quote |
| Durkin Supply | 610140 | ~$300 |
| Casella Extra Pickup | 610160 | ~$670 |
| BlueTriton Water Delivery | 637230 | ~$200 |
| Water/Sewer (if no Nexus) | 613310 | Budget amount |

Each row creates a **DR expense / CR 213100 Accrued Expenses** journal entry that auto-reverses next period.
""")

    # ── Pass 1 Outputs ────────────────────────────────────────────────────────
    with st.expander("📄  Step 2 — What Pass 1 Produces"):
        st.markdown("""
After clicking **Generate JEs**, two files are available to download:

| File | Contents | What to do with it |
|------|----------|--------------------|
| **GA_Accruals_JE.csv** | All accrual entries: invoice proration, budget gaps, historical recurring, management fee, contract supplements, payroll bonus accruals, tenant utility billings | **Import into Yardi** as a journal batch |
| **GA_Prepaid_Ledger.xlsx** | Updated prepaid amortization schedule with this month's releases applied | **Save** — upload as the prior-month ledger next month |

> The pipeline also shows a **summary table** of every entry generated, grouped by layer
> (Layer 1 Nexus, Layer 2 Proration, Layer 3 Budget Gap, etc.) so you can review before posting.
""")

    # ── Yardi Upload Step ─────────────────────────────────────────────────────
    with st.expander("⬆️  Step 3 — Post to Yardi & Run the Close"):
        st.markdown("""
**In Yardi, before running the final close:**

1. Go to **Journals → Import Journal Entries**
2. Import `GA_Accruals_JE.csv` → review the batch → post
3. Verify the journal batch posted cleanly (no errors)
4. Run the **month-end close** in Yardi (locks the period)

**What Yardi auto-reverses on the 1st of next month:**
All accrual and management fee entries auto-reverse on the first day of the following period.
This is standard accrual accounting — no manual reversal needed.

> **Note:** The Singerman 8-tab monthly report (Balance Sheet, Income Statement, T12,
> Trial Balance MTD/YTD, GL MTD/YTD, Tenancy) is downloaded directly from Yardi
> after the close — it is **not** generated by this pipeline.
""")

    # ── Pass 2 ────────────────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("📥  Step 4 — Pass 2: What to Re-Upload", expanded=True):
        st.markdown(
            "After the close runs in Yardi, re-export the **final versions** of these files. "
            "They reflect all journal entries that were posted (including the ones from Pass 1). "
            "Upload them in the **Pass 2 upload zone** at the top of the Pass 2 tab — they override the "
            "Pass 1 versions for the final reports."
        )
        st.markdown("""
| File | Why it needs to be re-exported |
|------|-------------------------------|
| **Yardi GL Detail** | Must include all accrual JEs that were posted — the pre-close GL is missing them |
| **Yardi Trial Balance** | Final balances after all JEs posted — used for GL ↔ TB tie-out |
| **Yardi Budget Comparison** | Actuals update after JEs post — needed for accurate variance commentary |
| **Yardi Bank Rec PDF** | Final reconciliation with outstanding checks as of close |
| **Berkadia Loan Statements** | Same file as Pass 1 — re-upload or the pipeline reuses the Pass 1 version |

> All other Pass 1 files (Nexus, Kardin, bank statements, T12, etc.) do **not** need to be
> re-exported — the pipeline reuses them automatically from Pass 1.
""")

    # ── Pass 2 Outputs ────────────────────────────────────────────────────────
    with st.expander("📊  Step 5 — What Pass 2 Produces"):
        st.markdown("""
After clicking **Generate Reports**, four files are available to download:

| File | Contents | Audience |
|------|----------|----------|
| **GA_Workpapers.xlsx** | GL ↔ TB tie-out for all balance sheet accounts, bank rec detail, debt service schedule. Grows month-over-month when the prior month file is uploaded. | Property Accountant / Accounting Manager |
| **GA_QC_Workbook.xlsx** | 7-point QC checklist: TB↔BC tie, budget variances, workpaper tie, MoM swings, BS tie-out, accrual coverage, misc checks | Property Accountant |
| **GA_Exceptions_Report.xlsx** | All flagged issues with severity (Error / Warning / Info), source, and recommended action | Property Accountant |
| **GA_BC_Internal.xlsx** | Annotated Budget Comparison with variance commentary in columns L/M — GRP internal use only | Property Accountant / Accounting Manager |

> **Before sending to Accounting Manager:** The Property Accountant should review all four files and clear any
> open Errors in the Exception Report. Warnings should be reviewed but may be acceptable.
""")

    # ── Post-Close Adjustments ────────────────────────────────────────────────
    with st.expander("🔧  Post-Close Adjustments (Pass 2 JEs)"):
        st.markdown("""
If the review uncovers items that need a correcting entry **after** the close (reclasses,
true-ups, corrections), use the **Post-Close Adjustments** table in the Pass 2 tab.

- Click **➕ Add JE Lines** to add a balanced pair of DR / CR rows
- Each JE pair is auto-numbered (PC-001, PC-002, …)
- Debits must equal Credits per JE# — the pipeline validates before export
- Download `GA_PostClose_JE.csv` and import into Yardi as a separate journal batch

Post-close JEs are **not** auto-reversing — they are permanent adjustments.
""")

    # ── Final Deliverables ────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("📬  Final Deliverables — What Goes Where"):
        st.markdown("""
#### To Accounting Manager
| Item | Source |
|------|--------|
| Workpapers (GL ↔ TB tie-out) | `GA_Workpapers.xlsx` from Pass 2 |
| Annotated Budget Comparison | `GA_BC_Internal.xlsx` from Pass 2 |
| Singerman 8-Tab Monthly Report | Downloaded directly from Yardi |

#### To Singerman (Capital Partner)
| Item | Source |
|------|--------|
| Monthly Report (BS, IS, T12, TB, GL, Tenancy) | Downloaded directly from Yardi — not from this pipeline |

#### Retained Internally (GRP)
| Item | Purpose |
|------|---------|
| `GA_QC_Workbook.xlsx` | GRP internal QC sign-off |
| `GA_Exceptions_Report.xlsx` | Audit trail of all flagged items |
| `GA_Prepaid_Ledger.xlsx` | Carry forward — upload next month as the prior-month ledger |
""")

    # ── Troubleshooting ───────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("🛠️  Common Issues & Tips"):
        st.markdown("""
**File uploaded but not recognized**
→ Use the dropdown next to the filename in the Pass 1 upload zone to manually select the file type.

**Management fee shows $0**
→ The pipeline couldn't find cash received. Check that the DACA statement or Receivable Detail
was uploaded. If both are missing, the fee will be $0 and will need a manual One-Off entry.

**Workpaper doesn't include prior months**
→ Upload the prior month's `GA_Workpapers.xlsx` in the Pass 2 upload zone. The pipeline appends
the new period's sheets to the existing file. Leave blank for January (first run of the year).

**RE Tax shows $0 in January**
→ January is a payment month (Berkadia pays from escrow). Enter the quarterly tax bill amount
in the RE Tax expander in the Pass 1 tab — the pipeline generates the DR 641110 / CR 115200 entry.

**Accrual entry says "REVIEW REQUIRED"**
→ This is a low-confidence entry — the account has a budget but no GL history this year.
Review whether the expense was actually incurred before posting. Delete the row in the
summary table if it should not be posted.

**Reset button**
→ Use **Reset All** (sidebar) to clear all uploads and start fresh. Use **Reset Pass 2**
(Pass 2 tab) to clear only the final-close files without losing Pass 1 results.
""")

    st.markdown("---")
    st.caption("Pipeline built by GRP · Version: May 2026")
