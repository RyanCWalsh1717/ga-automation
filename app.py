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
from workpaper_generator import generate_workpapers
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
    initial_sidebar_state="expanded",
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

# Pass 2 — Report Generation
if "pass2_complete" not in st.session_state:
    st.session_state.pass2_complete = False
if "pass2_engine_result" not in st.session_state:
    st.session_state.pass2_engine_result = None
if "pass2_output_files" not in st.session_state:
    st.session_state.pass2_output_files = {}

# DataEditor tables (persist across reruns)
if "tenant_billing_df" not in st.session_state:
    import pandas as pd
    st.session_state.tenant_billing_df = pd.DataFrame({
        "Tenant":        ["Accent Therapeutics", "Keros Therapeutics (N)",
                          "Keros Therapeutics (S)", "Orum Therapeutics",
                          "Santi Therapeutics"],
        "Electric ($)":  [0.0, 0.0, 0.0, 0.0, 0.0],
        "Gas ($)":       [0.0, 0.0, 0.0, 0.0, 0.0],
    })

if "manual_je_df" not in st.session_state:
    import pandas as pd
    st.session_state.manual_je_df = pd.DataFrame({
        "JE #":             ["", ""],
        "Description":      ["", ""],
        "Account Code":     ["", ""],
        "Amount":           [0.0, 0.0],
        "Line Description": ["", ""],
    })

if "manual_accruals_df" not in st.session_state:
    import pandas as pd
    st.session_state.manual_accruals_df = pd.DataFrame({
        "Account Code": ["613310", "637150", "637150", "617110", "619120",
                         "627230", "635110", "610140", "610160", "637230", ""],
        "Account Name": ["Utilities-Water/Sewer", "Admin-Tenant Relations",
                         "Admin-Tenant Relations", "HVAC Maint-Contract Svc",
                         "Water Contract Svc", "Fire Life Safety",
                         "Snow & Ice Removal", "Cleaning Mat/Supplies",
                         "Cleaning-Trash Removal (extra)", "Admin-Materials/Supplies", ""],
        "Vendor":       ["Town of Lexington", "Transaction Associates",
                         "Jones Lang Lasalle", "HAVAC (quarterly)",
                         "PPM", "J&M Brown (quarterly)",
                         "Outdoor Pride", "Durkin",
                         "Casella (extra lines)", "BlueTriton/Primo", ""],
        "Amount ($)":   [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "Description":  ["Semi-annual ÷ 6 (enter invoice ÷ 6)",
                         "Monthly retainer", "Monthly brokerage fee",
                         "Quarterly HVAC contract", "Monthly water treatment",
                         "Quarterly fire/life safety PM", "Pending storm invoices",
                         "Monthly cleaning supplies", "Service lines not yet in Yardi",
                         "Water delivery", ""],
        "Auto-Reverse": [True, True, True, True, True, True, True, True, True, True, True],
    })


# ── Header ───────────────────────────────────────────────────
st.markdown("<h1 class='main-header'>Greatland Realty Partners</h1>", unsafe_allow_html=True)
st.markdown("### GA Automation — Monthly Close Pipeline")
st.markdown("**Revolution Labs — 1050 Waltham St, Lexington, MA**")
st.divider()


# ── Sidebar: File uploads ────────────────────────────────────
st.sidebar.markdown("## File Uploads")

FILE_GROUPS = {
    "core": "📄 Core Close Files",
    "bank": "🏦 Bank Statements",
    "ref":  "📎 Reference",
}

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
    "nexus_accrual": (
        "Nexus Accrual Detail (.xls)", "xls", False, "core",
        "Enables AP accrual detection (Layer 1 — open invoices not yet posted to GL). "
        "Without it: invoice-proration (Layer 2), budget gap (Layer 3), and historical (Layer 4) still run.",
    ),
    "nexus_paid": (
        "Nexus Invoice Detail — Paid (.xls/.xlsx)", ["xls", "xlsx"], False, "core",
        "Paid invoices export from Nexus. Pipeline auto-detects multi-period (prepaid) invoices "
        "by scanning the line description for service-period date ranges. Detected prepaids are "
        "added to the Prepaid Ledger and amortized month-by-month. "
        "Without it: no new prepaids detected from paid invoices this month.",
    ),
    # ── Bank ──────────────────────────────────────────────────
    "bank_rec": (
        "Yardi Bank Rec PDF — Operating (.pdf)", "pdf", False, "bank",
        "PREFERRED bank source. Reads Yardi's pre-computed reconciliation: bank balance, "
        "outstanding checks, reconciled balance, and $0 difference. Enables Operating bank "
        "rec tab in the BS workpaper (PNC x3993 vs GL 111100). Without it: no bank rec tab.",
    ),
    "daca_bank": (
        "DACA Bank Statement — KeyBank x5132 (.pdf)", "pdf", False, "bank",
        "Used as the management fee cash-received basis (matches JLL's methodology). "
        "Also enables DACA bank rec tab in the BS workpaper (KeyBank x5132 vs GL 115100). "
        "Without it: management fee falls back to GL 111100 debits as the cash basis.",
    ),
    "pnc_bank": (
        "PNC Bank Statement — raw (.pdf)", "pdf", False, "bank",
        "Used for transaction-level GL↔bank matching when Yardi Bank Rec PDF is not uploaded. "
        "If Yardi Bank Rec PDF is uploaded, that takes priority and this file is not used for reconciliation.",
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

def _save_upload(uf, key: str):
    """Save uploaded file to temp dir. Skips re-write if file already correct size."""
    temp_file = os.path.join(st.session_state.temp_dir, uf.name)
    if not os.path.exists(temp_file) or os.path.getsize(temp_file) != uf.size:
        with open(temp_file, "wb") as f:
            f.write(uf.getbuffer())
    st.session_state.uploaded_files[key] = temp_file

# Render each file group
for group_key, group_label in FILE_GROUPS.items():
    st.sidebar.markdown(f"**{group_label}**")
    for key, (label, file_type, required, grp, help_text) in FILE_CONFIG.items():
        if grp != group_key:
            continue
        col1, col2 = st.sidebar.columns([5, 1])
        with col1:
            if key == "loan":
                multi = st.file_uploader(
                    label, type="pdf", accept_multiple_files=True,
                    key="uploader_loan", help=help_text,
                )
                if multi:
                    paths = []
                    for uf in multi:
                        temp_file = os.path.join(st.session_state.temp_dir, uf.name)
                        if not os.path.exists(temp_file) or os.path.getsize(temp_file) != uf.size:
                            with open(temp_file, "wb") as f:
                                f.write(uf.getbuffer())
                        paths.append(temp_file)
                    st.session_state.uploaded_files["loan"] = paths
            else:
                uf = st.file_uploader(
                    label, type=file_type,
                    key=f"uploader_{key}", help=help_text,
                )
                if uf is not None:
                    _save_upload(uf, key)
        with col2:
            if key in st.session_state.uploaded_files:
                st.markdown("✅")
    st.sidebar.markdown("")

# ── Missing-output warnings ───────────────────────────────────
uploaded_keys = set(st.session_state.uploaded_files.keys())
missing_impact = []
if "trial_balance"     not in uploaded_keys: missing_impact.append("No BS tie-out validation (Pass 2)")
if "budget_comparison" not in uploaded_keys: missing_impact.append("No budget gap accruals or variance comments")
if "bank_rec"          not in uploaded_keys: missing_impact.append("No Operating bank rec tab (Pass 2)")
if "daca_bank"         not in uploaded_keys: missing_impact.append("No DACA bank rec tab (Pass 2)")
if "loan"              not in uploaded_keys: missing_impact.append("No debt service tab (Pass 2)")

uploaded_count = len(uploaded_keys)
gl_uploaded = "gl" in uploaded_keys

if missing_impact:
    with st.sidebar.expander(f"⚠️ {len(missing_impact)} output(s) won't generate", expanded=False):
        for m in missing_impact:
            st.caption(f"• {m}")

st.sidebar.markdown(f"**{uploaded_count} file(s) uploaded**")
st.sidebar.divider()

if not gl_uploaded:
    st.sidebar.warning("⚠️ GL Detail is required to run either pass.")

# ── Bank Rec Settings (Pass 1 + Pass 2) ──────────────────────────────────────
st.sidebar.markdown("## Bank Rec")
prior_period_outstanding = st.sidebar.number_input(
    "Prior Period Outstanding Checks ($)",
    min_value=0.0,
    value=0.0,
    step=100.0,
    format="%.2f",
    help=(
        "Only needed when using the raw PNC statement (no Yardi Bank Rec PDF). "
        "Enter the total of checks outstanding from prior periods that aren't in the "
        "current GL export. Leave $0 when uploading the Yardi Bank Rec PDF — "
        "Yardi's pre-computed values are used directly in that case."
    ),
)
prior_period_outstanding = prior_period_outstanding if prior_period_outstanding > 0 else 0.0
st.sidebar.divider()

# ── QC Settings (Pass 1: management fee basis) ────────────────────────────────
st.sidebar.markdown("## Management Fee")
cash_received_override = st.sidebar.number_input(
    "Cash Received Override ($)",
    min_value=0.0,
    value=0.0,
    step=1000.0,
    format="%.2f",
    help=(
        "Override the auto-detected cash received used for the management fee calculation. "
        "Auto-detect priority: DACA additions → GL 111100 debits → revenue proxy. "
        "Only enter a value here if you know the correct amount and the auto-detection is wrong."
    ),
)
cash_received_override = cash_received_override if cash_received_override > 0 else None
st.sidebar.divider()

# ── Tenant Utility Billing (Pass 1 only) ────────────────────────────────────
st.sidebar.markdown("## Tenant Utility Billing")
st.sidebar.caption(
    "Enter electric and gas amounts per tenant from the monthly meter read. "
    "Posts as: DR Accounts Receivable / CR 440500 Tenant Electric and CR 440700 Tenant Gas. "
    "Leave all $0 to skip — pipeline will auto-accrue the budget amount if meter reads aren't in the GL yet."
)

_tenant_billing_edited = st.sidebar.data_editor(
    st.session_state.tenant_billing_df,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "Tenant":       st.column_config.TextColumn("Tenant", width="medium"),
        "Electric ($)": st.column_config.NumberColumn("Electric ($)", format="$%.2f", width="small"),
        "Gas ($)":      st.column_config.NumberColumn("Gas ($)",      format="$%.2f", width="small"),
    },
    key="tenant_billing_editor",
)
st.session_state.tenant_billing_df = _tenant_billing_edited

_tenant_utility_rows = []
_tu_elec_total = 0.0
_tu_gas_total  = 0.0
for _, _trow in _tenant_billing_edited.iterrows():
    _tname = str(_trow.get("Tenant", "") or "").strip()
    _telec = float(_trow.get("Electric ($)", 0) or 0)
    _tgas  = float(_trow.get("Gas ($)", 0) or 0)
    if _tname and (_telec > 0 or _tgas > 0):
        _tenant_utility_rows.append({'tenant': _tname, 'electric': _telec, 'gas': _tgas})
        _tu_elec_total += _telec
        _tu_gas_total  += _tgas

if _tenant_utility_rows:
    st.sidebar.caption(
        f"✓ {len(_tenant_utility_rows)} tenant(s) — "
        f"Electric ${_tu_elec_total:,.2f} / Gas ${_tu_gas_total:,.2f}"
    )
else:
    st.sidebar.caption("↳ No entries — will auto-accrue budget if meter read not in GL")

st.sidebar.divider()

# ── Bonus Accruals (Pass 1 only) ─────────────────────────────────────────────
st.sidebar.markdown("**Bonus Accruals**")
st.sidebar.caption(
    "Auto-detected from Kardin when budget file is uploaded. "
    "Enter an override only for special bonuses outside the Kardin schedule. "
    "Leave $0 to use Kardin auto-detection."
)

_rm_bonus_amt = st.sidebar.number_input(
    "R&M Bonus Override (615110)",
    min_value=0.0, value=0.0, step=100.0, format="%.2f",
    key="widget_bonus_615110",
    help=(
        "Engineering / maintenance staff bonus. "
        "Posts to 615110 alongside regular payroll. "
        "Leave $0 to auto-detect from Kardin."
    ),
)
_admin_bonus_amt = st.sidebar.number_input(
    "Admin Bonus Override (637110)",
    min_value=0.0, value=0.0, step=100.0, format="%.2f",
    key="widget_bonus_637110",
    help=(
        "Administrative staff bonus. "
        "Posts to 637110 alongside regular payroll. "
        "Leave $0 to auto-detect from Kardin."
    ),
)
_bonus_overrides: dict = {}
if _rm_bonus_amt > 0:
    _bonus_overrides['615110'] = _rm_bonus_amt
if _admin_bonus_amt > 0:
    _bonus_overrides['637110'] = _admin_bonus_amt

st.sidebar.divider()

# ── Reset (sidebar) ───────────────────────────────────────────
if st.sidebar.button("🔄 Reset All", use_container_width=True,
                     help="Clear all results and uploaded files"):
    st.session_state.pass1_complete = False
    st.session_state.pass1_engine_result = None
    st.session_state.pass1_output_files = {}
    st.session_state.pass2_complete = False
    st.session_state.pass2_engine_result = None
    st.session_state.pass2_output_files = {}
    st.session_state.uploaded_files = {}
    shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
    st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")
    import pandas as _pd
    st.session_state.manual_je_df = _pd.DataFrame({
        "JE #": ["", ""], "Description": ["", ""],
        "Account Code": ["", ""], "Amount": [0.0, 0.0], "Line Description": ["", ""],
    })
    if "manual_accruals_df" in st.session_state:
        st.session_state.manual_accruals_df["Amount ($)"] = 0.0
    st.rerun()


# ═══════════════════════════════════════════════════════════════
# ── Main content: Two-pass tabs ──────────────────────────────
# ═══════════════════════════════════════════════════════════════
import pandas as pd

tab1, tab2 = st.tabs([
    "📋 Pass 1 — Generate JEs  (Pre-Close)",
    "📊 Pass 2 — Generate Reports  (Post-Close)",
])


# ──────────────────────────────────────────────────────────────
# TAB 1 — PASS 1: JE GENERATION
# ──────────────────────────────────────────────────────────────
with tab1:
    st.markdown("""
    **What this does:** Reads your pre-close Yardi GL and detects every accrual entry needed
    to complete the month-end close — invoices in Nexus not yet posted, budget gaps,
    historical patterns, management fee, prepaid amortization, one-off items you enter below.
    Exports three Yardi-import CSVs.

    **Next step after this tab:** Upload the CSVs to Yardi → run final close → switch to **Pass 2**.
    """)
    st.divider()

    # ── One-Off Accruals Table ────────────────────────────────────────────────
    with st.expander("🧾 One-Off Accruals  (DR expense → CR 211200 auto)", expanded=False):
        st.caption(
            "Use this for known invoices not yet in Nexus or Yardi — quarterly contracts, "
            "seasonal items, recurring retainers, semi-annual billings, etc. "
            "Enter the **debit side only** — the credit to **211200 Accrued Expenses** is generated automatically. "
            "Leave Amount at $0 to skip a row this month. Add new rows at the bottom for anything not pre-seeded. "
            "These export in the Accruals JE CSV alongside the pipeline's auto-detected entries."
        )
        accruals_edited_df = st.data_editor(
            st.session_state.manual_accruals_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "Account Code":  st.column_config.TextColumn("Account", width="small",
                                     help="6-digit Yardi GL account code (e.g. 613310)"),
                "Account Name":  st.column_config.TextColumn("Account Name", width="medium"),
                "Vendor":        st.column_config.TextColumn("Vendor", width="medium"),
                "Amount ($)":    st.column_config.NumberColumn("Amount ($)", format="$%,.2f",
                                     width="small", min_value=0.0,
                                     help="Positive amount — debit to expense account"),
                "Description":   st.column_config.TextColumn("Description", width="large",
                                     help="Description for the Yardi JE line"),
                "Auto-Reverse":  st.column_config.CheckboxColumn("Rev?", width="small",
                                     help="Check to auto-reverse next period"),
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

    # ── Manual JEs Table ─────────────────────────────────────────────────────
    with st.expander("📝 Manual Journal Entries & Reclasses  (fully balanced)", expanded=False):
        st.caption(
            "Use this for reclasses between accounts, true-up entries, or any adjustment where you control "
            "both the debit and credit sides (i.e., the offset is not 211200 Accrued Expenses). "
            "Positive Amount = Debit, Negative = Credit. Group lines with the same **JE #** — they must net to $0. "
            "Exports as a separate Manual JE CSV (not mixed with accruals)."
        )
        edited_df = st.data_editor(
            st.session_state.manual_je_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "JE #":             st.column_config.TextColumn("JE #", width="small"),
                "Description":      st.column_config.TextColumn("JE Description", width="medium"),
                "Account Code":     st.column_config.TextColumn("Account", width="small"),
                "Amount":           st.column_config.NumberColumn("Amount (+DR / -CR)",
                                        format="$%,.2f", width="small"),
                "Line Description": st.column_config.TextColumn("Line Description", width="large"),
            },
            key="manual_je_editor",
        )
        st.session_state.manual_je_df = edited_df

        valid_rows = edited_df[
            edited_df["Account Code"].fillna("").str.strip().astype(bool) &
            (edited_df["Amount"] != 0)
        ]
        if not valid_rows.empty:
            balance_check = valid_rows.groupby("JE #")["Amount"].sum()
            all_balanced = True
            for je_id, total in balance_check.items():
                if abs(total) > 0.01:
                    st.warning(f"⚠️ {je_id} is out of balance by ${abs(total):,.2f}", icon="⚠️")
                    all_balanced = False
            if all_balanced:
                total_dr = valid_rows[valid_rows["Amount"] > 0]["Amount"].sum()
                st.success(
                    f"✅ All entries balanced — {len(balance_check)} JE(s), ${total_dr:,.2f} total debits",
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

                # Step 2: Detect accrual entries (5-layer)
                status_text.text("Step 2/6: Detecting accrual entries (5 layers)...")
                progress_bar.progress(25)

                # Build manual exclusion list (account codes with entries in the One-Off table)
                # so Layers 2-4 don't double-accrue those accounts.
                _accruals_tbl_early = st.session_state.get("manual_accruals_df")
                _manual_accruals_input = []
                if _accruals_tbl_early is not None and not _accruals_tbl_early.empty:
                    _manual_accruals_input = [
                        {
                            'account_code': str(r["Account Code"]).strip(),
                            'account_name': str(r.get("Account Name", "") or "").strip(),
                            'amount': 0,   # 0 → Layer 0 skips JE; only needed for dedup exclusion
                            'description': '',
                        }
                        for _, r in _accruals_tbl_early.iterrows()
                        if str(r.get("Account Code", "") or "").strip()
                        and float(r.get("Amount ($)", 0) or 0) > 0
                    ]

                je_lines = build_accrual_entries(
                    nexus_data or [],
                    period=close_period,
                    property_name=engine_result.property_name or '',
                    gl_data=gl_parsed,
                    budget_data=bc_parsed,
                    manual_accruals=_manual_accruals_input or [],
                    tenant_utility_rows=_tenant_utility_rows or None,
                    kardin_records=engine_result.parsed.get('kardin_budget') or [],
                    bonus_overrides=_bonus_overrides or None,
                )

                # Step 3: Prepaid ledger — load → merge → release JEs → advance
                status_text.text("Step 3/6: Processing prepaid ledger...")
                progress_bar.progress(45)

                ledger_path = st.session_state.uploaded_files.get("prepaid_ledger")
                ledger_active, ledger_completed = prepaid_ledger.load(ledger_path)

                # Parse paid invoices for new prepaid detection
                nexus_paid_path = st.session_state.uploaded_files.get("nexus_paid")
                nexus_paid_records = []
                if nexus_paid_path:
                    try:
                        from parsers.nexus_paid_invoices import parse as parse_nexus_paid
                        nexus_paid_records = parse_nexus_paid(nexus_paid_path)
                        if nexus_paid_records and '_parse_error' in nexus_paid_records[0]:
                            st.warning(f"Nexus Paid file parse error: {nexus_paid_records[0]['_parse_error']}")
                            nexus_paid_records = []
                    except Exception as _e:
                        st.warning(f"Could not parse Nexus Paid file: {_e}")

                ledger_active, newly_added_paid = prepaid_ledger.merge_nexus(
                    ledger_active, nexus_paid_records, close_period
                )
                ledger_active, newly_added_accrual = prepaid_ledger.merge_nexus(
                    ledger_active, nexus_data or [], close_period
                )
                newly_added = newly_added_paid + newly_added_accrual

                # Build visual amortization schedule
                _all_nexus_for_amort = nexus_paid_records + (nexus_data or [])
                amort_lines = build_prepaid_amortization(_all_nexus_for_amort, close_period=close_period)

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

                fee_result = calculate_mgmt_fee(
                    gl_parsed=gl_parsed,
                    budget_rows=bc_parsed or [],
                    manual_override=cash_received_override,
                    daca_parsed=_daca_parsed,
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

                # Step 5: Manual JEs + One-Off Accrual JEs
                status_text.text("Step 5/6: Building manual entries...")
                progress_bar.progress(75)

                # Manual JEs & Reclasses (fully balanced, user-entered)
                manual_je_lines = []
                _mdf = st.session_state.manual_je_df
                _valid = _mdf[
                    _mdf["Account Code"].str.strip().astype(bool) &
                    (_mdf["Amount"] != 0)
                ] if not _mdf.empty else _mdf
                for _, _row in _valid.iterrows():
                    _amt = float(_row["Amount"])
                    manual_je_lines.append({
                        "je_number":    str(_row["JE #"]).strip(),
                        "account_code": str(_row["Account Code"]).strip(),
                        "description":  str(_row["Description"]).strip(),
                        "reference":    str(_row["JE #"]).strip(),
                        "debit":        _amt if _amt > 0 else 0.0,
                        "credit":       -_amt if _amt < 0 else 0.0,
                        "source":       "manual",
                        "date":         close_period,
                    })

                # One-Off Accruals → DR expense / CR 211200
                _supplement_je_lines = []
                _periodic_supplement_rows = []
                _sup_base = len(je_lines) // 2 + len(prepaid_release_je) // 2 + len(fee_je) // 2

                _accruals_tbl = st.session_state.get("manual_accruals_df")
                if _accruals_tbl is not None and not _accruals_tbl.empty:
                    _active_accruals = _accruals_tbl[
                        _accruals_tbl["Account Code"].fillna("").str.strip().astype(bool) &
                        (_accruals_tbl["Amount ($)"].fillna(0) > 0)
                    ]
                    for _, _row in _active_accruals.iterrows():
                        _periodic_supplement_rows.append({
                            'account_code': str(_row["Account Code"]).strip(),
                            'account_name': str(_row.get("Account Name", "") or "").strip()
                                            or str(_row["Account Code"]).strip(),
                            'amount':       float(_row["Amount ($)"]),
                            'description':  str(_row.get("Description", "") or "").strip()
                                            or f'{_row.get("Vendor","") or ""} — one-off accrual',
                            'vendor':       str(_row.get("Vendor", "") or "").strip(),
                            'auto_reverse': bool(_row.get("Auto-Reverse", True)),
                        })

                for _si, _sup in enumerate(_periodic_supplement_rows):
                    _sje_id  = f'SUP-{_sup_base + _si + 1:04d}'
                    _sup_amt = round(float(_sup['amount']), 2)
                    _sup_desc   = _sup.get('description') or f"{_sup['account_name']} — one-off accrual"
                    _sup_vendor = _sup.get('vendor') or _sup['account_name']
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
                            'account_code': '211200', 'account_name': 'Accrued Expenses',
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
                    + manual_je_lines
                    + _supplement_je_lines
                )

                _accrual_sources = {
                    'nexus', 'budget_gap', 'historical', 'management_fee',
                    'management_fee_catchup', 'invoice_proration', 'prepaid_amortization',
                    'contract_supplement', 'tenant_utility_billing', 'bonus_accrual',
                }
                _prepaid_sources = {'prepaid_ledger'}
                _manual_sources  = {'manual'}

                _accrual_lines = [l for l in all_je_lines if l.get('source') in _accrual_sources]
                _prepaid_lines  = [l for l in all_je_lines if l.get('source') in _prepaid_sources]
                _manual_lines   = [l for l in all_je_lines if l.get('source') in _manual_sources]

                _accrual_csv_path = _prepaid_csv_path = _manual_csv_path = None

                _prop_code = (engine_result.parsed.get('gl') and
                              engine_result.parsed['gl'].metadata.property_code) or 'revlabpm'

                if _accrual_lines:
                    _accrual_csv_path = os.path.join(st.session_state.temp_dir, "GA_Accruals_JE.csv")
                    generate_yardi_je_csv(_accrual_lines, _accrual_csv_path,
                                          period=close_period, property_code=_prop_code)

                if _prepaid_lines:
                    _prepaid_csv_path = os.path.join(st.session_state.temp_dir, "GA_Prepaid_JE.csv")
                    generate_yardi_je_csv(_prepaid_lines, _prepaid_csv_path,
                                          period=close_period, property_code=_prop_code)

                if _manual_lines:
                    _manual_csv_path = os.path.join(st.session_state.temp_dir, "GA_Manual_JE.csv")
                    generate_yardi_je_csv(_manual_lines, _manual_csv_path,
                                          period=close_period, property_code=_prop_code)

                # Persist Pass 1 outputs
                p1 = st.session_state.pass1_output_files
                p1["all_je_lines"]          = all_je_lines
                p1["accrual_je_csv"]        = _accrual_csv_path
                p1["prepaid_je_csv"]        = _prepaid_csv_path
                p1["manual_je_csv"]         = _manual_csv_path
                p1["fee_result"]            = fee_result
                p1["catchup_amount"]        = _catchup_amount
                p1["amort_lines"]           = amort_lines
                p1["ledger_active"]         = ledger_active
                p1["ledger_completed"]      = ledger_completed
                p1["newly_added_prepaids"]  = newly_added
                p1["prepaid_ledger_updated"]= updated_ledger_path

                progress_bar.progress(100)
                status_text.text("✓ JEs ready for Yardi upload!")
                st.session_state.pass1_complete = True
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

        # ── Management Fee ─────────────────────────────────────────────────
        if fee_result and fee_result.cash_received > 0:
            st.markdown("### Management Fee JE")
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

            _catchup_amt = p1.get("catchup_amount")
            if _catchup_amt and _catchup_amt > 0:
                st.warning(
                    f"**Management Fee Catch-up Detected — ${_catchup_amt:,.2f}**\n\n"
                    f"Account 637130 shows a net credit (auto-reversal of prior month accrual with "
                    f"no matching invoice). A catch-up entry **(MGT-002)** has been included in the "
                    f"Accruals CSV. **Verify before posting:** confirm the prior month check is still "
                    f"outstanding in AP (211200) before uploading.",
                )
            st.divider()

        # ── JE Preview Table ───────────────────────────────────────────────
        dr_lines = [l for l in all_je_lines if (l.get('debit') or 0) > 0]
        if dr_lines:
            st.markdown("### Accrual Journal Entries")
            source_labels = {
                'nexus':                  'Nexus AP',
                'invoice_proration':      'Invoice Proration',
                'budget_gap':             'Budget Gap',
                'historical':             'Historical Pattern',
                'prepaid_amortization':   'Prepaid Amort.',
                'prepaid_ledger':         'Prepaid Release',
                'management_fee':         'Management Fee',
                'management_fee_catchup': 'Mgmt Fee Catch-up',
                'bonus_accrual':          'Payroll Bonus',
                'contract_supplement':    'One-Off Accrual',
                'tenant_utility_billing': 'Tenant Utility',
                'manual':                 'Manual JE',
            }
            source_totals = {}
            for l in dr_lines:
                src = l.get('source', 'other')
                source_totals[src] = source_totals.get(src, 0) + (l.get('debit') or 0)

            cols = st.columns(len(source_totals) + 1)
            with cols[0]:
                st.metric("Total JEs", len(set(l.get('je_number', '') for l in dr_lines)))
            for i, (src, total) in enumerate(source_totals.items(), 1):
                with cols[i]:
                    st.metric(source_labels.get(src, src), f"${total:,.2f}")

            accrual_rows = []
            for l in dr_lines:
                accrual_rows.append({
                    "JE #":        l.get('je_number', ''),
                    "Source":      source_labels.get(l.get('source', ''), l.get('source', '')),
                    "GL Account":  l.get('account_code', ''),
                    "Description": (l.get('description') or '')[:80],
                    "Amount":      l.get('debit') or 0,
                })
            st.dataframe(accrual_rows, use_container_width=True, hide_index=True,
                         column_config={
                             "JE #":        st.column_config.TextColumn(width="small"),
                             "Source":      st.column_config.TextColumn(width="small"),
                             "GL Account":  st.column_config.TextColumn(width="small"),
                             "Description": st.column_config.TextColumn(width="large"),
                             "Amount":      st.column_config.NumberColumn(format="$%,.2f"),
                         })
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
        if ledger_active or ledger_completed or newly_added:
            st.markdown("### Prepaid Ledger")
            col_l1, col_l2, col_l3 = st.columns(3)
            with col_l1:
                st.metric("Active Prepaid Items", len(ledger_active))
            with col_l2:
                st.metric("New This Month", len(newly_added))
            with col_l3:
                st.metric("Completed This Month", len(ledger_completed))
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

        # Zip of all 3 CSVs + updated ledger
        import zipfile, io
        period_label = (result.period or 'Period').replace('-', '_')
        p1_zip_files = {
            f"RevLabs_{period_label}_Accruals_JE.csv":      p1.get("accrual_je_csv"),
            f"RevLabs_{period_label}_Prepaid_JE.csv":       p1.get("prepaid_je_csv"),
            f"RevLabs_{period_label}_Manual_JE.csv":        p1.get("manual_je_csv"),
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
                use_container_width=True,
            )

        col_d1, col_d2, col_d3, col_d4 = st.columns(4)
        for col, key, label, fname in [
            (col_d1, "accrual_je_csv",        "📄 Accruals JE",     f"GA_Accruals_JE_{datetime.now().strftime('%Y%m%d')}.csv"),
            (col_d2, "prepaid_je_csv",         "📄 Prepaid JE",      f"GA_Prepaid_JE_{datetime.now().strftime('%Y%m%d')}.csv"),
            (col_d3, "manual_je_csv",          "📄 Manual JE",       f"GA_Manual_JE_{datetime.now().strftime('%Y%m%d')}.csv"),
            (col_d4, "prepaid_ledger_updated", "📊 Prepaid Ledger",  f"GA_Prepaid_Ledger_{datetime.now().strftime('%Y%m%d')}.xlsx"),
        ]:
            fpath = p1.get(key)
            if fpath and os.path.exists(fpath):
                with col:
                    with open(fpath, "rb") as f:
                        st.download_button(
                            label=label,
                            data=f.read(),
                            file_name=fname,
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

    # ── Pass 2: Final GL upload ───────────────────────────────────────────────
    st.markdown("#### Final GL — Post-Close")
    st.caption(
        "Export a fresh GL from Yardi **after** the Pass 1 JE CSVs have been uploaded and posted. "
        "All other files (bank statements, trial balance, budget comparison, loan statements) "
        "are reused from the sidebar — only the GL needs to be re-exported."
    )

    _p2_gl_col1, _p2_gl_col2 = st.columns([5, 1])
    with _p2_gl_col1:
        _p2_gl_upload = st.file_uploader(
            "Yardi GL Detail — Final Close (.xlsx)",
            type="xlsx",
            key="uploader_gl_pass2",
            help="Post-close GL export from Yardi (after all accrual JEs are posted). "
                 "This replaces the pre-close GL uploaded in the sidebar for Pass 2 only.",
        )
    with _p2_gl_col2:
        if _p2_gl_upload is not None:
            st.markdown("✅")

    if _p2_gl_upload is not None:
        _p2_gl_path = os.path.join(st.session_state.temp_dir, f"gl_pass2_{_p2_gl_upload.name}")
        if not os.path.exists(_p2_gl_path) or os.path.getsize(_p2_gl_path) != _p2_gl_upload.size:
            with open(_p2_gl_path, "wb") as _f:
                _f.write(_p2_gl_upload.getbuffer())
        st.session_state.uploaded_files["gl_pass2"] = _p2_gl_path
        st.caption(f"✓ Final GL ready: **{_p2_gl_upload.name}**")
    else:
        if "gl_pass2" not in st.session_state.uploaded_files:
            st.caption("⬆️ Upload the post-close GL to enable Pass 2")

    # ── Pass 2: Budget Comparison upload ─────────────────────────────────────
    st.markdown("#### Budget Comparison — Final Close")
    st.caption(
        "Re-export the Budget Comparison from Yardi after the close posts. "
        "Once accrual JEs are in Yardi the actuals column updates, which gives "
        "variance comments and the annotated BC the correct final numbers."
    )
    _p2_bc_col1, _p2_bc_col2 = st.columns([5, 1])
    with _p2_bc_col1:
        _p2_bc_upload = st.file_uploader(
            "Yardi Budget Comparison (.xlsx)",
            type="xlsx",
            key="uploader_budget_comparison_pass2",
            help="Post-close Budget Comparison export. Overrides the sidebar Budget Comparison for Pass 2.",
        )
    with _p2_bc_col2:
        if _p2_bc_upload is not None:
            st.markdown("✅")
    if _p2_bc_upload is not None:
        _p2_bc_path = os.path.join(st.session_state.temp_dir, f"bc_pass2_{_p2_bc_upload.name}")
        if not os.path.exists(_p2_bc_path) or os.path.getsize(_p2_bc_path) != _p2_bc_upload.size:
            with open(_p2_bc_path, "wb") as _f:
                _f.write(_p2_bc_upload.getbuffer())
        st.session_state.uploaded_files["budget_comparison_pass2"] = _p2_bc_path
        st.caption(f"✓ Final BC ready: **{_p2_bc_upload.name}**")
    else:
        if "budget_comparison_pass2" not in st.session_state.uploaded_files:
            if "budget_comparison" in st.session_state.uploaded_files:
                st.caption("↳ Using Budget Comparison from sidebar (upload final version above to override)")
            else:
                st.caption("⬆️ Upload final Budget Comparison to enable variance comments on final actuals")

    # ── Pass 2: Trial Balance upload ─────────────────────────────────────────
    st.markdown("#### Trial Balance — Final Close")
    st.caption(
        "Export the Trial Balance from Yardi after the close is complete. "
        "Used for the GL↔TB tie-out in the BS workpaper."
    )
    _p2_tb_col1, _p2_tb_col2 = st.columns([5, 1])
    with _p2_tb_col1:
        _p2_tb_upload = st.file_uploader(
            "Yardi Trial Balance (.xlsx)",
            type="xlsx",
            key="uploader_trial_balance_pass2",
            help="Post-close Trial Balance export. Overrides the sidebar Trial Balance for Pass 2.",
        )
    with _p2_tb_col2:
        if _p2_tb_upload is not None:
            st.markdown("✅")
    if _p2_tb_upload is not None:
        _p2_tb_path = os.path.join(st.session_state.temp_dir, f"tb_pass2_{_p2_tb_upload.name}")
        if not os.path.exists(_p2_tb_path) or os.path.getsize(_p2_tb_path) != _p2_tb_upload.size:
            with open(_p2_tb_path, "wb") as _f:
                _f.write(_p2_tb_upload.getbuffer())
        st.session_state.uploaded_files["trial_balance_pass2"] = _p2_tb_path
        st.caption(f"✓ Final TB ready: **{_p2_tb_upload.name}**")
    else:
        if "trial_balance_pass2" not in st.session_state.uploaded_files:
            if "trial_balance" in st.session_state.uploaded_files:
                st.caption("↳ Using Trial Balance from sidebar (upload final version above to override)")
            else:
                st.caption("⬆️ Upload final Trial Balance to enable BS workpaper tie-out")

    # ── Pass 2: Bank Rec upload ───────────────────────────────────────────────
    st.markdown("#### Yardi Bank Rec — Final Close")
    st.caption(
        "Export the Bank Rec PDF from Yardi after the close posts. "
        "Used for the Operating account reconciliation tab in the workpapers."
    )
    _p2_br_col1, _p2_br_col2 = st.columns([5, 1])
    with _p2_br_col1:
        _p2_br_upload = st.file_uploader(
            "Yardi Bank Rec PDF (.pdf)",
            type="pdf",
            key="uploader_bank_rec_pass2",
            help="Post-close Bank Rec PDF. Overrides the sidebar Bank Rec for Pass 2.",
        )
    with _p2_br_col2:
        if _p2_br_upload is not None:
            st.markdown("✅")
    if _p2_br_upload is not None:
        _p2_br_path = os.path.join(st.session_state.temp_dir, f"br_pass2_{_p2_br_upload.name}")
        if not os.path.exists(_p2_br_path) or os.path.getsize(_p2_br_path) != _p2_br_upload.size:
            with open(_p2_br_path, "wb") as _f:
                _f.write(_p2_br_upload.getbuffer())
        st.session_state.uploaded_files["bank_rec_pass2"] = _p2_br_path
        st.caption(f"✓ Final Bank Rec ready: **{_p2_br_upload.name}**")
    else:
        if "bank_rec_pass2" not in st.session_state.uploaded_files:
            if "bank_rec" in st.session_state.uploaded_files:
                st.caption("↳ Using Bank Rec from sidebar (upload final version above to override)")
            else:
                st.caption("⬆️ Upload final Bank Rec PDF to enable bank reconciliation workpaper")

    # ── Pass 2: Loan Statements upload ───────────────────────────────────────
    st.markdown("#### Berkadia Loan Statements — Final Close")
    st.caption(
        "Upload the final month's loan billing statements. "
        "Used for the debt service workpaper tab."
    )
    _p2_loan_col1, _p2_loan_col2 = st.columns([5, 1])
    with _p2_loan_col1:
        _p2_loan_upload = st.file_uploader(
            "Berkadia Loan Statements (.pdf)",
            type="pdf",
            accept_multiple_files=True,
            key="uploader_loan_pass2",
            help="Post-close loan billing PDFs (up to 3). Overrides the sidebar loan files for Pass 2.",
        )
    with _p2_loan_col2:
        if _p2_loan_upload:
            st.markdown("✅")
    if _p2_loan_upload:
        _p2_loan_paths = []
        for _lf in _p2_loan_upload:
            _lp = os.path.join(st.session_state.temp_dir, f"loan_pass2_{_lf.name}")
            if not os.path.exists(_lp) or os.path.getsize(_lp) != _lf.size:
                with open(_lp, "wb") as _f:
                    _f.write(_lf.getbuffer())
            _p2_loan_paths.append(_lp)
        st.session_state.uploaded_files["loan_pass2"] = _p2_loan_paths
        st.caption(f"✓ {len(_p2_loan_paths)} loan statement(s) ready")
    else:
        if "loan_pass2" not in st.session_state.uploaded_files:
            if "loan" in st.session_state.uploaded_files:
                st.caption("↳ Using loan statements from sidebar (upload final versions above to override)")
            else:
                st.caption("⬆️ Upload loan statements to enable debt service workpaper")

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
            for _k in ("gl_pass2", "budget_comparison_pass2", "trial_balance_pass2", "bank_rec_pass2", "loan_pass2"):
                st.session_state.uploaded_files.pop(_k, None)
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
                if st.session_state.uploaded_files.get("bank_rec_pass2"):
                    files_dict["bank_rec"] = st.session_state.uploaded_files["bank_rec_pass2"]
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
                if "trial_balance" in st.session_state.uploaded_files:
                    try:
                        from parsers.yardi_trial_balance import parse as parse_tb
                        tb_result = parse_tb(st.session_state.uploaded_files["trial_balance"])
                    except Exception as _e:
                        st.warning(f"Could not parse Trial Balance: {_e}")

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

                if tb_result and gl_parsed:
                    try:
                        bs_wp_path = os.path.join(st.session_state.temp_dir, "GA_BS_Workpaper.xlsx")
                        # GL is final — no je_adjustments needed. The GL already reflects
                        # all posted JEs from Pass 1, so the BS workpaper ties clean.
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
                        )
                        st.session_state.pass2_output_files["bs_workpaper"] = bs_wp_path
                    except Exception as _e:
                        st.warning(f"BS Workpaper generation skipped: {_e}")
                else:
                    if not tb_result:
                        st.info("Upload a Trial Balance file to enable the BS Workpaper.", icon="ℹ️")

                # Step 3: Institutional workpapers (bank rec, debt service, rent roll)
                status_text.text("Step 3/6: Generating institutional workpapers...")
                progress_bar.progress(42)
                workpaper_path = os.path.join(st.session_state.temp_dir, "GA_Workpapers.xlsx")
                try:
                    generate_workpapers(engine_result, workpaper_path)
                    st.session_state.pass2_output_files["workpapers"] = workpaper_path
                except Exception as _e:
                    st.warning(f"Workpapers skipped: {_e}")

                # Step 4: Management fee (informational — already in GL)
                status_text.text("Step 4/6: Verifying management fee...")
                progress_bar.progress(58)
                try:
                    fee_result = calculate_mgmt_fee(
                        gl_parsed=gl_parsed,
                        budget_rows=bc_parsed or [],
                        manual_override=cash_received_override,
                        daca_parsed=daca_bank_data,
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
                    )
                    st.session_state.pass2_output_files["qc_report"] = qc_report
                    qc_path = os.path.join(st.session_state.temp_dir, "GA_QC_Workbook.xlsx")
                    generate_qc_workbook(qc_report, qc_path)
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

                        # Annotated BC (GRP internal)
                        _bc_file = st.session_state.uploaded_files.get("budget_comparison")
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
        _gl_111    = float(p2.get("gl_cash_balance") or 0)
        _daca_gl   = float(p2.get("daca_gl_balance") or 0)
        if _bank_rec or _daca_data:
            st.markdown("### Bank Reconciliation Summary")
            _rec_cols = st.columns(2)
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

        # ── Download Section ───────────────────────────────────────────────
        st.markdown("### Download Reports")

        import zipfile, io
        period_label = (result.period or 'Period').replace('-', '_')

        p2_zip_files = {
            f"RevLabs_{period_label}_BS_Workpaper.xlsx":    p2.get("bs_workpaper"),
            f"RevLabs_{period_label}_Workpapers.xlsx":      p2.get("workpapers"),
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
                help="BS Workpaper, Institutional Workpapers, QC Workbook, Exception Report, Annotated BC",
            )

        st.divider()
        st.markdown("##### Individual Downloads")
        col1, col2 = st.columns(2)

        _dl_items = [
            ("bs_workpaper",    "📋 BS Workpaper",
             f"GA_BS_Workpaper_{datetime.now().strftime('%Y%m%d')}.xlsx"),
            ("workpapers",      "📁 Institutional Workpapers",
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
