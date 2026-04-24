"""
GA Automation — Monthly Report Pipeline
========================================
A Streamlit web app for the Greatland Realty Partners CRE accounting pipeline.
Processes Yardi, Nexus, bank, and loan exports to produce monthly reports for Singerman.
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
from report_generator import generate_report, generate_exception_report
from workpaper_generator import generate_workpapers
import traceback
from accrual_entry_generator import (
    build_accrual_entries, generate_yardi_je_import, generate_yardi_je_csv,
    build_prepaid_amortization, write_prepaid_amortization_tab,
    build_prepaid_release_je,
)
import prepaid_ledger
import bs_workpaper_generator
from variance_comments import (
    generate_variance_comments,
    generate_variance_comments_grp,
    write_comments_to_budget_comparison,
)
from qc_engine import run_qc, generate_qc_workbook
from management_fee import calculate as calculate_mgmt_fee, accrued_fee_from_bc


# ── Page configuration ───────────────────────────────────────
st.set_page_config(
    page_title="GA Automation",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS for styling ───────────────────────────────────
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

    .status-clean {
        color: var(--success-color);
        font-weight: bold;
    }

    .status-warnings {
        color: var(--warning-color);
        font-weight: bold;
    }

    .status-errors {
        color: var(--error-color);
        font-weight: bold;
    }

    .exception-error {
        background-color: #ffe6e6;
        border-left: 4px solid var(--error-color);
        padding: 10px;
        margin: 10px 0;
        border-radius: 4px;
    }

    .exception-warning {
        background-color: #fff3cd;
        border-left: 4px solid var(--warning-color);
        padding: 10px;
        margin: 10px 0;
        border-radius: 4px;
    }

    .exception-info {
        background-color: #e7f3ff;
        border-left: 4px solid var(--info-color);
        padding: 10px;
        margin: 10px 0;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state initialization ─────────────────────────────
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

if "processing_complete" not in st.session_state:
    st.session_state.processing_complete = False

if "engine_result" not in st.session_state:
    st.session_state.engine_result = None

if "output_files" not in st.session_state:
    st.session_state.output_files = {}

if "temp_dir" not in st.session_state:
    st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")

if "n_manual_rows" not in st.session_state:
    st.session_state.n_manual_rows = 0


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
        "JE #":             ["OOE-0001", "OOE-0001"],
        "Description":      ["",          ""],
        "Account Code":     ["",          ""],
        "Amount":           [0.0,         0.0],
        "Line Description": ["",          ""],
    })

if "manual_accruals_df" not in st.session_state:
    import pandas as pd
    # Pre-seed with common one-off accruals. Leave Amount at $0 for months
    # where the item doesn't apply. Add rows for anything new.
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
st.markdown("**Revolution Labs — 275 Grove St, Newton, MA**")
st.divider()


# ── Sidebar: File uploads ────────────────────────────────────
st.sidebar.markdown("## File Uploads")

# ── File config ───────────────────────────────────────────────
# key: (label, file_type, required, group, help_text)
# group: "core" | "bank" | "ref"
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

# Preserve file_config alias for downstream code that reads len(file_config)
file_config = FILE_CONFIG

def _save_upload(uf, key: str):
    """Save an uploaded file to temp dir and register in session state.

    Streamlit reruns the full script on every widget interaction, so this
    function is called on every rerun while a file is uploaded.  We skip
    the write when the file already exists at the correct size — avoids
    re-writing multi-MB PDFs/Excel files on every sidebar click.
    """
    temp_file = os.path.join(st.session_state.temp_dir, uf.name)
    if not os.path.exists(temp_file) or os.path.getsize(temp_file) != uf.size:
        with open(temp_file, "wb") as f:
            f.write(uf.getbuffer())
    st.session_state.uploaded_files[key] = temp_file

# Render each group
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

    st.sidebar.markdown("")  # spacing between groups

# ── Missing-output warnings ───────────────────────────────────
uploaded_keys = set(st.session_state.uploaded_files.keys())
missing_impact = []
if "trial_balance"    not in uploaded_keys: missing_impact.append("No BS tie-out validation")
if "budget_comparison" not in uploaded_keys: missing_impact.append("No budget gap accruals or variance comments")
if "bank_rec"         not in uploaded_keys: missing_impact.append("No Operating bank rec tab")
if "daca_bank"        not in uploaded_keys: missing_impact.append("No DACA bank rec tab")
if "loan"             not in uploaded_keys: missing_impact.append("No debt service tab")

uploaded_count = len(uploaded_keys)
gl_uploaded = "gl" in uploaded_keys

if missing_impact:
    with st.sidebar.expander(f"⚠️ {len(missing_impact)} output(s) won't generate", expanded=False):
        for m in missing_impact:
            st.caption(f"• {m}")

st.sidebar.markdown(f"**{uploaded_count} file(s) uploaded**")
st.sidebar.divider()

if not gl_uploaded:
    st.sidebar.warning("⚠️ GL Detail is required to run.")

# ── Bank Rec Settings ────────────────────────────────────────────
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

# ── QC Settings ─────────────────────────────────────────────────
st.sidebar.markdown("## QC Settings")
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

# ── Tenant Utility Billing (Meter Read JE) ──────────────────────────────────
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

# Build per-tenant rows for pipeline
_tenant_utility_rows = []
_tu_elec_total = 0.0
_tu_gas_total  = 0.0
for _, _trow in _tenant_billing_edited.iterrows():
    _tname = str(_trow.get("Tenant", "") or "").strip()
    _telec = float(_trow.get("Electric ($)", 0) or 0)
    _tgas  = float(_trow.get("Gas ($)", 0) or 0)
    if _tname and (_telec > 0 or _tgas > 0):
        _tenant_utility_rows.append({
            'tenant':   _tname,
            'electric': _telec,
            'gas':      _tgas,
        })
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

# ── Bonus Accruals (sidebar — overrides Kardin auto-detection) ────────────────
st.sidebar.divider()
st.sidebar.markdown("**Bonus Accruals**")
st.sidebar.caption(
    "Auto-detected from Kardin when budget file is uploaded. "
    "Enter an override only for special bonuses outside the Kardin schedule. "
    "Leave $0 to use Kardin auto-detection."
)

_rm_bonus_amt = st.sidebar.number_input(
    "R&M Bonus Override (615110)",
    min_value=0.0,
    value=0.0,
    step=100.0,
    format="%.2f",
    key="widget_bonus_615110",
    help=(
        "Engineering / maintenance staff bonus. "
        "Posts to 615110 alongside regular payroll. "
        "Leave $0 to auto-detect from Kardin (Kardin annual / 12 − standard month). "
        "Enter a specific amount only to override the Kardin calculation."
    ),
)

_admin_bonus_amt = st.sidebar.number_input(
    "Admin Bonus Override (637110)",
    min_value=0.0,
    value=0.0,
    step=100.0,
    format="%.2f",
    key="widget_bonus_637110",
    help=(
        "Administrative staff bonus. "
        "Posts to 637110 alongside regular payroll. "
        "Leave $0 to auto-detect from Kardin. "
        "Enter a specific amount only to override the Kardin calculation."
    ),
)

# Build bonus overrides dict — passed to build_accrual_entries
_bonus_overrides: dict = {}
if _rm_bonus_amt > 0:
    _bonus_overrides['615110'] = _rm_bonus_amt
if _admin_bonus_amt > 0:
    _bonus_overrides['637110'] = _admin_bonus_amt

# These are populated from the main-area tables below
_manual_accruals_input = []   # one-sided accruals (DR only; CR 211200 auto-generated)
_periodic_supplement_rows = []  # kept for backward compat — merged from accruals table

st.sidebar.divider()


# ── Main content ─────────────────────────────────────────────

# Process button
col_btn1, col_btn2, col_btn3 = st.columns([2, 1, 1])

with col_btn1:
    run_button = st.button(
        "🚀 Run Pipeline",
        disabled=not gl_uploaded,
        use_container_width=True,
        key="run_pipeline_btn",
    )

with col_btn2:
    if st.button("🔄 Reset", use_container_width=True):
        st.session_state.processing_complete = False
        st.session_state.engine_result = None
        st.session_state.output_files = {}
        st.session_state.uploaded_files = {}
        shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
        st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")
        import pandas as _pd
        st.session_state.manual_je_df = _pd.DataFrame({
            "JE #": ["OOE-0001", "OOE-0001"], "Description": ["", ""],
            "Account Code": ["", ""], "Amount": [0.0, 0.0], "Line Description": ["", ""],
        })
        # Reset accruals table amounts to $0 but keep the pre-seeded rows
        if "manual_accruals_df" in st.session_state:
            st.session_state.manual_accruals_df["Amount ($)"] = 0.0
        st.rerun()

# ── Manual JE / Reclass Input ────────────────────────────────
import pandas as pd

# ── Table 1: One-Off Accruals ─────────────────────────────────────────────────
# DR the expense account; pipeline auto-generates the CR to 211200 Accrued Expenses.
# Use for: known invoices not yet in Nexus/Yardi, quarterly/semi-annual contracts,
# seasonal items (snow), recurring retainers, etc.
# Pre-seeded rows show common monthly items — just fill in the Amount for the month.
with st.expander("🧾 One-Off Accruals  (DR expense → CR 211200 auto)", expanded=False):
    st.caption(
        "Use this for known invoices not yet in Nexus or Yardi — quarterly contracts, seasonal items, "
        "recurring retainers, semi-annual billings, etc. "
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
                                 help="Check to auto-reverse next period (standard for accruals)"),
        },
        key="manual_accruals_editor",
    )
    st.session_state.manual_accruals_df = accruals_edited_df

    # Summary of queued accruals
    _accrual_active = accruals_edited_df[
        accruals_edited_df["Account Code"].fillna("").str.strip().astype(bool) &
        (accruals_edited_df["Amount ($)"].fillna(0) > 0)
    ]
    if not _accrual_active.empty:
        _total_accruals = _accrual_active["Amount ($)"].sum()
        st.success(
            f"✅ {len(_accrual_active)} accrual(s) queued — ${_total_accruals:,.2f} total debits",
            icon="✅",
        )

st.divider()

# ── Table 2: Manual Journal Entries & Reclasses ───────────────────────────────
# Full balanced JEs — user enters both DR and CR lines explicitly.
# Use for: reclasses between accounts, owner distributions, true-up entries,
# anything that doesn't fit the standard DR expense / CR 211200 pattern.
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
            "JE #":             st.column_config.TextColumn("JE #", width="small",
                                    help="Group lines with the same JE # (e.g. OOE-0001)"),
            "Description":      st.column_config.TextColumn("JE Description", width="medium",
                                    help="Overall description of the entry"),
            "Account Code":     st.column_config.TextColumn("Account", width="small",
                                    help="6-digit Yardi GL account code"),
            "Amount":           st.column_config.NumberColumn("Amount (+DR / -CR)", format="$%,.2f",
                                    width="small", help="Positive = Debit, Negative = Credit"),
            "Line Description": st.column_config.TextColumn("Line Description", width="large",
                                    help="Per-line description for Yardi import"),
        },
        key="manual_je_editor",
    )
    st.session_state.manual_je_df = edited_df

    # Balance validation per JE#
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
            st.success(f"✅ All entries balanced — {len(balance_check)} JE(s), ${total_dr:,.2f} total debits", icon="✅")

st.divider()


# ── Processing ───────────────────────────────────────────────
if run_button:
    with st.spinner("Processing pipeline..."):
        try:
            # Build files dict for engine
            files_dict = {
                key: st.session_state.uploaded_files.get(key)
                for key in file_config.keys()
            }

            # Create progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Step 1: Parse files
            status_text.text("Step 1/3: Parsing files...")
            progress_bar.progress(20)

            engine_result = run_pipeline(
                files_dict,
                prior_period_outstanding=prior_period_outstanding,
            )
            st.session_state.engine_result = engine_result

            # Step 2: Generate main report
            status_text.text("Step 2/3: Generating monthly report...")
            progress_bar.progress(60)

            report_path = os.path.join(st.session_state.temp_dir, "GA_Monthly_Report.xlsx")
            generate_report(engine_result, report_path)
            st.session_state.output_files["monthly_report"] = report_path

            # Step 3: Build accrual entries and prepaid schedule (needed for workpapers)
            status_text.text("Step 3/7: Building accrual entries...")
            progress_bar.progress(65)

            nexus_data = engine_result.parsed.get('nexus_accrual')
            gl_parsed = engine_result.parsed.get('gl')
            bc_parsed = engine_result.parsed.get('budget_comparison')

            # Build _manual_accruals_input from the One-Off Accruals data_editor NOW,
            # before calling build_accrual_entries.  This populates _manual_accounts
            # inside the engine so those account codes are excluded from Layers 2-4
            # (budget-gap, proration, historical), preventing double-accruals.
            # NOTE: the actual JE lines for these entries are built later via
            # _periodic_supplement_rows → _supplement_je_lines (labeled SUP-XXXX),
            # not through Layer 0 / MAN-XXXX.  We only need the account codes here
            # for exclusion; passing amount=0 skips the JE generation in Layer 0.
            _accruals_tbl_early = st.session_state.get("manual_accruals_df")
            if _accruals_tbl_early is not None and not _accruals_tbl_early.empty:
                _manual_accruals_input = [
                    {
                        'account_code': str(r["Account Code"]).strip(),
                        'account_name': str(r.get("Account Name", "") or "").strip(),
                        'amount': 0,   # 0 → Layer 0 skips JE; we only need the dedup exclusion
                        'description': '',
                    }
                    for _, r in _accruals_tbl_early.iterrows()
                    if str(r.get("Account Code", "") or "").strip()
                    and float(r.get("Amount ($)", 0) or 0) > 0
                ]

            je_lines = build_accrual_entries(
                nexus_data or [],
                period=engine_result.period or '',
                property_name=engine_result.property_name or '',
                gl_data=gl_parsed,
                budget_data=bc_parsed,
                manual_accruals=_manual_accruals_input or [],
                tenant_utility_rows=_tenant_utility_rows or None,
                kardin_records=engine_result.parsed.get('kardin_budget') or [],
                bonus_overrides=_bonus_overrides or None,
            )
            st.session_state.output_files["je_lines"] = je_lines

            # ── Prepaid ledger: load → merge → release JEs → advance ──
            close_period = engine_result.period or ''
            ledger_path  = st.session_state.uploaded_files.get("prepaid_ledger")
            ledger_active, ledger_completed = prepaid_ledger.load(ledger_path)

            # Parse paid invoices file (separate Nexus export) and detect prepaids
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

            # Add new prepaids from paid invoices (primary source for prepaid detection)
            ledger_active, newly_added_paid = prepaid_ledger.merge_nexus(
                ledger_active, nexus_paid_records, close_period
            )

            # Also check open accrual invoices for any prepaids (secondary, belt-and-suspenders)
            ledger_active, newly_added_accrual = prepaid_ledger.merge_nexus(
                ledger_active, nexus_data or [], close_period
            )
            newly_added = newly_added_paid + newly_added_accrual

            # Build visual amortization schedule — combine paid invoices (primary) + open accrual invoices
            _all_nexus_for_amort = nexus_paid_records + (nexus_data or [])
            amort_lines = build_prepaid_amortization(
                _all_nexus_for_amort,
                close_period=close_period,
            )
            st.session_state.output_files["prepaid_amortization"] = amort_lines
            st.session_state.output_files["newly_added_prepaids"] = newly_added
            st.session_state.output_files["nexus_paid_records"]   = nexus_paid_records

            # Generate prepaid release JEs for months 2+ (from ledger)
            ledger_release_lines = prepaid_ledger.get_current_amortization(
                ledger_active, close_period
            )
            prepaid_release_je = build_prepaid_release_je(
                ledger_release_lines,
                period=close_period,
                je_start=len(je_lines) // 2 + 1,
            )

            # Advance the ledger (increment months_amortized, expire completed items)
            ledger_active, ledger_completed = prepaid_ledger.advance_period(
                ledger_active, ledger_completed, close_period
            )

            # Save updated ledger for next month's upload
            updated_ledger_path = os.path.join(
                st.session_state.temp_dir, "GA_Prepaid_Ledger_Updated.xlsx"
            )
            prepaid_ledger.save(ledger_active, ledger_completed, updated_ledger_path)
            st.session_state.output_files["prepaid_ledger_updated"] = updated_ledger_path
            st.session_state.output_files["ledger_active"]    = ledger_active
            st.session_state.output_files["ledger_completed"] = ledger_completed

            # Step 4: Generate workpapers (includes prepaid tab)
            status_text.text("Step 4/7: Generating workpapers...")
            progress_bar.progress(72)

            workpaper_path = os.path.join(st.session_state.temp_dir, "GA_Workpapers.xlsx")
            generate_workpapers(engine_result, workpaper_path)
            # Append prepaid amortization tab to workpapers
            if amort_lines or ledger_release_lines:
                from openpyxl import load_workbook as _lw
                _wb = _lw(workpaper_path)
                all_amort = amort_lines + [
                    {**l, 'is_current_period': True, 'month_index': l.get('month_index', 1),
                     'total_months': l.get('total_months', 1), 'service_start': None,
                     'service_end': None, 'total_amount': l.get('monthly_amount', 0)}
                    for l in ledger_release_lines
                ]
                write_prepaid_amortization_tab(
                    _wb, all_amort,
                    period=close_period,
                    property_name=engine_result.property_name or '',
                )
                _wb.save(workpaper_path)
            st.session_state.output_files["workpapers"] = workpaper_path

            # Step 5: Management fee calculation — inject JE before writing file
            status_text.text("Step 5/7: Calculating management fee...")
            progress_bar.progress(82)

            try:
                from parsers.yardi_trial_balance import parse as parse_tb
                tb_result = None
                if "trial_balance" in st.session_state.uploaded_files:
                    tb_result = parse_tb(st.session_state.uploaded_files["trial_balance"])
                st.session_state.output_files["tb_result"] = tb_result
            except Exception:
                tb_result = None
                st.session_state.output_files["tb_result"] = None

            # Parse DACA statement early — needed as management fee basis
            # (We parse it here so the fee uses the correct source; Step 5b
            #  will reuse the result rather than re-parsing.)
            _daca_file_early = st.session_state.uploaded_files.get("daca_bank")
            _daca_parsed_early = None
            if _daca_file_early and os.path.exists(_daca_file_early):
                try:
                    from parsers.keybank_daca import parse as parse_daca_early
                    _daca_parsed_early = parse_daca_early(_daca_file_early)
                except Exception:
                    _daca_parsed_early = None

            gl_data_for_fee = engine_result.parsed.get('gl')
            fee_result = calculate_mgmt_fee(
                gl_parsed=gl_data_for_fee,
                budget_rows=bc_parsed or [],
                manual_override=cash_received_override,
                daca_parsed=_daca_parsed_early,
            )
            st.session_state.output_files["fee_result"] = fee_result

            # Inject management fee JE into accrual entries, then write file
            from management_fee import (
                build_management_fee_je,
                detect_prior_period_catchup,
                build_catchup_je,
            )
            fee_je = build_management_fee_je(
                fee_result,
                period=engine_result.period or '',
                property_code=engine_result.property_name or 'revlabpm',
                je_number=f'MGT-{len(je_lines)//2 + 1:03d}',
            )

            # ── Prior-period management fee catch-up detection ────────────────
            # Checks 637130 for unmatched auto-reversal credits (prior month
            # check cut but not yet cashed → reversal in 637130, no offset).
            _catchup_amount = detect_prior_period_catchup(gl_data_for_fee)
            st.session_state.output_files["mgmt_fee_catchup_amount"] = _catchup_amount
            _catchup_je = []
            if _catchup_amount and _catchup_amount > 0:
                _catchup_je = build_catchup_je(
                    _catchup_amount,
                    period=engine_result.period or '',
                    property_code=engine_result.property_name or 'revlabpm',
                    je_number=f'MGT-{len(je_lines)//2 + 2:03d}',
                )
            st.session_state.output_files["catchup_je"] = _catchup_je
            # Convert manual JE dataframe → je_lines format
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

            # ── One-Off Accrual JEs (from main-area table) ────────────────────
            # Convert active rows from the One-Off Accruals data_editor into
            # balanced JEs: DR expense account / CR 211200 Accrued Expenses.
            # Built AFTER build_accrual_entries so auto-detected entries and
            # user-supplied supplements coexist for the same account.
            _supplement_je_lines = []
            _sup_base = (
                len(je_lines) // 2
                + len(prepaid_release_je) // 2
                + len(fee_je) // 2
            )
            # Pull active rows from the data_editor table
            _accruals_tbl = st.session_state.get("manual_accruals_df")
            if _accruals_tbl is not None and not _accruals_tbl.empty:
                _active_accruals = _accruals_tbl[
                    _accruals_tbl["Account Code"].fillna("").str.strip().astype(bool) &
                    (_accruals_tbl["Amount ($)"].fillna(0) > 0)
                ]
                for _si, (_, _row) in enumerate(_active_accruals.iterrows()):
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

            _si_offset = 0
            for _si, _sup in enumerate(_periodic_supplement_rows):
                _sje_id   = f'SUP-{_sup_base + _si + 1:04d}'
                _sup_amt  = round(float(_sup['amount']), 2)
                _sup_desc = (
                    _sup.get('description')
                    or f"{_sup['account_name']} — one-off accrual"
                )
                _sup_vendor = _sup.get('vendor') or _sup['account_name']
                _supplement_je_lines.extend([
                    {
                        'je_number':      _sje_id, 'line': 1,
                        'date':           close_period,
                        'account_code':   _sup['account_code'],
                        'account_name':   _sup['account_name'],
                        'description':    _sup_desc,
                        'reference':      'ONE-OFF-ACCRUAL',
                        'debit':          _sup_amt,
                        'credit':         0,
                        'vendor':         _sup_vendor,
                        'invoice_number': '',
                        'source':         'contract_supplement',
                        'confidence':     'high',
                    },
                    {
                        'je_number':      _sje_id, 'line': 2,
                        'date':           close_period,
                        'account_code':   '211200',
                        'account_name':   'Accrued Expenses',
                        'description':    _sup_desc,
                        'reference':      'ONE-OFF-ACCRUAL',
                        'debit':          0,
                        'credit':         _sup_amt,
                        'vendor':         _sup_vendor,
                        'invoice_number': '',
                        'source':         'contract_supplement',
                        'confidence':     'high',
                    },
                ])
            st.session_state.output_files["supplement_je_lines"] = _supplement_je_lines

            all_je_lines = je_lines + prepaid_release_je + fee_je + _catchup_je + manual_je_lines + _supplement_je_lines
            st.session_state.output_files["all_je_lines"] = all_je_lines
            # ── Split into 3 separate Yardi CSVs ──────────────────
            _accrual_sources = {'nexus', 'budget_gap', 'historical', 'management_fee', 'management_fee_catchup', 'invoice_proration', 'prepaid_amortization', 'contract_supplement', 'tenant_utility_billing', 'bonus_accrual'}
            _prepaid_sources  = {'prepaid_ledger'}
            _manual_sources   = {'manual'}

            _accrual_lines = [l for l in all_je_lines if l.get('source') in _accrual_sources]
            _prepaid_lines  = [l for l in all_je_lines if l.get('source') in _prepaid_sources]
            _manual_lines   = [l for l in all_je_lines if l.get('source') in _manual_sources]

            if _accrual_lines:
                _path = os.path.join(st.session_state.temp_dir, "GA_Accruals_JE.csv")
                generate_yardi_je_csv(_accrual_lines, _path,
                    period=engine_result.period or '', property_code='revlabpm')
                st.session_state.output_files["accrual_je_csv"] = _path

            if _prepaid_lines:
                _path = os.path.join(st.session_state.temp_dir, "GA_Prepaid_JE.csv")
                generate_yardi_je_csv(_prepaid_lines, _path,
                    period=engine_result.period or '', property_code='revlabpm')
                st.session_state.output_files["prepaid_je_csv"] = _path

            if _manual_lines:
                _path = os.path.join(st.session_state.temp_dir, "GA_OneOff_JE.csv")
                generate_yardi_je_csv(_manual_lines, _path,
                    period=engine_result.period or '', property_code='revlabpm')
                st.session_state.output_files["oneoff_je_csv"] = _path

            # Step 5b: Parse bank statements (Operating + DACA)
            bank_rec_data  = None
            gl_cash_balance = None
            daca_bank_data  = None
            daca_gl_balance = None

            _gl_result = engine_result.parsed.get('gl')

            # ── PNC Operating — Yardi Bank Rec PDF ────────────
            # Reuse the already-parsed result from engine.run_pipeline() —
            # avoids re-opening and re-extracting the full 10-page PDF a second time.
            bank_rec_data = engine_result.parsed.get("bank_rec")
            if bank_rec_data and _gl_result:
                for _a in (_gl_result.accounts or []):
                    if _a.account_code == '111100':
                        gl_cash_balance = _a.ending_balance
                        break

            # ── DACA — KeyBank x5132 (reuse early parse from mgmt fee step) ──
            # _daca_parsed_early was parsed before the management fee calc
            # so the fee uses the correct basis (DACA additions).
            daca_bank_data = _daca_parsed_early  # may be None if not uploaded
            if daca_bank_data is not None:
                st.session_state.output_files["daca_bank_data"] = daca_bank_data
                if _gl_result:
                    for _a in (_gl_result.accounts or []):
                        if _a.account_code == '115100':
                            daca_gl_balance = _a.ending_balance
                            break

            # Store bank rec related values for dashboard
            st.session_state.output_files["bank_rec_data"]   = bank_rec_data
            st.session_state.output_files["gl_cash_balance"] = gl_cash_balance
            st.session_state.output_files["daca_gl_balance"] = daca_gl_balance

            # Step 5c: Generate BS Workpaper (if TB uploaded)
            if tb_result and engine_result.parsed.get('gl'):
                try:
                    bs_wp_path = os.path.join(st.session_state.temp_dir, "GA_BS_Workpaper.xlsx")
                    bs_workpaper_generator.generate(
                        gl_result=engine_result.parsed.get('gl'),
                        tb_result=tb_result,
                        output_path=bs_wp_path,
                        period=engine_result.period or '',
                        property_name=engine_result.property_name or 'Revolution Labs',
                        prepaid_ledger_active=ledger_active or [],
                        bank_rec_data=bank_rec_data,
                        gl_cash_balance=gl_cash_balance,
                        daca_bank_data=daca_bank_data,
                        daca_gl_balance=daca_gl_balance,
                    )
                    st.session_state.output_files["bs_workpaper"] = bs_wp_path
                except Exception as _e:
                    st.warning(f"BS Workpaper generation skipped: {_e}")

            # Step 6: Run QC engine
            status_text.text("Step 6/7: Running QC checks...")
            progress_bar.progress(88)

            # Reuse Kardin records already parsed by engine.run_pipeline() —
            # avoids a second openpyxl load of the multi-sheet Kardin workbook.
            kardin_records = engine_result.parsed.get("kardin_budget") or []

            # Derive period_month from engine_result period string
            _period_month = 1
            try:
                _m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', engine_result.period or '')
                _month_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
                if _m:
                    _period_month = _month_map.get(_m.group(1), 1)
            except Exception:
                pass

            qc_report = run_qc(
                budget_rows=bc_parsed or [],
                tb_result=tb_result,
                gl_parsed=gl_data_for_fee,
                kardin_records=kardin_records,
                accrual_entries=je_lines if je_lines else [],
                period=engine_result.period or '',
                property_name=engine_result.property_name or 'Revolution Labs Owner, LLC',
                period_month=_period_month,
                cash_received=fee_result.cash_received if fee_result.cash_received > 0 else None,
            )
            st.session_state.output_files["qc_report"] = qc_report

            qc_path = os.path.join(st.session_state.temp_dir, "GA_QC_Workbook.xlsx")
            generate_qc_workbook(qc_report, qc_path)
            st.session_state.output_files["qc_workbook"] = qc_path

            # Step 7: Generate variance comments + annotated BC
            status_text.text("Step 7/7: Generating variance comments...")
            progress_bar.progress(92)

            api_key = None
            try:
                api_key = st.secrets.get("ANTHROPIC_API_KEY")
            except Exception:
                pass  # No secrets configured

            # Build pro-forma JE adjustments so comments reflect projected final-close
            # actuals (GL + all pipeline JEs), not the pre-close Yardi snapshot.
            # Convention:
            #   Revenue (4xxxxx): actual = net credit activity → adjustment = credit − debit
            #   Expense (5-8xxxxx): actual = net debit activity → adjustment = debit − credit
            _je_adjustments: Dict[str, float] = {}
            for _adj_line in (all_je_lines or []):
                _adj_code = str(_adj_line.get('account_code', '') or '').strip()
                if not _adj_code or not is_income_statement_account(_adj_code):
                    continue
                _net_debit = (
                    float(_adj_line.get('debit', 0) or 0)
                    - float(_adj_line.get('credit', 0) or 0)
                )
                # Revenue: CR increases actual → flip sign; Expense: DR increases actual
                _adj_delta = -_net_debit if is_revenue_account(_adj_code) else _net_debit
                _je_adjustments[_adj_code] = _je_adjustments.get(_adj_code, 0.0) + _adj_delta

            # Call grp variant directly — returns Dict keyed by account code,
            # which we need for the BC write-back AND the dashboard display
            _gl_for_vc   = engine_result.parsed.get('gl')
            _bc_for_vc   = bc_parsed or []
            _kard_for_vc = engine_result.parsed.get('kardin_budget') or []
            _period_str  = engine_result.period or ''
            _prop_str    = engine_result.property_name or 'Revolution Labs Owner, LLC'

            _api_fallback_reason = None  # set below if API was requested but failed
            if _bc_for_vc:
                comments_map = generate_variance_comments_grp(
                    budget_rows=_bc_for_vc,
                    gl_parsed=_gl_for_vc,
                    kardin_records=_kard_for_vc,
                    period=_period_str,
                    property_name=_prop_str,
                    api_key=api_key,
                    je_adjustments=_je_adjustments,
                )
                # Detect silent API fallback — surface before user sees outputs
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
                    method = 'data-driven (API FALLBACK)'
                else:
                    method = 'api' if api_key else 'data-driven'

                var_comments = [
                    {
                        'account_code': code,
                        'account_name': entry['account_name'],
                        'comment': entry.get('mtd_comment', ''),
                        'ytd_comment': entry.get('ytd_comment', ''),
                        'mtd_tier': entry.get('mtd_tier', 'tier_3'),
                        'ytd_tier': entry.get('ytd_tier', 'tier_3'),
                        'method': method,
                        'api_fallback_reason': _api_fallback_reason,
                    }
                    for code, entry in comments_map.items()
                    if entry.get('mtd_tier') != 'tier_3' or entry.get('ytd_tier') != 'tier_3'
                ]
                st.session_state.output_files["variance_comments"] = var_comments

                # Write annotated BC (GRP internal — comments in cols L/M)
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
                    st.session_state.output_files["annotated_bc"] = _annotated_bc_path
            else:
                st.session_state.output_files["variance_comments"] = []

            # Step 8: Generate exception report
            status_text.text("Step 7/7: Generating validation report...")
            progress_bar.progress(96)

            # Inject API fallback into exception list so it appears in the Excel report
            if api_key and _api_fallback_reason:
                engine_result.exceptions.append(Exception_(
                    severity='warning',
                    category='commentary',
                    source='variance_comments',
                    description=(
                        'Variance commentary API fallback: AI commentary was requested but '
                        'the API call failed. Comments in the Budget Comparison are '
                        f'data-driven templates, NOT AI-generated. '
                        f'Reason: {_api_fallback_reason}'
                    ),
                    details={'api_fallback_reason': _api_fallback_reason},
                ))

            exception_path = os.path.join(st.session_state.temp_dir, "GA_Exceptions_Report.xlsx")
            generate_exception_report(engine_result, exception_path)
            st.session_state.output_files["exception_report"] = exception_path

            progress_bar.progress(100)
            status_text.text("✓ Pipeline complete!")
            st.session_state.processing_complete = True

            # Brief success message
            st.success("Pipeline processing complete!", icon="✅")

        except Exception as e:
            tb = traceback.format_exc()
            st.error(f"Pipeline error: {str(e)}", icon="❌")
            st.code(tb, language="python")
            st.session_state.processing_complete = False


# ── Results dashboard ────────────────────────────────────────
if st.session_state.processing_complete and st.session_state.engine_result:
    result = st.session_state.engine_result

    st.divider()
    st.markdown("## Results Dashboard")

    # Status banner
    status = result.status
    if status == "CLEAN":
        status_color = "#2ecc71"
        status_text = "✅ CLEAN"
    elif status == "WARNINGS":
        status_color = "#f39c12"
        status_text = "➀️ WARNINGS"
    else:  # ERRORS
        status_color = "#e74c3c"
        status_text = "❌ ERRORS"

    st.markdown(f"""
    <div style="background-color: {status_color}20; border-left: 5px solid {status_color}; padding: 15px; border-radius: 5px; margin: 15px 0;">
        <h3 style="color: {status_color}; margin: 0;">{status_text}</h3>
    </div>
    """, unsafe_allow_html=True)

    # ── Period-state indicator ─────────────────────────────────────
    _ps = result.period_state
    if _ps and _ps.get('state') != 'unknown':
        _state_labels = {
            'pre_close':  ('🟡 Pre-Close', '#f39c12',
                           'GL is for the current period — books still open. '
                           'Accruals are provisional until final close.'),
            'at_close':   ('🟢 At Close', '#2ecc71',
                           'Month-end close window. Final accruals are being posted.'),
            'post_close': ('🔵 Post-Close', '#3498db',
                           'Running retrospective analysis on a closed period. '
                           'Accruals and reports are final for this month.'),
        }
        _label, _color, _desc = _state_labels.get(
            _ps['state'], ('⚪ Unknown', '#95a5a6', '')
        )
        _promo = ' _(promoted by 213100 GL signal)_' if _ps.get('promoted') else ''
        _days = _ps.get('days_since_close', 0)
        _day_str = (f'{abs(_days)} day{"s" if abs(_days) != 1 else ""} until close'
                    if _days < 0 else
                    f'{_days} day{"s" if _days != 1 else ""} since close'
                    if _days > 0 else 'closes today')
        with st.expander(f"{_label} — {_day_str}{_promo}", expanded=False):
            st.markdown(_desc)
            if _ps.get('gl_signal_detected'):
                st.info(
                    f"213100 GL signal detected: net credit ${_ps['gl_signal_amount']:,.2f} "
                    f"— prior-period auto-reversals have posted."
                )

    # ── QC Summary Panel ──────────────────────────────────────────
    qc_report = st.session_state.output_files.get("qc_report")
    if qc_report:
        st.markdown("### QC Checks")
        qc_overall = qc_report.overall_status
        qc_color = {'PASS': '#2ecc71', 'FLAG': '#f39c12', 'FAIL': '#e74c3c'}.get(qc_overall, '#95a5a6')
        qc_icon = {'PASS': '✅', 'FLAG': '⚠️', 'FAIL': '❌'}.get(qc_overall, 'ℹ️')
        st.markdown(f"""
        <div style="background:{qc_color}20;border-left:5px solid {qc_color};padding:10px 15px;border-radius:5px;margin:10px 0 5px;">
            <strong style="color:{qc_color};">{qc_icon} QC Overall: {qc_overall}</strong>
            &nbsp;&nbsp;&nbsp;{sum(1 for c in qc_report.checks if c.status=='PASS')} PASS &nbsp;
            {sum(1 for c in qc_report.checks if c.status=='FLAG')} FLAG &nbsp;
            {sum(1 for c in qc_report.checks if c.status=='FAIL')} FAIL
        </div>
        """, unsafe_allow_html=True)

        qc_rows = []
        for chk in qc_report.checks:
            chk_icon = {'PASS': '✅', 'FLAG': '⚠️', 'FAIL': '❌'}.get(chk.status, '')
            qc_rows.append({
                "Check": chk.check_id.replace('CHECK_', '') + ' — ' + chk.check_name,
                "Status": f"{chk_icon} {chk.status}",
                "Findings": chk.flag_count,
                "Summary": chk.summary,
            })
        st.dataframe(qc_rows, use_container_width=True, hide_index=True,
                     column_config={
                         "Check": st.column_config.TextColumn(width="medium"),
                         "Status": st.column_config.TextColumn(width="small"),
                         "Findings": st.column_config.NumberColumn(width="small"),
                         "Summary": st.column_config.TextColumn(width="large"),
                     })
        st.divider()

    # ── Management Fee Panel ───────────────────────────────────────
    fee_result = st.session_state.output_files.get("fee_result")
    if fee_result and fee_result.cash_received > 0:
        st.markdown("### Management Fee")
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1:
            st.metric("Cash Received", f"${fee_result.cash_received:,.0f}",
                      help=f"Source: {fee_result.cash_source}")
        with col_f2:
            st.metric(f"JLL Fee ({fee_result.jll_rate:.2%})", f"${fee_result.jll_fee:,.0f}")
        with col_f3:
            st.metric(f"GRP Fee ({fee_result.grp_rate:.2%})", f"${fee_result.grp_fee:,.0f}")
        with col_f4:
            st.metric(f"Total ({fee_result.total_rate:.2%})", f"${fee_result.total_fee:,.0f}")
        bc_parsed_for_fee = engine_result.parsed.get('budget_comparison') or []
        accrued = accrued_fee_from_bc(bc_parsed_for_fee)
        if accrued > 0:
            diff = accrued - fee_result.total_fee
            diff_str = f"${abs(diff):,.0f} {'over' if diff > 0 else 'under'} calculated"
            st.caption(f"BC accrued (637130): ${accrued:,.2f} — {diff_str}")

        # ── Prior-period catch-up alert ────────────────────────────
        _catchup_amt = st.session_state.output_files.get("mgmt_fee_catchup_amount")
        if _catchup_amt and _catchup_amt > 0:
            st.warning(
                f"**Management Fee Catch-up Detected — ${_catchup_amt:,.2f}**\n\n"
                f"Account 637130 shows a net credit of ${_catchup_amt:,.2f} in the current period "
                f"(auto-reversal of prior month accrual with no matching invoice entry). "
                f"This typically means the prior month's GRP/JLL check was not cashed before "
                f"bank close. A catch-up entry **(MGT-002)** has been included in the Accruals CSV "
                f"to restore the expense.\n\n"
                f"**Verify before posting:** confirm the prior month check is still outstanding "
                f"in AP (account 211200) before uploading the catch-up JE to Yardi."
            )
            _catchup_total_dr = (fee_result.total_fee or 0) + _catchup_amt
            st.caption(
                f"Total 637130 DR this period: ${fee_result.total_fee:,.2f} (current month, MGT-001) "
                f"+ ${_catchup_amt:,.2f} (prior month catch-up, MGT-002) = ${_catchup_total_dr:,.2f}"
            )

        st.divider()

    # ── Accruals Panel ─────────────────────────────────────────────
    all_je_lines = st.session_state.output_files.get("all_je_lines", [])
    # DR lines only (debit > 0) represent unique accrual entries
    dr_lines = [l for l in all_je_lines if (l.get('debit') or 0) > 0]

    if dr_lines:
        st.markdown("### Accrual Journal Entries")

        # Source breakdown
        source_totals = {}
        for l in dr_lines:
            src = l.get('source', 'other')
            source_totals[src] = source_totals.get(src, 0) + (l.get('debit') or 0)

        source_labels = {
            'manual':                'Manual Override',
            'nexus':                 'Nexus AP',
            'invoice_proration':     'Invoice Proration',
            'prepaid_amortization':  'Prepaid Amortization',
            'budget_gap':            'Budget Gap',
            'historical':            'Historical Pattern',
            'prepaid_ledger':        'Prepaid Release',
            'management_fee':         'Management Fee',
            'management_fee_catchup': 'Mgmt Fee Catch-up',
            'bonus_accrual':          'Payroll Bonus',
            'other':                  'Other',
        }

        cols = st.columns(len(source_totals) + 1)
        with cols[0]:
            st.metric("Total JEs", len(set(l.get('je_number','') for l in dr_lines)),
                      help="Unique journal entries across all sources")
        for i, (src, total) in enumerate(source_totals.items(), 1):
            with cols[i]:
                st.metric(source_labels.get(src, src), f"${total:,.2f}")

        # Detail table — one row per DR line (each = one side of one JE)
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

        # Nexus pending detail (expanded, only if Nexus was uploaded)
        nexus_parsed = result.parsed.get('nexus_accrual')
        if nexus_parsed:
            pending = [r for r in nexus_parsed if (r.get('invoice_status') or '').lower() == 'pending approval']
            if pending:
                with st.expander(f"Nexus AP Detail — {len(pending)} pending invoice(s)"):
                    nexus_rows = []
                    for r in pending:
                        nexus_rows.append({
                            "Vendor":       r['vendor'],
                            "Invoice #":    r['invoice_number'],
                            "Invoice Date": str(r['invoice_date']) if r['invoice_date'] else '',
                            "Description":  r['line_description'],
                            "GL Account":   r['gl_account_number'],
                            "Amount":       r['amount'],
                            "Prepaid":      "Yes" if r.get('is_prepaid') else "",
                        })
                    st.dataframe(nexus_rows, use_container_width=True, hide_index=True,
                                 column_config={
                                     "Vendor":       st.column_config.TextColumn(width="medium"),
                                     "Invoice #":    st.column_config.TextColumn(width="small"),
                                     "Invoice Date": st.column_config.TextColumn(width="small"),
                                     "Description":  st.column_config.TextColumn(width="large"),
                                     "GL Account":   st.column_config.TextColumn(width="small"),
                                     "Amount":       st.column_config.NumberColumn(format="$%,.2f"),
                                     "Prepaid":      st.column_config.TextColumn(width="small"),
                                 })
        st.divider()
    else:
        # No accruals generated — still show the panel with a note
        st.markdown("### Accrual Journal Entries")
        st.info("No accrual entries generated this period. Upload a Nexus file, Budget Comparison, "
                "or Prepaid Ledger to enable additional accrual detection layers.", icon="ℹ️")
        st.divider()

    # ── Prepaid Amortization Panel ──────────────────────────────────
    amort_lines = st.session_state.output_files.get("prepaid_amortization", [])
    if amort_lines:
        st.markdown("### Prepaid Expense Amortization")
        current_lines = [l for l in amort_lines if l.get('is_current_period')]
        future_lines = [l for l in amort_lines if not l.get('is_current_period')]
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.metric("Current Period Expense", f"${sum(l['monthly_amount'] for l in current_lines):,.2f}")
        with col_p2:
            st.metric("Future Periods (Prepaid Asset)", f"${sum(l['monthly_amount'] for l in future_lines):,.2f}")

        amort_rows = []
        for l in amort_lines:
            amort_rows.append({
                "Vendor": l['vendor'],
                "Invoice #": l['invoice_number'],
                "Period": l['period_label'],
                "Month": f"{l['month_index']}/{l['total_months']}",
                "Monthly Amount": l['monthly_amount'],
                "GL Account": l['gl_account_number'],
                "Current Period": "Yes" if l.get('is_current_period') else "",
            })
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

    # ── Prepaid Ledger Status Panel ────────────────────────────────
    ledger_active    = st.session_state.output_files.get("ledger_active", [])
    ledger_completed = st.session_state.output_files.get("ledger_completed", [])
    newly_added      = st.session_state.output_files.get("newly_added_prepaids", [])

    if ledger_active or ledger_completed or newly_added:
        st.markdown("### Prepaid Ledger")
        col_l1, col_l2, col_l3 = st.columns(3)
        with col_l1:
            st.metric("Active Prepaid Items", len(ledger_active),
                      help="Items still being amortized — upload updated ledger next month")
        with col_l2:
            st.metric("New This Month", len(newly_added),
                      help="Invoices auto-detected as prepaid from the Nexus Invoice Detail (Paid) file and added to the ledger")
        with col_l3:
            st.metric("Completed This Month", len(ledger_completed),
                      help="Items that fully amortized as of this close")

        if ledger_active:
            ledger_rows = []
            for item in ledger_active:
                svc_start = item.get('service_start')
                svc_end   = item.get('service_end')
                ledger_rows.append({
                    "Vendor": item.get('vendor', ''),
                    "Invoice #": item.get('invoice_number', ''),
                    "GL Account": item.get('gl_account_number', ''),
                    "Monthly Amt": item.get('monthly_amount', 0),
                    "Months Left": int(item.get('remaining_months', 0) or 0),
                    "Service End": str(svc_end) if svc_end else '',
                    "First Added": item.get('first_added_period', ''),
                })
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

    # Summary metrics
    st.markdown("### Summary Metrics")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Files Processed",
            result.summary.get("files_processed", 0),
        )

    with col2:
        st.metric(
            "GL Accounts",
            result.summary.get("gl_accounts", 0),
        )

    with col3:
        st.metric(
            "GL Transactions",
            result.summary.get("gl_transactions", 0),
        )

    with col4:
        gl_balanced = "Yes" if result.summary.get("gl_balanced") else "No"
        st.metric(
            "GL Balanced",
            gl_balanced,
        )

    with col5:
        exception_count = result.summary.get("exceptions_error", 0) + result.summary.get("exceptions_warning", 0)
        st.metric(
            "Exceptions",
            f"{result.summary.get('exceptions_error', 0)}E / {result.summary.get('exceptions_warning', 0)}W",
        )

    st.divider()

    # Validation results
    st.markdown("### Parser Status")
    parser_data = []
    for parser_name, parsed_data in result.parsed.items():
        parser_data.append({
            "Parser": parser_name.replace("_", " ").title(),
            "Status": "✅ Success",
        })

    if parser_data:
        st.dataframe(
            parser_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Parser": st.column_config.TextColumn(),
                "Status": st.column_config.TextColumn(),
            }
        )

    st.divider()

    # Budget variances with comments
    if result.budget_variances:
        st.markdown("### Budget Variances (Flagged)")

        # Get variance comments if generated
        var_comments = st.session_state.output_files.get("variance_comments", [])
        comments_map = {vc['account_code']: vc.get('comment', '') for vc in var_comments} if var_comments else {}
        comment_method = var_comments[0].get('method', 'data-driven') if var_comments else 'none'

        if comment_method == 'api':
            st.caption("Variance comments generated via Claude API")
        elif comment_method == 'data-driven':
            st.caption("Variance comments generated from GL transaction detail (configure ANTHROPIC_API_KEY in Streamlit secrets for narrative comments)")

        variance_data = []
        for var in result.budget_variances:
            code = var.get("account_code", "")
            variance_data.append({
                "Account Code": code,
                "Account Name": var.get("account_name", ""),
                "Actual": var.get("ptd_actual", 0),
                "Budget": var.get("ptd_budget", 0),
                "Variance": var.get("variance", 0),
                "Variance %": f"{var.get('variance_pct', 0):.1f}%",
                "Comment": comments_map.get(code, ''),
            })

        st.dataframe(
            variance_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Account Code": st.column_config.TextColumn(width="small"),
                "Account Name": st.column_config.TextColumn(width="medium"),
                "Actual": st.column_config.NumberColumn(format="$%,.2f"),
                "Budget": st.column_config.NumberColumn(format="$%,.2f"),
                "Variance": st.column_config.NumberColumn(format="$%,.2f"),
                "Variance %": st.column_config.TextColumn(width="small"),
                "Comment": st.column_config.TextColumn(width="large"),
            }
        )
        st.divider()

    # ── Bank Rec Summary Panel ─────────────────────────────────────────────────
    _bank_rec  = st.session_state.output_files.get("bank_rec_data")
    _daca_data = st.session_state.output_files.get("daca_bank_data")
    _gl_111    = float(st.session_state.output_files.get("gl_cash_balance") or 0)
    _daca_gl   = float(st.session_state.output_files.get("daca_gl_balance") or 0)

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

    # Bank reconciliation (engine cross-match detail)
    if result.gl_bank_matches:
        with st.expander("Engine Bank Match Detail"):
            recon_data = []
            for match in result.gl_bank_matches:
                recon_data.append({
                    "Description": match.description,
                    "GL Amount": match.amount_a,
                    "Bank Amount": match.amount_b,
                    "Matched": "✅" if match.matched else "⚠️",
                    "Variance": abs(match.variance),
                })

            st.dataframe(
                recon_data,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Description": st.column_config.TextColumn(),
                    "GL Amount": st.column_config.NumberColumn(format="$%,.2f"),
                    "Bank Amount": st.column_config.NumberColumn(format="$%,.2f"),
                    "Matched": st.column_config.TextColumn(),
                    "Variance": st.column_config.NumberColumn(format="$%,.2f"),
                }
            )
        st.divider()

    # Debt service
    if result.debt_service_check and result.debt_service_check.get("loans"):
        st.markdown("### Debt Service Summary")

        debt_data = []
        for loan in result.debt_service_check["loans"]:
            debt_data.append({
                "Loan": loan.get("name", "Unknown"),
                "Principal Balance": loan.get("principal_balance", 0),
                "Interest Paid YTD": loan.get("interest_paid_ytd", 0),
            })

        st.dataframe(
            debt_data,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Loan": st.column_config.TextColumn(),
                "Principal Balance": st.column_config.NumberColumn(format="$%,.2f"),
                "Interest Paid YTD": st.column_config.NumberColumn(format="$%,.2f"),
            }
        )
        st.divider()

    # Exceptions list
    if result.exceptions:
        st.markdown("### Exceptions & Findings")

        for i, exc in enumerate(result.exceptions):
            severity_class = f"exception-{exc.severity}"
            severity_badge = {
                "error": "🔴 ERROR",
                "warning": "🟡 WARNING",
                "info": "🔵 INFO",
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

    # Download section
    st.markdown("### Download Results")

    # ── Draft Package (zip of all outputs) ───────────────────────
    import zipfile, io
    period_label = (engine_result.period or 'Period').replace('-', '_')
    zip_files = {
        f"RevLabs_{period_label}_BS_Workpaper.xlsx":    st.session_state.output_files.get("bs_workpaper"),
        f"RevLabs_{period_label}_Accruals_JE.csv":      st.session_state.output_files.get("accrual_je_csv"),
        f"RevLabs_{period_label}_Prepaid_JE.csv":       st.session_state.output_files.get("prepaid_je_csv"),
        f"RevLabs_{period_label}_OneOff_JE.csv":        st.session_state.output_files.get("oneoff_je_csv"),
        f"RevLabs_{period_label}_QC_Workbook.xlsx":     st.session_state.output_files.get("qc_workbook"),
        f"RevLabs_{period_label}_Workpapers.xlsx":      st.session_state.output_files.get("workpapers"),
        f"RevLabs_{period_label}_Prepaid_Ledger.xlsx":  st.session_state.output_files.get("prepaid_ledger_updated"),
        f"RevLabs_{period_label}_Monthly_Report.xlsx":  st.session_state.output_files.get("monthly_report"),
        f"RevLabs_{period_label}_BC_Internal.xlsx":     st.session_state.output_files.get("annotated_bc"),
    }
    zip_files = {k: v for k, v in zip_files.items() if v and os.path.exists(v)}

    if zip_files:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fname, fpath in zip_files.items():
                zf.write(fpath, fname)
        zip_buf.seek(0)
        st.download_button(
            label=f"📦 Download Draft Package — All Outputs ({len(zip_files)} files)",
            data=zip_buf,
            file_name=f"RevLabs_{period_label}_Close_Package_{datetime.now().strftime('%Y%m%d')}.zip",
            mime="application/zip",
            use_container_width=True,
            help="All pipeline outputs in one zip — BS Workpaper, JE Import, QC, Workpapers, Prepaid Ledger",
        )
    st.divider()
    st.markdown("##### Individual Downloads")

    col1, col2 = st.columns(2)

    with col1:
        if "monthly_report" in st.session_state.output_files:
            report_path = st.session_state.output_files["monthly_report"]
            if os.path.exists(report_path):
                with open(report_path, "rb") as f:
                    st.download_button(
                        label="📊 Download Monthly Report",
                        data=f.read(),
                        file_name=f"GA_Monthly_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

    with col2:
        if "workpapers" in st.session_state.output_files:
            wp_path = st.session_state.output_files["workpapers"]
            if os.path.exists(wp_path):
                with open(wp_path, "rb") as f:
                    st.download_button(
                        label="📑 Download Workpapers",
                        data=f.read(),
                        file_name=f"GA_Workpapers_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

    col3, col4 = st.columns(2)

    with col3:
        if "accrual_je_csv" in st.session_state.output_files:
            _p = st.session_state.output_files["accrual_je_csv"]
            if os.path.exists(_p):
                with open(_p, "rb") as f:
                    st.download_button(
                        label="📝 Download Accruals JE (.csv)",
                        data=f.read(),
                        file_name=f"RevLabs_Accruals_JE_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        help="AP accruals, budget gaps, historical patterns, management fee",
                    )

    with col4:
        if "prepaid_je_csv" in st.session_state.output_files:
            _p = st.session_state.output_files["prepaid_je_csv"]
            if os.path.exists(_p):
                with open(_p, "rb") as f:
                    st.download_button(
                        label="📝 Download Prepaid JE (.csv)",
                        data=f.read(),
                        file_name=f"RevLabs_Prepaid_JE_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        help="Prepaid amortization entries — upload separately from accruals",
                    )

    col3c, col4c = st.columns(2)
    with col3c:
        if "oneoff_je_csv" in st.session_state.output_files:
            _p = st.session_state.output_files["oneoff_je_csv"]
            if os.path.exists(_p):
                with open(_p, "rb") as f:
                    st.download_button(
                        label="📝 Download One-Off JE (.csv)",
                        data=f.read(),
                        file_name=f"RevLabs_OneOff_JE_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        help="Manual reclasses and one-off adjustments — review before posting",
                    )

    with col4c:
        if "exception_report" in st.session_state.output_files:
            exception_path = st.session_state.output_files["exception_report"]
            if os.path.exists(exception_path):
                with open(exception_path, "rb") as f:
                    st.download_button(
                        label="📋 Download Validation Report",
                        data=f.read(),
                        file_name=f"GA_Exceptions_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

    col_l, col_r = st.columns(2)

    with col_l:
        if "prepaid_ledger_updated" in st.session_state.output_files:
            pl_path = st.session_state.output_files["prepaid_ledger_updated"]
            if os.path.exists(pl_path):
                with open(pl_path, "rb") as f:
                    st.download_button(
                        label="🗂️ Download Updated Prepaid Ledger",
                        data=f.read(),
                        file_name=f"GA_Prepaid_Ledger_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Upload this file next month as the Prepaid Ledger to continue amortization tracking",
                    )

    col_bs1, col_bs2 = st.columns(2)

    with col_bs1:
        if "bs_workpaper" in st.session_state.output_files:
            bs_path = st.session_state.output_files["bs_workpaper"]
            if os.path.exists(bs_path):
                with open(bs_path, "rb") as f:
                    st.download_button(
                        label="📒 Download BS Reconciliation Workpaper",
                        data=f.read(),
                        file_name=f"GA_BS_Workpaper_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Balance sheet reconciliation: one tab per BS account with GL vs TB tie-out",
                    )

    col5, col6 = st.columns(2)

    with col5:
        if "qc_workbook" in st.session_state.output_files:
            qc_path = st.session_state.output_files["qc_workbook"]
            if os.path.exists(qc_path):
                with open(qc_path, "rb") as f:
                    st.download_button(
                        label="🔍 Download QC Workbook",
                        data=f.read(),
                        file_name=f"GA_QC_Workbook_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

    with col6:
        if "annotated_bc" in st.session_state.output_files:
            ann_bc_path = st.session_state.output_files["annotated_bc"]
            if os.path.exists(ann_bc_path):
                with open(ann_bc_path, "rb") as f:
                    st.download_button(
                        label="💬 Download Budget Comparison (Internal + Comments)",
                        data=f.read(),
                        file_name=f"GA_Budget_Comparison_Internal_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="GRP internal use only — contains Claude variance comments in columns L/M",
                    )


# ── Info section (when no processing yet) ─────────────────────
if not st.session_state.processing_complete:
    st.info("""
    **Instructions:**
    1. Upload the **Yardi GL Detail** file in the sidebar (required — everything else is optional)
    2. Upload any additional files you have: Trial Balance, Budget Comparison, Kardin, Nexus, bank statements, loan PDFs
    3. Fill in the **One-Off Accruals** table above for any known invoices not yet in Nexus/Yardi (quarterly contracts, retainers, seasonal items, semi-annual billings, etc.)
    4. Use the **Manual JEs & Reclasses** table for entries where you control both sides (reclasses, true-ups, owner distributions)
    5. Click **Run Pipeline** — pipeline auto-detects accruals, reconciles the bank, computes the management fee, and runs 7 QC checks
    6. Download outputs: Singerman workbook, workpapers, QC report, and 3 Yardi-import JE CSVs (Accruals, Prepaid, Manual)
    """, icon="ℹ️")


# ── Cleanup on session end ───────────────────────────────────
def cleanup_on_exit():
    """Clean up temporary files."""
    if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
        shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)


import atexit
atexit.register(cleanup_on_exit)
