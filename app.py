"""
GA Automation — Monthly Report Pipeline
========================================
A Streamlit web app for the Greatland Realty Partners CRE accounting pipeline.
Processes Yardi, Nexus, bank, and loan exports to produce monthly reports for Singerman.
"""

import streamlit as st
import sys
import os
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

# ── Setup paths ──────────────────────────────────────────────
pipeline_dir = Path(__file__).parent / "pipeline"
if str(pipeline_dir) not in sys.path:
    sys.path.insert(0, str(pipeline_dir))

from engine import run_pipeline, EngineResult
from report_generator import generate_report, generate_exception_report
from workpaper_generator import generate_workpapers
import traceback
from accrual_entry_generator import (
    build_accrual_entries, generate_yardi_je_import,
    build_prepaid_amortization, write_prepaid_amortization_tab,
)
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


# ── Header ───────────────────────────────────────────────────
st.markdown("<h1 class='main-header'>Greatland Realty Partners</h1>", unsafe_allow_html=True)
st.markdown("### GA Automation — Monthly Report Pipeline")
st.markdown("**Revolution Labs | 1050 Waltham**")
st.divider()


# ── Sidebar: File uploads ────────────────────────────────────
st.sidebar.markdown("## File Uploads")

file_config = {
    "gl": ("Yardi GL Detail (.xlsx)", "*.xlsx", True),
    "trial_balance": ("Yardi Trial Balance (.xlsx)", "*.xlsx", False),
    "income_statement": ("Yardi Income Statement (.xlsx)", "*.xlsx", False),
    "budget_comparison": ("Yardi Budget Comparison (.xlsx)", "*.xlsx", False),
    "rent_roll": ("Yardi Rent Roll (.xlsx)", "*.xlsx", False),
    "nexus_accrual": ("Nexus Accrual Detail (.xls)", "*.xls", False),
    "pnc_bank": ("PNC Bank Statement (.pdf)", "*.pdf", False),
    "loan": ("Berkadia Loan PDFs (.pdf)", "*.pdf", False),
    "kardin_budget": ("Kardin Budget (.xlsx)", "*.xlsx", False),
}

for key, (label, file_type, required) in file_config.items():
    col1, col2 = st.sidebar.columns([5, 1])

    with col1:
        if key == "loan":
            uploaded_files_multi = st.file_uploader(
                label,
                type="pdf",
                accept_multiple_files=True,
                key="uploader_loan",
            )
            if uploaded_files_multi:
                paths = []
                for uf in uploaded_files_multi:
                    temp_file = os.path.join(st.session_state.temp_dir, uf.name)
                    with open(temp_file, "wb") as f:
                        f.write(uf.getbuffer())
                    paths.append(temp_file)
                st.session_state.uploaded_files["loan"] = paths
        else:
            uploaded_file = st.file_uploader(
                label,
                type=file_type.replace("*.", ""),
                key=f"uploader_{key}",
            )

            if uploaded_file is not None:
                # Save to temp directory
                temp_file = os.path.join(st.session_state.temp_dir, uploaded_file.name)
                with open(temp_file, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.session_state.uploaded_files[key] = temp_file

    with col2:
        if key in st.session_state.uploaded_files:
            st.markdown("✅")

# Display file count
uploaded_count = len(st.session_state.uploaded_files)
st.sidebar.markdown(f"**Files uploaded: {uploaded_count} / {len(file_config)}**")

# Check if GL file (required) is uploaded
gl_uploaded = "gl" in st.session_state.uploaded_files
st.sidebar.divider()

if not gl_uploaded:
    st.sidebar.warning("⚠️ GL Detail file is required to run the pipeline.")

# ── Bank Rec Settings ────────────────────────────────────────────
st.sidebar.markdown("## Bank Rec")
prior_period_outstanding = st.sidebar.number_input(
    "Prior Period Outstanding Checks ($)",
    min_value=0.0,
    value=0.0,
    step=100.0,
    format="%.2f",
    help=(
        "Outstanding checks from prior periods not visible in the current GL. "
        "Leave 0 on first run — if a reconciling difference appears, enter "
        "that amount here to close the rec. GRP confirms these are legitimate "
        "uncleared checks from a prior month."
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
    help="Override auto-detected cash received for management fee calc. Leave 0 to auto-detect from GL.",
)
cash_received_override = cash_received_override if cash_received_override > 0 else None
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
        st.rerun()

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

            je_lines = build_accrual_entries(
                nexus_data or [],
                period=engine_result.period or '',
                property_name=engine_result.property_name or '',
                gl_data=gl_parsed,
                budget_data=bc_parsed,
            )

            amort_lines = build_prepaid_amortization(
                nexus_data or [],
                close_period=engine_result.period or '',
            )
            st.session_state.output_files["prepaid_amortization"] = amort_lines

            # Step 4: Generate workpapers (includes prepaid tab)
            status_text.text("Step 4/7: Generating workpapers...")
            progress_bar.progress(72)

            workpaper_path = os.path.join(st.session_state.temp_dir, "GA_Workpapers.xlsx")
            generate_workpapers(engine_result, workpaper_path)
            # Append prepaid amortization tab to workpapers
            if amort_lines:
                from openpyxl import load_workbook as _lw
                _wb = _lw(workpaper_path)
                write_prepaid_amortization_tab(
                    _wb, amort_lines,
                    period=engine_result.period or '',
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

            gl_data_for_fee = engine_result.parsed.get('gl')
            fee_result = calculate_mgmt_fee(
                gl_parsed=gl_data_for_fee,
                budget_rows=bc_parsed or [],
                manual_override=cash_received_override,
            )
            st.session_state.output_files["fee_result"] = fee_result

            # Inject management fee JE into accrual entries, then write file
            from management_fee import build_management_fee_je
            fee_je = build_management_fee_je(
                fee_result,
                period=engine_result.period or '',
                property_code=engine_result.property_name or 'revlabpm',
                je_number=f'MGT-{len(je_lines)//2 + 1:03d}',
            )
            all_je_lines = je_lines + fee_je
            if all_je_lines:
                je_path = os.path.join(st.session_state.temp_dir, "GA_Accrual_JE_Import.xlsx")
                generate_yardi_je_import(
                    all_je_lines, je_path,
                    period=engine_result.period or '',
                    property_name=engine_result.property_name or '',
                )
                st.session_state.output_files["accrual_je"] = je_path

            # Step 6: Run QC engine
            status_text.text("Step 6/7: Running QC checks...")
            progress_bar.progress(88)

            try:
                from parsers.kardin_budget import parse as parse_kardin
                kardin_records = []
                if "kardin_budget" in st.session_state.uploaded_files:
                    kardin_records = parse_kardin(st.session_state.uploaded_files["kardin_budget"])
            except Exception:
                kardin_records = []

            # Derive period_month from engine_result period string
            _period_month = 1
            try:
                import re as _re
                _m = _re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', engine_result.period or '')
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

            # Call grp variant directly — returns Dict keyed by account code,
            # which we need for the BC write-back AND the dashboard display
            _gl_for_vc   = engine_result.parsed.get('gl')
            _bc_for_vc   = bc_parsed or []
            _kard_for_vc = engine_result.parsed.get('kardin_budget') or []
            _period_str  = engine_result.period or ''
            _prop_str    = engine_result.property_name or 'Revolution Labs Owner, LLC'

            if _bc_for_vc:
                comments_map = generate_variance_comments_grp(
                    budget_rows=_bc_for_vc,
                    gl_parsed=_gl_for_vc,
                    kardin_records=_kard_for_vc,
                    period=_period_str,
                    property_name=_prop_str,
                    api_key=api_key,
                )
                # Store list format for dashboard (backwards-compatible)
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
        st.divider()

    # ── Accruals Panel ─────────────────────────────────────────────
    nexus_parsed = result.parsed.get('nexus_accrual')
    if nexus_parsed:
        pending = [r for r in nexus_parsed if (r.get('invoice_status') or '').lower() == 'pending approval']
        if pending:
            st.markdown("### AP Accruals — Pending Approval")
            col_a1, col_a2, col_a3 = st.columns(3)
            with col_a1:
                st.metric("Invoices to Accrue", len(pending))
            with col_a2:
                st.metric("Total Accrual Amount", f"${sum(r['amount'] for r in pending):,.2f}")
            with col_a3:
                prepaid_count = sum(1 for r in pending if r.get('is_prepaid'))
                st.metric("Multi-Period (Prepaid) Invoices", prepaid_count)

            accrual_rows = []
            for r in pending:
                accrual_rows.append({
                    "Vendor": r['vendor'],
                    "Invoice #": r['invoice_number'],
                    "Invoice Date": str(r['invoice_date']) if r['invoice_date'] else '',
                    "Description": r['line_description'],
                    "GL Account": r['gl_account_number'],
                    "Amount": r['amount'],
                    "Prepaid": "Yes" if r.get('is_prepaid') else "",
                })
            st.dataframe(accrual_rows, use_container_width=True, hide_index=True,
                         column_config={
                             "Vendor": st.column_config.TextColumn(width="medium"),
                             "Invoice #": st.column_config.TextColumn(width="small"),
                             "Invoice Date": st.column_config.TextColumn(width="small"),
                             "Description": st.column_config.TextColumn(width="large"),
                             "GL Account": st.column_config.TextColumn(width="small"),
                             "Amount": st.column_config.NumberColumn(format="$%,.2f"),
                             "Prepaid": st.column_config.TextColumn(width="small"),
                         })
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

    # Bank reconciliation
    if result.gl_bank_matches:
        st.markdown("### Bank Reconciliation")

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
        if "accrual_je" in st.session_state.output_files:
            je_path = st.session_state.output_files["accrual_je"]
            if os.path.exists(je_path):
                with open(je_path, "rb") as f:
                    st.download_button(
                        label="📝 Download Accrual JE Import",
                        data=f.read(),
                        file_name=f"GA_Accrual_JE_Import_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

    with col4:
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
    1. Upload at least the **Yardi GL Detail** file (required)
    2. Upload any additional data files you have available
    3. Click **Run Pipeline** to process and generate reports
    4. Results and downloadable reports will appear below
    """, icon="ℹ️")


# ── Cleanup on session end ───────────────────────────────────
def cleanup_on_exit():
    """Clean up temporary files."""
    if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
        shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)


import atexit
atexit.register(cleanup_on_exit)
