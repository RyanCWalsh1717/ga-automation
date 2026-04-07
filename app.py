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
from accrual_entry_generator import build_accrual_entries, generate_yardi_je_import
from variance_comments import generate_variance_comments


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
    "income_statement": ("Yardi Income Statement (.xlsx)", "*.xlsx", False),
    "budget_comparison": ("Yardi Budget Comparison (.xlsx)", "*.xlsx", False),
    "rent_roll": ("Yardi Rent Roll (.xlsx)", "*.xlsx", False),
    "nexus_accrual": ("Nexus Accrual Detail (.xls)", "*.xls", False),
    "pnc_bank": ("PNC Bank Statement (.pdf)", "*.pdf", False),
    "loan": ("Berkadia Loan Statement (.xlsx)", "*.xlsx", False),
    "kardin_budget": ("Kardin Budget (.xlsx)", "*.xlsx", False),
    "monthly_report": ("Monthly Report Template (.xlsx)", "*.xlsx", False),
}

for key, (label, file_type, required) in file_config.items():
    col1, col2 = st.sidebar.columns([5, 1])

    with col1:
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

            engine_result = run_pipeline(files_dict)
            st.session_state.engine_result = engine_result

            # Step 2: Generate main report
            status_text.text("Step 2/3: Generating monthly report...")
            progress_bar.progress(60)

            report_path = os.path.join(st.session_state.temp_dir, "GA_Monthly_Report.xlsx")
            generate_report(engine_result, report_path)
            st.session_state.output_files["monthly_report"] = report_path

            # Step 3: Generate workpapers
            status_text.text("Step 3/4: Generating workpapers...")
            progress_bar.progress(70)

            workpaper_path = os.path.join(st.session_state.temp_dir, "GA_Workpapers.xlsx")
            generate_workpapers(engine_result, workpaper_path)
            st.session_state.output_files["workpapers"] = workpaper_path

            # Step 4: Generate accrual JE import
            status_text.text("Step 4/5: Generating accrual entries...")
            progress_bar.progress(80)

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
            if je_lines:
                    je_path = os.path.join(st.session_state.temp_dir, "GA_Accrual_JE_Import.xlsx")
                    generate_yardi_je_import(
                        je_lines, je_path,
                        period=engine_result.period or '',
                        property_name=engine_result.property_name or '',
                    )
                    st.session_state.output_files["accrual_je"] = je_path

            # Step 5: Generate variance comments
            status_text.text("Step 5/6: Generating variance comments...")
            progress_bar.progress(88)

            api_key = None
            try:
                api_key = st.secrets.get("ANTHROPIC_API_KEY")
            except Exception:
                pass  # No secrets configured

            if engine_result.budget_variances:
                var_comments = generate_variance_comments(engine_result, api_key=api_key)
                st.session_state.output_files["variance_comments"] = var_comments
            else:
                st.session_state.output_files["variance_comments"] = []

            # Step 6: Generate exception report
            status_text.text("Step 6/6: Generating validation report...")
            progress_bar.progress(95)

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
        status_text = "➠️ WARNINGS"
    else:  # ERRORS
        status_color = "#e74c3c"
        status_text = "❌ ERRORS"

    st.markdown(f"""
    <div style="background-color: {status_color}20; border-left: 5px solid {status_color}; padding: 15px; border-radius: 5px; margin: 15px 0;">
        <h3 style="color: {status_color}; margin: 0;">{status_text}</h3>
    </div>
    """, unsafe_allow_html=True)

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
        if "workpapers" in st.ses
