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
    build_accrual_entries, generate_yardi_je_csv, generate_etl_csv,
    build_prepaid_amortization, build_prepaid_release_je,
    check_prior_accrual_vs_actual,
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
from mgmt_fee_invoice import generate_invoice as generate_mgmt_fee_invoice
from audit_trail_generator import generate_audit_trail


# ── Data directory + property discovery ──────────────────────────────────────
_DATA_DIR = Path(__file__).parent / "data"

def _committed_path(prop_code: str, filename: str) -> Optional[str]:
    """Return path to a committed reference file if it exists, else None."""
    p = _DATA_DIR / prop_code / filename
    return str(p) if p.exists() else None

def _save_checklist_now() -> None:
    """Persist current close_tracker + custom items to GitHub/local."""
    try:
        from checklist_persistence import save_checklist, session_to_state, period_to_key
        _code = st.session_state.get('active_property_code', 'revlabspm')
        _pkey = st.session_state.get('checklist_period_key', current_period_key())
        _state = session_to_state(
            st.session_state.close_tracker,
            st.session_state.get('custom_checklist_items', []),
            _code, _pkey,
        )
        save_checklist(_code, _pkey, _state, str(_DATA_DIR))
    except Exception:
        pass   # persistence is best-effort; never crash the app


def _discover_properties() -> list[dict]:
    """
    Scan data/ for subfolders with a config.yaml.
    Returns list of {'code', 'display_name', 'address', 'cfg'} dicts.
    Falls back to hardcoded RevLabs entry if none found.
    """
    from property_config import discover_properties as _disc
    props = _disc(str(_DATA_DIR))
    if not props:
        # Fallback so the app always has at least one property
        from property_config import load_property_config
        cfg = load_property_config('revlabspm', str(_DATA_DIR))
        props = [{'code': 'revlabspm', 'display_name': cfg.display(),
                  'address': cfg.property_address, 'cfg': cfg}]
    return props


# ── Page configuration ───────────────────────────────────────
st.set_page_config(
    page_title="GA Close | GRP",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Brand palette ── */
    :root {
        --grp-green:     #1A5C22;
        --grp-green-mid: #2E7D32;
        --grp-green-lit: #E8F5E9;
        --success-color: #2E7D32;
        --warning-color: #E65100;
        --error-color:   #B71C1C;
        --info-color:    #1565C0;
        --text-primary:  #212121;
        --text-muted:    #616161;
        --border:        #E0E0E0;
        --bg-alt:        #F9FAFB;
    }

    /* ── Hero banner ── */
    .grp-hero {
        background: linear-gradient(135deg, var(--grp-green) 0%, var(--grp-green-mid) 100%);
        border-radius: 10px;
        padding: 0;
        overflow: hidden;
        margin-bottom: 18px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.18);
        display: flex;
        align-items: stretch;
        min-height: 110px;
    }
    .grp-hero-photo {
        width: 260px;
        min-width: 260px;
        object-fit: cover;
        border-radius: 10px 0 0 10px;
        display: block;
    }
    .grp-hero-body {
        padding: 20px 28px;
        flex: 1;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .grp-hero-title {
        color: #ffffff;
        font-size: 1.55rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        margin: 0 0 4px 0;
        line-height: 1.2;
    }
    .grp-hero-sub {
        color: #C8E6C9;
        font-size: 0.88rem;
        margin: 0 0 10px 0;
        font-weight: 400;
    }
    .grp-hero-badges {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 4px;
    }
    .grp-badge {
        background: rgba(255,255,255,0.18);
        border: 1px solid rgba(255,255,255,0.35);
        border-radius: 20px;
        padding: 3px 12px;
        color: #ffffff;
        font-size: 0.75rem;
        font-weight: 500;
        white-space: nowrap;
    }
    .grp-hero-logo {
        padding: 20px 24px;
        display: flex;
        align-items: center;
        justify-content: flex-end;
        min-width: 160px;
    }
    .grp-logo-text {
        color: rgba(255,255,255,0.85);
        font-size: 0.7rem;
        text-align: right;
        line-height: 1.4;
        font-weight: 500;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }

    /* ── Sidebar property card ── */
    .grp-sidebar-card {
        background: linear-gradient(135deg, var(--grp-green) 0%, var(--grp-green-mid) 100%);
        border-radius: 8px;
        padding: 14px 16px;
        margin-bottom: 12px;
        color: white;
    }
    .grp-sidebar-prop {
        font-size: 0.82rem;
        font-weight: 700;
        color: #ffffff;
        margin: 0 0 2px 0;
        letter-spacing: 0.03em;
    }
    .grp-sidebar-addr {
        font-size: 0.72rem;
        color: #C8E6C9;
        margin: 0;
    }

    /* ── Section headers ── */
    .grp-section {
        color: var(--grp-green);
        font-size: 1.05rem;
        font-weight: 700;
        border-bottom: 2px solid var(--grp-green-lit);
        padding-bottom: 4px;
        margin: 18px 0 10px 0;
    }

    /* ── Status pills ── */
    .status-clean    { color: var(--success-color); font-weight: 600; }
    .status-warnings { color: var(--warning-color); font-weight: 600; }
    .status-errors   { color: var(--error-color);   font-weight: 600; }

    /* ── Exception cards ── */
    .exception-error {
        background-color: #FFEBEE;
        border-left: 4px solid var(--error-color);
        padding: 10px 14px; margin: 8px 0; border-radius: 5px;
    }
    .exception-warning {
        background-color: #FFF8E1;
        border-left: 4px solid var(--warning-color);
        padding: 10px 14px; margin: 8px 0; border-radius: 5px;
    }
    .exception-info {
        background-color: #E3F2FD;
        border-left: 4px solid var(--info-color);
        padding: 10px 14px; margin: 8px 0; border-radius: 5px;
    }

    /* ── Metric cards ── */
    .metric-card {
        background: #ffffff;
        padding: 16px 20px;
        border-radius: 8px;
        border-left: 4px solid var(--grp-green);
        box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    }

    /* ── Streamlit element tweaks ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 6px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        font-weight: 500;
    }
    div[data-testid="stSidebarContent"] {
        padding-top: 12px;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state initialization ─────────────────────────────
# Shared
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}
if "temp_dir" not in st.session_state:
    st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")
if "active_property_code" not in st.session_state:
    # Default to first discovered property
    _init_props = _discover_properties()
    st.session_state.active_property_code = _init_props[0]['code'] if _init_props else 'revlabspm'
if "_prev_active_property_code" not in st.session_state:
    st.session_state._prev_active_property_code = st.session_state.active_property_code

# Pass 1 — JE Generation
if "pass1_complete" not in st.session_state:
    st.session_state.pass1_complete = False
if "pass1_engine_result" not in st.session_state:
    st.session_state.pass1_engine_result = None
if "pass1_output_files" not in st.session_state:
    st.session_state.pass1_output_files = {}
if "pass1_run_count" not in st.session_state:
    st.session_state.pass1_run_count = 0
if "upload_key_p1" not in st.session_state:
    st.session_state.upload_key_p1 = 0
if "tub_key" not in st.session_state:
    st.session_state.tub_key = 0

# Pass 2 — Report Generation
if "pass2_complete" not in st.session_state:
    st.session_state.pass2_complete = False
if "pass2_engine_result" not in st.session_state:
    st.session_state.pass2_engine_result = None
if "pass2_output_files" not in st.session_state:
    st.session_state.pass2_output_files = {}
if "upload_key_p2" not in st.session_state:
    st.session_state.upload_key_p2 = 0

# Audit trail & sign-off
if "prepared_by" not in st.session_state:
    st.session_state.prepared_by = "Ryan Walsh"
if "signoff_state" not in st.session_state:
    st.session_state.signoff_state = {}
if "close_tracker" not in st.session_state:
    st.session_state.close_tracker = {}
if "confirm_reset_all" not in st.session_state:
    st.session_state.confirm_reset_all = False
if "confirm_reset_p2" not in st.session_state:
    st.session_state.confirm_reset_p2 = False

# Dashboard checklist persistence
from checklist_persistence import (
    current_period_key, period_key_to_label, period_to_key,
    load_checklist, save_checklist, session_to_state, state_to_session,
)
if "checklist_period_key" not in st.session_state:
    st.session_state.checklist_period_key = current_period_key()
if "custom_checklist_items" not in st.session_state:
    st.session_state.custom_checklist_items = []
if "checklist_loaded" not in st.session_state:
    st.session_state.checklist_loaded = False

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

import pandas as pd  # needed for manual_accruals_df init and stale-session reset
if "manual_accruals_df" not in st.session_state:
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

# If session has stale columns from an older version, reset the whole table so
# pre-seeded Vendor/Description text is also cleared.
if any(_col in st.session_state.manual_accruals_df.columns for _col in ("CR Account", "Auto-Reverse")):
    _n = 11
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


# ── Image asset loader ────────────────────────────────────────
import base64 as _b64

def _img_b64(fname: str) -> str | None:
    """Load an image from assets/ and return a base64 data-URI, or None."""
    _path = os.path.join(os.path.dirname(__file__), 'assets', fname)
    if os.path.exists(_path):
        with open(_path, 'rb') as _f:
            _raw = _b64.b64encode(_f.read()).decode()
        _ext = fname.rsplit('.', 1)[-1].lower()
        _mime = {'png': 'image/png', 'jpg': 'image/jpeg',
                 'jpeg': 'image/jpeg', 'svg': 'image/svg+xml'}.get(_ext, 'image/png')
        return f'data:{_mime};base64,{_raw}'
    return None

_LOGO_SRC   = _img_b64('grp_logo.png') or _img_b64('grp_logo.svg')

def _prop_hero_src(property_code: str) -> Optional[str]:
    """
    Return a base64 data-URI for the property's hero photo.
    Checks data/{property_code}/hero.jpg|png|webp first,
    then falls back to assets/{property_code}_hero.jpg,
    then to the generic revlabs_hero fallback.
    """
    import base64 as _b64mod
    _exts = ['.jpg', '.jpeg', '.png', '.webp']
    # 1. Property-specific file in data dir
    for _ext in _exts:
        _p = _DATA_DIR / property_code / f'hero{_ext}'
        if _p.exists():
            _raw = _b64mod.b64encode(_p.read_bytes()).decode()
            _mime = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
                     'png': 'image/png', 'webp': 'image/webp'}.get(_ext.lstrip('.'), 'image/jpeg')
            return f'data:{_mime};base64,{_raw}'
    # 2. Assets folder named after the property
    for _ext in _exts:
        _src = _img_b64(f'{property_code}_hero{_ext}')
        if _src:
            return _src
    # 3. Legacy RevLabs fallback
    return _img_b64('revlabs_hero.jpg') or _img_b64('revlabs_hero.png')

_HERO_SRC = _prop_hero_src(st.session_state.get('active_property_code', 'revlabspm'))

# ── Hero banner ───────────────────────────────────────────────
_hero_alt = st.session_state.get('active_property_code', 'Property')
_photo_html = (
    f'<img src="{_HERO_SRC}" class="grp-hero-photo" alt="{_hero_alt}"/>'
    if _HERO_SRC else ''
)
_logo_html = (
    f'<img src="{_LOGO_SRC}" style="max-width:140px;max-height:60px;" alt="GRP Logo"/>'
    if _LOGO_SRC else
    '<div class="grp-logo-text">Greatland<br>Realty<br>Partners</div>'
)

# Load the active property config for the hero banner
# (_active_cfg is set in the sidebar section below, but on first render we
# need it here too — load it again; it's cheap and cached by the YAML file.)
from property_config import load_property_config as _lpc_hero
_hero_cfg = _lpc_hero(st.session_state.get('active_property_code', 'revlabspm'), str(_DATA_DIR))

_hero_title = f"{_hero_cfg.display()} Monthly Close"
_hero_sub   = ' &nbsp;|&nbsp; '.join(filter(None, [
    _hero_cfg.property_address,
    f"Managed by {_hero_cfg.management_company} for {_hero_cfg.investor_name}"
    if _hero_cfg.management_company and _hero_cfg.investor_name else
    (_hero_cfg.management_company or _hero_cfg.investor_name),
]))
_hero_badges = [f"🏢 {_hero_cfg.property_code}"]
if _hero_cfg.property_size_sf:
    _hero_badges.append(f"📐 ~{_hero_cfg.property_size_sf:,} SF")
if _hero_cfg.property_type:
    _hero_badges.append(f"🔬 {_hero_cfg.property_type}")
_hero_badge_html = " ".join(f'<span class="grp-badge">{b}</span>' for b in _hero_badges)

st.markdown(f"""
<div class="grp-hero">
    {_photo_html}
    <div class="grp-hero-body">
        <div class="grp-hero-title">{_hero_title}</div>
        <div class="grp-hero-sub">{_hero_sub}</div>
        <div class="grp-hero-badges">{_hero_badge_html}</div>
    </div>
    <div class="grp-hero-logo">{_logo_html}</div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ──────────────────────────────────────────────────────────────────
prior_period_outstanding = 0.0  # Yardi Bank Rec PDF includes all outstanding items

# ── Property selector ────────────────────────────────────────────────────────
_all_props   = _discover_properties()
_prop_codes  = [p['code'] for p in _all_props]
_prop_labels = {p['code']: f"{p['display_name']}  ({p['code']})" for p in _all_props}

if len(_all_props) > 1:
    _selected_code = st.sidebar.selectbox(
        "🏢 Property",
        options=_prop_codes,
        index=_prop_codes.index(st.session_state.active_property_code)
              if st.session_state.active_property_code in _prop_codes else 0,
        format_func=lambda c: _prop_labels.get(c, c),
        key="active_property_code",
        help="Select the property to run the close for. Switching resets all pipeline state.",
    )
else:
    _selected_code = _prop_codes[0] if _prop_codes else 'revlabspm'
    st.session_state.active_property_code = _selected_code

# Detect property change — reset pipeline state so stale results don't carry over
if st.session_state.get('_prev_active_property_code') != _selected_code:
    st.session_state._prev_active_property_code = _selected_code
    st.session_state.pass1_complete        = False
    st.session_state.pass1_engine_result   = None
    st.session_state.pass1_output_files    = {}
    st.session_state['pass1_gl_activity_log'] = []
    st.session_state.pass2_complete        = False
    st.session_state.pass2_engine_result   = None
    st.session_state.pass2_output_files    = {}
    st.session_state.uploaded_files        = {}
    # Reset checklist so the new property's data is loaded
    st.session_state.checklist_loaded = False
    st.session_state.close_tracker = {}
    st.session_state.custom_checklist_items = []

# Load config for the selected property
from property_config import load_property_config as _load_prop_cfg
_active_cfg = _load_prop_cfg(_selected_code, str(_DATA_DIR))

# Committed Kardin budget for the active property
_COMMITTED_BUDGET = _committed_path(
    _active_cfg.property_code,
    _active_cfg.kardin_budget_file or 'GA_Kardin_Budget_FY2026.xlsx',
)

# Sidebar property card
_sb_logo = (
    f'<img src="{_LOGO_SRC}" style="max-width:120px;max-height:44px;margin-bottom:8px;display:block;" alt="GRP"/>'
    if _LOGO_SRC else ''
)
_sb_addr_line = _active_cfg.property_address or ''
_sb_mgmt_line = ' · '.join(filter(None, [
    _active_cfg.management_company, _active_cfg.investor_name
]))
st.sidebar.markdown(f"""
<div class="grp-sidebar-card">
    {_sb_logo}
    <div class="grp-sidebar-prop">{_active_cfg.display()} — {_active_cfg.property_code}</div>
    <div class="grp-sidebar-addr">{_sb_addr_line}<br>{_sb_mgmt_line}</div>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.session_state.prepared_by = st.sidebar.text_input(
    "Prepared by",
    value=st.session_state.prepared_by,
    help="Stamped on every workpaper tab and the run log.",
)

if not st.session_state.confirm_reset_all:
    if st.sidebar.button("🔄 Reset All", use_container_width=True,
                         help="Clear all results and uploaded files"):
        st.session_state.confirm_reset_all = True
        st.rerun()
else:
    st.sidebar.warning("⚠️ This will wipe **all** uploads, results, and sign-offs.")
    _ra_col1, _ra_col2 = st.sidebar.columns(2)
    if _ra_col1.button("✅ Confirm", use_container_width=True, key="confirm_reset_all_btn"):
        st.session_state.confirm_reset_all = False
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
        st.session_state.bulk_overrides_wp = {}
        st.session_state.signoff_state = {}
        st.session_state.close_tracker = {}
        st.session_state.upload_key_p1 += 1
        st.session_state.upload_key_p2 += 1
        shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
        st.session_state.temp_dir = tempfile.mkdtemp(prefix="ga_automation_")
        import pandas as _pd
        st.session_state.post_close_je_df = _pd.DataFrame({
            "JE #": ["PC-001", "PC-001"], "Description": ["", ""],
            "Account Code": ["", ""],
            "Debit ($)": [0.0, 0.0], "Credit ($)": [0.0, 0.0],
            "Line Description": ["", ""],
        })
        st.session_state.pop("_pcje_latest", None)
        if "manual_accruals_df" in st.session_state:
            st.session_state.manual_accruals_df["Amount ($)"] = 0.0
        st.session_state.tub_key += 1   # forces TUB number inputs to re-render at $0
        st.session_state.custom_checklist_items = []
        st.session_state.checklist_loaded = False
        st.rerun()
    if _ra_col2.button("❌ Cancel", use_container_width=True, key="cancel_reset_all_btn"):
        st.session_state.confirm_reset_all = False
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
        "Enables historical pattern accruals (Layer 3) and variance commentary. "
        "Without it: only Nexus invoice and invoice-proration accruals generated; no variance comments.",
    ),
    "kardin_budget": (
        "Kardin 2026 Budget (.xlsx)", "xlsx", False, "core",
        "Enables QC YTD budget vs Kardin cross-check AND payroll bonus accruals (Layer 4 — "
        "used as fallback when no annual bonus is entered in the Bonus Accrual expander). "
        "Without it: QC budget check and Kardin-derived bonus accruals skipped.",
    ),
    "t12_statement": (
        "12-Month Income Statement (.xlsx)", "xlsx", False, "core",
        "Powers MoM Swings tab (Tab 4) in Pass 2 QC workbook with real prior-month actuals "
        "instead of derived YTD-PTD. Critical for January (prior month = December actuals). "
        "Also improves Layer 3 historical accrual detection in Pass 1.",
    ),
    "nexus_accrual": (
        "Nexus Invoice Detail (.xls / .xlsx)", ["xls", "xlsx"], False, "core",
        "Enables AP accrual detection (Layer 1 — open invoices not yet posted to GL). "
        "Without it: invoice-proration (Layer 2) and historical (Layer 3) still run.",
    ),
    # ── Bank ──────────────────────────────────────────────────
    "bank_rec": (
        "Yardi Bank Rec PDF — Operating (.pdf)", "pdf", False, "bank",
        "PREFERRED bank source. Reads Yardi's pre-computed reconciliation: bank balance, "
        "outstanding checks, reconciled balance, and $0 difference. Enables Operating bank "
        "rec tab in the BS workpaper (PNC x3993 vs GL 111100). Without it: no bank rec tab.",
    ),
    "receivable_summary": (
        "Yardi Receivable Summary (.xlsx)", "xlsx", False, "bank",
        "PRIMARY management fee basis — explicit Prepayment row gives the cleanest prepayment exclusion. "
        "Export from Yardi after bank rec is complete. Takes priority over Receivable Detail when both are uploaded. "
        "Without it: falls back to Receivable Detail (if uploaded) or DACA additions.",
    ),
    "receivable_detail": (
        "Yardi Receivable Detail (.xlsx)", "xlsx", False, "bank",
        "ALTERNATE management fee basis — JLL's exact method. Export from Yardi after bank rec is complete. "
        "Pair with the AR Detail Aging for accurate prepayment exclusion. "
        "Without it: falls back to DACA additions. (Receivable Summary is preferred when available.)",
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
    "bank_rec_dev_xlsx": (
        "Yardi Development Bank Rec — 111210 (.xlsx)", "xlsx", False, "bank",
        "Yardi Bank Reconciliation Report for the BofA development account (x3132). "
        "Copies the raw Yardi export directly into the '111210 Cash - Development' tab "
        "in the workpaper — matching the exact format of the 111100 PNC and 115100 DACA tabs. "
        "Without it: tab is generated from the BofA PDF (ending balance only, no detail).",
    ),
    "capital_schedule": (
        "Capital Accounts Schedule (.xlsx)", "xlsx", False, "reference",
        "Capital improvement schedules (154500 Building Improvements, 181200 LC, "
        "181300 Legal, 181400 TI). Upload the monthly Rev Labs capital schedule. "
        "Drives the 4 capital account tabs in the workpaper. "
        "Without it: capital tabs show GL transactions only.",
    ),
    "capital_seed": (
        "Capital Schedule Seed (.xlsx)", "xlsx", False, "reference",
        "January 2026 seed file (Book3.xlsx) — all 7 capital accounts "
        "(152100 Land, 154100 Building, 154500 Bldg Improvements, 171100 CIP, "
        "181200 LC, 181300 Legal, 181400 TI). "
        "Used only when no Capital Accounts Schedule is uploaded. "
        "From February onward the prior workpaper carry-forward supersedes this.",
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
    "nexus_accrual", "bank_rec", "receivable_summary", "receivable_detail", "ar_aging",
    "bank_rec_dev", "capital_schedule", "capital_seed", "daca_bank", "loan",
    "prepaid_ledger", "unknown",
]
_P1_SLOT_LABELS = [_FILE_LABELS.get(k, k) for k in _P1_SLOT_KEYS]


# ═══════════════════════════════════════════════════════════════
# ── Main content: Two-pass tabs ──────────────────────────────
# ═══════════════════════════════════════════════════════════════
import pandas as pd

tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "🏠  Dashboard",
    "📋  Pass 1 — Generate JEs",
    "📊  Pass 2 — Generate Reports & JEs",
    "📖  How to Use",
    "⚙️  Properties",
])


# ══════════════════════════════════════════════════════════════
# TAB 0 — DASHBOARD / CLOSE CHECKLIST
# ══════════════════════════════════════════════════════════════
with tab0:

    # ── Load checklist from GitHub once per session / property ────────────────
    _ck_prop   = st.session_state.get('active_property_code', 'revlabspm')
    _ck_pkey   = st.session_state.checklist_period_key
    if not st.session_state.get('checklist_loaded', False):
        try:
            _ck_state = load_checklist(_ck_prop, _ck_pkey, str(_DATA_DIR))
            _ct_loaded, _ci_loaded = state_to_session(_ck_state)
            # Merge: don't overwrite steps already set by auto-detect in Pass 1/2
            for _sk, _sv in _ct_loaded.items():
                if _sk not in st.session_state.close_tracker:
                    st.session_state.close_tracker[_sk] = _sv
            if not st.session_state.custom_checklist_items:
                st.session_state.custom_checklist_items = _ci_loaded
        except Exception:
            pass
        st.session_state.checklist_loaded = True

    from close_tracker_generator import CLOSE_TRACKER_STEPS as _CTS

    # ── Top control bar ───────────────────────────────────────────────────────
    _ck_col_name, _ck_col_period, _ck_col_prog = st.columns([2, 2, 4])

    with _ck_col_name:
        _team_names = (_active_cfg.team_members
                       if _active_cfg.team_members
                       else ['Ryan Walsh', 'Natasha Parker', 'Lauren Sullivan'])
        _cur_name   = st.session_state.get('prepared_by', _team_names[0])
        _name_idx   = _team_names.index(_cur_name) if _cur_name in _team_names else 0
        _chosen_name = st.selectbox(
            "👤 I am",
            _team_names,
            index=_name_idx,
            key='dashboard_user_name',
        )
        if _chosen_name != st.session_state.prepared_by:
            st.session_state.prepared_by = _chosen_name

    with _ck_col_period:
        # Build a list of the last 3 months + next month for selector
        from datetime import date
        _today = date.today()
        _period_options = []
        for _mo_offset in range(-2, 2):
            _mo = (_today.month - 1 + _mo_offset) % 12 + 1
            _yr = _today.year + ((_today.month - 1 + _mo_offset) // 12)
            _period_options.append(f'{_yr}_{_mo:02d}')
        if _ck_pkey not in _period_options:
            _period_options.append(_ck_pkey)
        _period_labels_map = {k: period_key_to_label(k) for k in _period_options}
        _period_sel_idx = _period_options.index(_ck_pkey) if _ck_pkey in _period_options else 0
        _period_chosen = st.selectbox(
            "📅 Close Period",
            _period_options,
            index=_period_sel_idx,
            format_func=lambda k: _period_labels_map[k],
            key='dashboard_period_sel',
        )
        if _period_chosen != st.session_state.checklist_period_key:
            st.session_state.checklist_period_key = _period_chosen
            st.session_state.checklist_loaded = False
            st.rerun()

    with _ck_col_prog:
        _n_steps   = len(_CTS)
        _n_done    = sum(1 for i in range(_n_steps) if i in st.session_state.close_tracker)
        _n_custom  = len(st.session_state.custom_checklist_items)
        _n_cdone   = sum(1 for c in st.session_state.custom_checklist_items if c.get('completed'))
        _total_all = _n_steps + _n_custom
        _done_all  = _n_done + _n_cdone
        _pct = int(100 * _done_all / _total_all) if _total_all else 0
        st.markdown(f"**{period_key_to_label(_ck_pkey)} Close**")
        st.progress(_pct)
        _pct_color = '#2E7D32' if _pct == 100 else '#1565C0'
        st.markdown(
            f"<span style='font-size:0.85rem;color:{_pct_color};font-weight:600;'>"
            f"{_done_all} / {_total_all} tasks complete ({_pct}%)</span>",
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Step checklist ─────────────────────────────────────────────────────────
    # Group into Pre-Close / Post-Close
    _PHASE_PRE  = list(range(0, 5))   # steps 0–4
    _PHASE_POST = list(range(5, 9))   # steps 5–8

    def _render_phase(phase_label: str, step_indices: list) -> None:
        st.markdown(
            f"<div style='font-size:0.78rem;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:0.08em;color:#757575;margin:8px 0 4px 0;'>{phase_label}</div>",
            unsafe_allow_html=True,
        )
        for _si, _sdesc, _stype in [s for s in _CTS if s[0] in step_indices]:
            _ct_entry = st.session_state.close_tracker.get(_si)
            _is_done  = bool(_ct_entry)
            _icon     = '✅' if _is_done else ('🔄' if _stype == 'auto' else '⬜')
            _by       = _ct_entry.get('completed_by', '') if _ct_entry else '—'
            _ts       = _ct_entry.get('timestamp', '') if _ct_entry else '—'
            _auto_tag = ' *(auto)*' if (_ct_entry and _ct_entry.get('auto')) else ''

            _sc1, _sc2, _sc3, _sc4, _sc5 = st.columns([0.4, 3.5, 1.8, 1.8, 1.5])
            with _sc1:
                st.markdown(f"<div style='font-size:1.1rem;padding-top:4px'>{_icon}</div>",
                            unsafe_allow_html=True)
            with _sc2:
                _color = '#212121' if _is_done else '#616161'
                _weight = '500' if _is_done else '400'
                st.markdown(
                    f"<div style='color:{_color};font-weight:{_weight};"
                    f"font-size:0.88rem;padding-top:6px;'>{_sdesc}{_auto_tag}</div>",
                    unsafe_allow_html=True,
                )
            with _sc3:
                st.markdown(
                    f"<div style='font-size:0.82rem;color:#616161;padding-top:6px;'>{_by}</div>",
                    unsafe_allow_html=True,
                )
            with _sc4:
                st.markdown(
                    f"<div style='font-size:0.82rem;color:#616161;padding-top:6px;'>{_ts}</div>",
                    unsafe_allow_html=True,
                )
            with _sc5:
                if _stype == 'manual' and not _is_done:
                    if st.button('Mark Complete', key=f'ck_mark_{_si}',
                                 use_container_width=True):
                        st.session_state.close_tracker[_si] = {
                            'completed_by': st.session_state.prepared_by,
                            'timestamp':    datetime.now().strftime('%m/%d/%Y %H:%M'),
                            'auto':         False,
                        }
                        _save_checklist_now()
                        st.rerun()
                elif _is_done and _stype == 'manual':
                    if st.button('↩ Undo', key=f'ck_undo_{_si}',
                                 use_container_width=True):
                        st.session_state.close_tracker.pop(_si, None)
                        _save_checklist_now()
                        st.rerun()
                else:
                    st.markdown('')   # spacer

    _render_phase('Pre-Close', _PHASE_PRE)
    _render_phase('Post-Close', _PHASE_POST)

    st.divider()

    # ── Custom (one-off) tasks ─────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:0.78rem;font-weight:700;text-transform:uppercase;"
        "letter-spacing:0.08em;color:#757575;margin:8px 0 4px 0;'>Custom Tasks "
        "This Close</div>",
        unsafe_allow_html=True,
    )

    for _ci_idx, _ci in enumerate(st.session_state.custom_checklist_items):
        _ci_done = _ci.get('completed', False)
        _ci_icon = '✅' if _ci_done else '⬜'
        _ci_by   = _ci.get('completed_by', '—') if _ci_done else _ci.get('created_by', '—')
        _ci_ts   = _ci.get('completed_at', '—') if _ci_done else f"added {_ci.get('created_at','')}"

        _cc1, _cc2, _cc3, _cc4, _cc5 = st.columns([0.4, 3.5, 1.8, 1.8, 1.5])
        with _cc1:
            st.markdown(f"<div style='font-size:1.1rem;padding-top:4px'>{_ci_icon}</div>",
                        unsafe_allow_html=True)
        with _cc2:
            _ci_color = '#212121' if _ci_done else '#616161'
            st.markdown(
                f"<div style='color:{_ci_color};font-size:0.88rem;padding-top:6px;'>"
                f"{_ci['label']}</div>",
                unsafe_allow_html=True,
            )
        with _cc3:
            st.markdown(
                f"<div style='font-size:0.82rem;color:#616161;padding-top:6px;'>{_ci_by}</div>",
                unsafe_allow_html=True,
            )
        with _cc4:
            st.markdown(
                f"<div style='font-size:0.82rem;color:#616161;padding-top:6px;'>{_ci_ts}</div>",
                unsafe_allow_html=True,
            )
        with _cc5:
            if not _ci_done:
                if st.button('Mark Complete', key=f'ci_mark_{_ci_idx}',
                             use_container_width=True):
                    st.session_state.custom_checklist_items[_ci_idx]['completed']    = True
                    st.session_state.custom_checklist_items[_ci_idx]['completed_by'] = (
                        st.session_state.prepared_by)
                    st.session_state.custom_checklist_items[_ci_idx]['completed_at'] = (
                        datetime.now().strftime('%m/%d/%Y %H:%M'))
                    _save_checklist_now()
                    st.rerun()
            else:
                if st.button('↩ Undo', key=f'ci_undo_{_ci_idx}',
                             use_container_width=True):
                    st.session_state.custom_checklist_items[_ci_idx]['completed']    = False
                    st.session_state.custom_checklist_items[_ci_idx]['completed_by'] = None
                    st.session_state.custom_checklist_items[_ci_idx]['completed_at'] = None
                    _save_checklist_now()
                    st.rerun()

    # ── Add custom task ────────────────────────────────────────────────────────
    with st.expander('➕  Add a custom task for this close', expanded=False):
        _new_label = st.text_input('Task description', key='new_custom_task_label',
                                   placeholder='e.g. Confirm Berkadia loan pay-off')
        if st.button('Add Task', key='add_custom_task_btn', type='primary'):
            if _new_label.strip():
                _new_id = f'custom_{len(st.session_state.custom_checklist_items)}'
                st.session_state.custom_checklist_items.append({
                    'id':           _new_id,
                    'label':        _new_label.strip(),
                    'created_by':   st.session_state.prepared_by,
                    'created_at':   datetime.now().strftime('%m/%d/%Y %H:%M'),
                    'completed':    False,
                    'completed_by': None,
                    'completed_at': None,
                })
                _save_checklist_now()
                st.rerun()
            else:
                st.warning('Please enter a task description.')


# ──────────────────────────────────────────────────────────────
# TAB 1 — PASS 1: JE GENERATION
# ──────────────────────────────────────────────────────────────
with tab1:
    st.markdown(
        "<div style='background:var(--grp-green-lit,#E8F5E9);border-left:4px solid #1A5C22;"
        "border-radius:5px;padding:8px 16px;margin-bottom:14px;font-size:0.85rem;color:#1A5C22;'>"
        "⬤ &nbsp;<strong>Pass 1 — Pre-Close</strong>&nbsp; Upload source files, review accruals, "
        "download JE CSVs → post to Yardi → run close."
        "</div>",
        unsafe_allow_html=True,
    )

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
        key=f"bulk_upload_p1_{st.session_state.upload_key_p1}",
        label_visibility="collapsed",
    )

    # Clear all Pass 1 slots so stale entries don't persist after a file is removed
    for _clr_key in set(_P1_SLOT_KEYS) - {"unknown"}:
        st.session_state.uploaded_files.pop(_clr_key, None)

    if _bulk_p1:
        _loan_paths_p1: list = []

        for _uf in _bulk_p1:
            _raw = bytes(_uf.getbuffer())
            _det_key, _conf, _det_label = _classify_file(_uf.name, _raw, pass2=False, property_config=_active_cfg)
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
    if "budget_comparison" not in uploaded_keys: missing_impact.append("No historical pattern accruals or variance comments")
    if "bank_rec"          not in uploaded_keys: missing_impact.append("No Operating bank rec tab (Pass 2)")
    if "daca_bank"         not in uploaded_keys: missing_impact.append("No DACA bank rec tab (Pass 2)")
    if "loan"              not in uploaded_keys: missing_impact.append("No debt service tab (Pass 2)")

    # Count all Pass 1 uploaded files — exclude Pass 2-only keys so the sidebar
    # count reflects exactly what was dropped in the bulk uploader.
    # List values (e.g. loan with 3 Berkadia PDFs) are unfolded so each PDF counts.
    _P2_ONLY_KEYS = {
        "gl_pass2", "budget_comparison_pass2", "trial_balance_pass2",
        "t12_statement_pass2", "loan_pass2", "bank_rec_pass2",
        "prior_workpaper", "run_log", "ap_aging_pass2",
        "bank_rec_xlsx_pass2", "daca_bank_xlsx_pass2",
        "prepaid_ledger_pass2", "capital_seed_pass2",
    }
    uploaded_count = sum(
        len(v) if isinstance(v, list) else 1
        for k, v in st.session_state.uploaded_files.items()
        if k not in _P2_ONLY_KEYS and k != "unknown" and v is not None
    )
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

    # ── Committed reference file status ───────────────────────────────────────
    if _COMMITTED_BUDGET:
        _budget_uploaded = "kardin_budget" in uploaded_keys
        if _budget_uploaded:
            st.caption("📊 **Kardin Budget:** Using uploaded file _(overrides committed)_")
        else:
            st.caption("📊 **Kardin Budget:** FY2026 on file — no upload needed")
    else:
        if "kardin_budget" not in uploaded_keys:
            st.caption("📊 **Kardin Budget:** Not uploaded — bonus accruals and QC budget check skipped")

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
                    key=f"tub_elec_{_tkey}_{st.session_state.tub_key}",
                )
                _tgas = st.number_input(
                    "Gas ($)", min_value=0.0, value=0.0, step=1.0, format="%.2f",
                    key=f"tub_gas_{_tkey}_{st.session_state.tub_key}",
                )
            if _telec > 0 or _tgas > 0:
                _tenant_utility_rows.append({'tenant': _tname, 'electric': _telec, 'gas': _tgas})
                _tu_elec_total += _telec
                _tu_gas_total  += _tgas
        if _tenant_utility_rows:
            st.caption(f"✓ {len(_tenant_utility_rows)} tenant(s) — Electric ${_tu_elec_total:,.2f} / Gas ${_tu_gas_total:,.2f}")
        else:
            _rd_loaded = bool(st.session_state.uploaded_files.get("receivable_detail"))
            if _rd_loaded:
                st.caption("↳ No entries — pipeline will read electric amounts from Receivable Detail (per-tenant)")
            elif bool(st.session_state.uploaded_files.get("receivable_summary")):
                st.caption("↳ No entries — Receivable Summary uploaded; upload Receivable Detail too for per-tenant electric breakdown")
            else:
                st.caption(
                    "↳ No entries — upload **Yardi Receivable Detail** in the sidebar for automatic "
                    "per-tenant electric amounts, or enter amounts above manually"
                )

    # ── Payroll Bonus Accrual ─────────────────────────────────────────────────
    with st.expander("💰 Payroll Bonus Accrual — Monthly (optional)", expanded=False):
        st.caption(
            "Enter the annual bonus budget for engineering and/or admin payroll. "
            "The pipeline accrues 1/12 each month and suppresses automatically "
            "in months when the actual bonus payment hits the GL."
        )
        _bonus_col1, _bonus_col2 = st.columns(2)
        with _bonus_col1:
            _bonus_rm = st.number_input(
                "RM-Pay/Wages (615110) — Annual Bonus ($)",
                min_value=0.0, value=0.0, step=1000.0, format="%.2f",
                key="widget_bonus_rm",
                help="Engineering/RM annual bonus budget. Monthly accrual = annual ÷ 12.",
            )
        with _bonus_col2:
            _bonus_admin = st.number_input(
                "Admin-Pay/Wages (637110) — Annual Bonus ($)",
                min_value=0.0, value=0.0, step=1000.0, format="%.2f",
                key="widget_bonus_admin",
                help="Administrative/office annual bonus budget. Monthly accrual = annual ÷ 12.",
            )
        _bonus_overrides: dict = {}
        if _bonus_rm > 0:
            _bonus_overrides['615110'] = float(_bonus_rm)
        if _bonus_admin > 0:
            _bonus_overrides['637110'] = float(_bonus_admin)
        if _bonus_overrides:
            _bonus_monthly_display = {k: f"${v/12:,.2f}/mo" for k, v in _bonus_overrides.items()}
            st.caption(f"✓ Monthly accruals: {' | '.join(f'{k}: {v}' for k, v in _bonus_monthly_display.items())}")
        else:
            st.caption("↳ No bonus entered — pipeline will use Kardin data if uploaded, otherwise skip")

    # ── RE Tax Bill ───────────────────────────────────────────────────────────
    with st.expander("🏛️ RE Tax Bill — Enter every month", expanded=False):
        st.caption(
            "Enter the quarterly RE Tax bill amount **every month** (same amount for all 3 months in each cycle). "
            "**Payment months (Jan / Apr / Jul / Oct):** Berkadia auto-posts the full bill to Yardi. "
            "Pipeline defers 2/3 → DR 135120 Prepaid RE Taxes / CR 641110 Real Estate Taxes. "
            "**Release months (all other):** Pipeline releases 1/3 → DR 641110 Real Estate Taxes / CR 135120 Prepaid RE Taxes. "
            "Net result: expense is spread evenly — 1/3 of the quarterly bill per month."
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
    to complete the month-end close — invoices in Nexus not yet posted, utility proration,
    historical recurring patterns, management fee, prepaid amortization, bonus accruals,
    and one-off items you enter below. Exports two Yardi-import files.

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
            st.session_state.bulk_overrides_p1 = {}
            st.session_state.upload_key_p1 += 1
            for _clr in list(st.session_state.uploaded_files.keys()):
                if _clr not in ("gl_pass2", "budget_comparison_pass2",
                                "trial_balance_pass2", "loan_pass2",
                                "prior_workpaper", "t12_statement_pass2"):
                    st.session_state.uploaded_files.pop(_clr, None)
            # Clear Pass 1 close tracker step (step 1 = JEs generated)
            st.session_state.close_tracker.pop(1, None)
            st.rerun()

    # ── Pass 1 Processing ─────────────────────────────────────────────────────
    if pass1_button:
        with st.spinner("Building accrual entries..."):
            try:
                files_dict = {key: st.session_state.uploaded_files.get(key)
                              for key in file_config.keys()}

                # Auto-load committed Kardin budget if not uploaded this session
                if not files_dict.get("kardin_budget") and _COMMITTED_BUDGET:
                    files_dict["kardin_budget"] = _COMMITTED_BUDGET

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

                # ── Loan statement date validation ────────────────────────────
                # Interest paid on the 7th covers the PRIOR month.  For the
                # January close, the correct statement is the one due 2/7/2026.
                # Flag a warning when the uploaded statement's payment_due_date
                # month doesn't equal close_period month + 1.
                _loan_stmts = engine_result.parsed.get('loan') or []
                if not isinstance(_loan_stmts, list):
                    _loan_stmts = [_loan_stmts]
                _cp_month_map = dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,
                                     Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12)
                _cp_mo = next(
                    (v for k, v in _cp_month_map.items() if k in close_period), 0
                )
                _expected_due_mo = (_cp_mo % 12) + 1  # Jan→2, Dec→1
                _cp_yr = re.search(r'\d{4}', close_period)
                _cp_yr = int(_cp_yr.group()) if _cp_yr else 0
                _expected_due_yr = _cp_yr + 1 if _cp_mo == 12 else _cp_yr
                for _ls in _loan_stmts:
                    if not isinstance(_ls, dict):
                        continue
                    _due = str(_ls.get('payment_due_date') or '')
                    if not _due:
                        continue
                    _due_parts = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', _due)
                    if not _due_parts:
                        continue
                    _due_mo, _due_yr = int(_due_parts.group(1)), int(_due_parts.group(3))
                    if _due_mo != _expected_due_mo or _due_yr != _expected_due_yr:
                        _loan_num = _ls.get('loan_number', 'unknown')
                        engine_result.exceptions.append(Exception_(
                            severity='warning',
                            category='loan',
                            source='berkadia_loan',
                            description=(
                                f"Loan {_loan_num}: statement due date {_due} does not match "
                                f"the {close_period} close. For {close_period}, upload the "
                                f"statement due {_expected_due_mo:02d}/07/{_expected_due_yr} "
                                f"(interest accrued in {close_period} is paid on the 7th of "
                                f"the following month)."
                            ),
                            details={
                                'loan_number': _loan_num,
                                'uploaded_due_date': _due,
                                'expected_due_month': f'{_expected_due_mo:02d}/{_expected_due_yr}',
                            },
                        ))

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

                # Parse T12 for Pass 1 (improves Layer 3 January historical accrual accuracy)
                _t12_file_p1 = st.session_state.uploaded_files.get("t12_statement")
                _t12_result_p1 = None
                if _t12_file_p1 and os.path.exists(_t12_file_p1):
                    try:
                        from parsers.yardi_t12 import parse as parse_t12
                        _t12_result_p1 = parse_t12(_t12_file_p1)
                    except Exception as _e:
                        st.warning(f"Could not parse 12-Month Statement for Pass 1: {_e}")

                # Parse Receivable Summary — highest-priority management fee source.
                # Provides explicit Prepayment row for clean prepayment exclusion.
                _rs_file = st.session_state.uploaded_files.get("receivable_summary")
                _rs_parsed = None
                if _rs_file and os.path.exists(_rs_file):
                    try:
                        from parsers.yardi_receivable_summary import parse as _parse_rs
                        _rs_parsed = _parse_rs(_rs_file)
                    except Exception:
                        _rs_parsed = None

                # Parse Receivable Detail — alternate management fee source, also used
                # by the accrual engine (Mode b per-tenant electric breakdown).
                _rd_file = st.session_state.uploaded_files.get("receivable_detail")
                _rd_parsed = None
                if _rd_file and os.path.exists(_rd_file):
                    try:
                        from parsers.yardi_receivable_detail import parse as _parse_rd
                        _rd_parsed = _parse_rd(_rd_file)
                    except Exception:
                        _rd_parsed = None

                # Step 3: Prepaid ledger — load → merge → release lines
                # Ledger contains Prepaid Other (135150) items only.
                # Insurance (135110) and RE Tax (135120) are excluded from the ledger
                # and handled by dedicated amortization functions in build_accrual_entries().
                status_text.text("Step 3/6: Processing prepaid ledger...")
                progress_bar.progress(45)

                ledger_path = st.session_state.uploaded_files.get("prepaid_ledger")
                ledger_active, ledger_completed = prepaid_ledger.load(ledger_path)

                # Merge Nexus Invoice Detail into ledger
                ledger_active, newly_added = prepaid_ledger.merge_nexus(
                    ledger_active, nexus_data or [], close_period
                )

                # Build visual amortization schedule
                amort_lines = build_prepaid_amortization(nexus_data or [], close_period=close_period)

                # Get release lines now — used to suppress duplicate amortization
                # in build_accrual_entries(); build_prepaid_release_je() called
                # after je_lines so JE numbering stays sequential.
                ledger_release_lines = prepaid_ledger.get_current_amortization(ledger_active, close_period)
                _ledger_release_accounts = {
                    str(item.get('gl_account_number', '')).strip()
                    for item in ledger_release_lines
                    if item.get('gl_account_number')
                }

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
                    bonus_overrides=_bonus_overrides or None,
                    kardin_records=engine_result.parsed.get('kardin_budget') or None,
                    t12_result=_t12_result_p1,
                    gl_activity_log=_gl_activity_log,
                    receivable_detail=_rd_parsed,
                    ledger_release_accounts=_ledger_release_accounts,
                )
                st.session_state['pass1_gl_activity_log'] = _gl_activity_log

                # Build prepaid release JEs after je_lines so JE numbers are sequential
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
                prepaid_ledger.save(ledger_active, ledger_completed, updated_ledger_path,
                                    period=close_period)

                # Step 4: Management fee (JE included in accruals CSV)
                status_text.text("Step 4/6: Calculating management fee...")
                progress_bar.progress(60)

                _daca_file = st.session_state.uploaded_files.get("daca_bank")
                _daca_parsed = None
                if _daca_file and os.path.exists(_daca_file):
                    try:
                        from parsers.yardi_daca_rec import (
                            is_yardi_daca_rec as _is_yardi_daca,
                            parse as _parse_yardi_daca,
                        )
                        from parsers.keybank_daca import parse as _parse_daca
                        if _is_yardi_daca(_daca_file):
                            _daca_parsed = _parse_yardi_daca(_daca_file)
                        else:
                            _daca_parsed = _parse_daca(_daca_file)
                    except Exception:
                        _daca_parsed = None

                # _rd_parsed already parsed above (before build_accrual_entries)

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
                    receivable_summary=_rs_parsed,
                    receivable_detail=_rd_parsed,
                    ar_aging=_ar_aging_parsed,
                )
                fee_je = build_management_fee_je(
                    fee_result,
                    period=close_period,
                    property_code=engine_result.property_name or _active_cfg.property_code,
                    je_number=f'MGT-{len(je_lines)//2 + 1:03d}',
                    property_config=_active_cfg,
                )

                _catchup_amount = detect_prior_period_catchup(gl_parsed)
                _catchup_je = []
                if _catchup_amount and _catchup_amount > 0:
                    _catchup_je = build_catchup_je(
                        _catchup_amount,
                        period=close_period,
                        property_code=engine_result.property_name or 'revlabspm',
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

                # Build a GL account lookup for compound accrual logic.
                # Stores the GL account object per account_code so we can read
                # J-type credits (prior-month auto-reversal) and non-J net change
                # (real K/P/C invoice activity) for each one-off accrual account.
                _sup_gl_accts: dict = {}
                if gl_parsed and hasattr(gl_parsed, 'accounts'):
                    for _sga in gl_parsed.accounts:
                        _sup_gl_accts[str(_sga.account_code).strip()] = _sga

                def _sup_j_credits(acct_obj) -> float:
                    """J-type credit total (auto-reversals of prior pipeline JEs)."""
                    if acct_obj is None:
                        return 0.0
                    return sum(
                        t.credit for t in getattr(acct_obj, 'transactions', [])
                        if t.credit > 0
                        and str(getattr(t, 'control', '') or '').upper().startswith('J')
                    )

                def _sup_j_debits(acct_obj) -> float:
                    """J-type debit total (pipeline JEs already posted this period)."""
                    if acct_obj is None:
                        return 0.0
                    return sum(
                        t.debit for t in getattr(acct_obj, 'transactions', [])
                        if t.debit > 0
                        and str(getattr(t, 'control', '') or '').upper().startswith('J')
                    )

                _sup_counter = 0
                for _sup in _periodic_supplement_rows:
                    _sup_acct_code = _sup['account_code']
                    _sup_monthly   = round(float(_sup['amount']), 2)   # user-entered monthly rate

                    # ── Compound accrual + real-invoice guard ────────────────────
                    # The J-credit in the current GL = prior month's accrual that
                    # auto-reversed.  Compound = j_credits + monthly_rate.
                    # Guard: suppress when non-J (real K/P/C invoice) net >= monthly_rate.
                    _sga_obj   = _sup_gl_accts.get(_sup_acct_code)
                    _sga_net   = float(getattr(_sga_obj, 'net_change', 0) or 0) if _sga_obj else 0.0
                    _sga_j_cr  = _sup_j_credits(_sga_obj)
                    _sga_j_dr  = _sup_j_debits(_sga_obj)
                    _sga_non_j = _sga_net - (_sga_j_dr - _sga_j_cr)   # K/P/C-type net

                    if _sga_non_j >= _sup_monthly:
                        continue   # real invoice posted — no accrual needed

                    _sup_compound   = _sga_j_cr + _sup_monthly
                    _sup_cmpd_note  = (f' — cumulative ${_sup_compound:,.0f} '
                                       f'(${_sga_j_cr:,.0f} prior + ${_sup_monthly:,.0f}/mo)'
                                       if _sga_j_cr > 0 else '')

                    _sje_id  = f'SUP-{_sup_base + _sup_counter + 1:04d}'
                    _sup_counter += 1
                    _sup_desc   = (_sup.get('description') or f"{_sup['account_name']} — one-off accrual") + _sup_cmpd_note
                    _sup_vendor = _sup.get('vendor') or _sup['account_name']
                    _sup_cr_acct = _sup.get('cr_account', '213100')
                    _sup_cr_name = _sup.get('cr_account_name', 'Accrued Expenses')
                    _sup_amt     = round(_sup_compound, 2)
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
                              engine_result.parsed['gl'].metadata.property_code) or 'revlabspm'

                if all_je_lines:
                    _accrual_csv_path = os.path.join(st.session_state.temp_dir, "GA_Accruals_JE.csv")
                    generate_etl_csv(all_je_lines, _accrual_csv_path,
                                     period=close_period, property_code=_prop_code,
                                     auto_reverse=True)

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

                # ── Auto-detect Close Tracker Step 1 ─────────────────────────
                _ct = st.session_state.close_tracker
                if 1 not in _ct:
                    _ct[1] = {
                        "completed_by": st.session_state.get('prepared_by', 'Ryan Walsh'),
                        "timestamp":    datetime.now().strftime("%m/%d/%Y %H:%M"),
                        "auto":         True,
                    }
                    _save_checklist_now()

                # ── Pass 1 Run Log ────────────────────────────────────────────
                try:
                    from run_log import append_run_log_pass1 as _append_p1_log
                    _p1_rl_path  = os.path.join(st.session_state.temp_dir, "GA_Run_Log.csv")
                    _p1_rl_prior = st.session_state.uploaded_files.get("run_log")
                    _p1_je_count = len(all_je_lines) // 2 if all_je_lines else 0
                    _p1_je_total = sum(float(l.get('debit', 0)) for l in (all_je_lines or []))
                    _append_p1_log(
                        output_path         = _p1_rl_path,
                        prior_log_path      = _p1_rl_prior,
                        timestamp           = datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        prepared_by         = st.session_state.get('prepared_by', 'Ryan Walsh'),
                        property_name       = engine_result.property_name or 'Revolution Labs',
                        period              = close_period,
                        je_count            = _p1_je_count,
                        je_total_dollars    = _p1_je_total,
                        close_tracker_complete = (
                            len(st.session_state.close_tracker) == 9
                        ),
                    )
                    st.session_state.pass1_output_files["run_log"] = _p1_rl_path
                except Exception as _p1_rle:
                    pass   # run log is non-critical

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
        # Show a compact warning listing accounts where the pipeline detected
        # an existing JE in the GL and suppressed automated accruals.
        _gl_log = st.session_state.get('pass1_gl_activity_log') or []
        _gl_log_real = [r for r in _gl_log if r.get('account_code') != 'TUB-MODE-B']
        if _gl_log_real:
            _gl_log_sorted = sorted(_gl_log_real, key=lambda x: x['account_code'])
            _acct_list = ', '.join(
                f"{r['account_code']} {r['account_name']}"
                for r in _gl_log_sorted
            )
            st.warning(
                f"⚠️  **Existing GL postings — accruals suppressed for "
                f"{len(_gl_log_sorted)} account{'s' if len(_gl_log_sorted) != 1 else ''}:** "
                f"{_acct_list}. "
                f"Confirm each posting is intentional before uploading JE CSVs to Yardi. "
                f"If a posting is incorrect, delete it from Yardi and re-run Pass 1."
            )
        # ── Management Fee ─────────────────────────────────────────────────
        if fee_result and fee_result.cash_received > 0:
            st.markdown("### Management Fee JE")
            _src_labels = {
                'receivable_summary':          'Receivable Summary (ex-Prepayments)',
                'receivable_detail+ar_aging':  'Receivable Detail (ex-Prepayments via AR Aging)',
                'receivable_detail':           'Receivable Detail (ex-Prepayments)',
                'daca_additions':              'DACA Additions',
                'gl_cash_account':             'GL 111100 Debits',
                'revenue_proxy':               'Revenue Proxy',
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
            st.caption(
                "JE reference prefixes: **IPR** = Invoice Proration · **REC** = Historical Recurring · "
                "**MGT** = Management Fee · **TUB** = Tenant Utility Billing · **SUP** = One-Off Accrual · "
                "**BNS** = Bonus Accrual. "
                "These codes are required for the Yardi import. "
                "✏️ **Description is editable** — click any cell to update the text. "
                "Edited descriptions are written to the downloaded CSV."
            )

            # ── Source → uploaded file label map ────────────────────────────
            _SOURCE_FILE_LABEL = {
                'nexus':                  'Nexus Invoice Detail',
                'invoice_proration':      'Yardi GL Detail',
                'historical':             'Yardi Budget Comparison',
                'prepaid_amortization':   'Prior Month Prepaid Ledger',
                'prepaid_ledger':         'Prior Month Prepaid Ledger',
                'management_fee':         'Management Fee',
                'management_fee_catchup': 'Management Fee (Catch-up)',
                'contract_supplement':    'One-Off Accrual Table',
                'tenant_utility_billing': 'Tenant Utility Billing',
                'bonus_accrual':          'Kardin Budget',
                'manual_addition':        'Manually Added',
            }

            # ── Description cleaner — strip amounts / verbose phrases ────────
            import re as _re_jed
            def _clean_je_desc(raw: str) -> str:
                s = str(raw)
                s = _re_jed.sub(r'\$[\d,]+(?:\.\d+)?(?:/\w+)?', '', s)   # $1,234.56/mo
                s = _re_jed.sub(r'\b[\d,]+\.\d+/\w+', '', s)             # 500.00/day
                s = _re_jed.sub(r'\([A-Za-z]+-?\s*\d{4}\)', '', s)       # (Jan-2026)
                s = _re_jed.sub(r'Historical recurring\s*[—-]\s*', '', s)
                s = _re_jed.sub(r'[Pp]roration\s*[—-]\s*', '', s)
                s = _re_jed.sub(r',?\s*no activity this period.*', '', s, flags=_re_jed.IGNORECASE)
                s = _re_jed.sub(r',?\s*no T12 uploaded.*', '', s, flags=_re_jed.IGNORECASE)
                s = _re_jed.sub(r',?\s*upload for.*', '', s, flags=_re_jed.IGNORECASE)
                s = _re_jed.sub(r'\(annual budget[^)]*\)', '', s, flags=_re_jed.IGNORECASE)
                s = _re_jed.sub(r'×\s*\d+\s*days?', '', s)
                s = _re_jed.sub(r'÷\s*\d+', '', s)
                s = _re_jed.sub(r'\s+', ' ', s).strip().strip('—:,- ').strip()
                return s[:120] if len(s) > 120 else s

            # ── Build CR lookup: je_number → cr_account_code ────────────────
            _cr_lookup: dict = {}
            for _cl in cr_lines:
                _je = _cl.get('je_number', '')
                if _je and _je not in _cr_lookup:
                    _cr_lookup[_je] = {
                        'code': str(_cl.get('account_code', '') or '').strip(),
                    }

            # Friendly labels for well-known CR accounts
            _CR_LABELS = {
                '115200': 'RE Tax Escrow',
                '115300': 'Insurance Escrow',
                '133110': 'Tenant AR Billback',
                '135110': 'Prepaid Insurance',
                '135120': 'Prepaid RE Taxes',
                '135150': 'Prepaids',
                '213100': 'Accrued Expenses',
                '213200': 'Accrued Interest Payable',
                '440500': 'Recovery - Electricity (Tenant Billing)',
                '440700': 'Recovery - Misc Utilities (Tenant Billing)',
                '613110': 'Utilities - Electricity (P&L Reclass)',
                '641110': 'Real Estate Taxes (deferral)',
            }
            def _cr_section_label(code: str) -> str:
                if code in _CR_LABELS:
                    return f"{code} — {_CR_LABELS[code]}"
                if code.startswith('115'):  return f"{code} — Escrow"
                if code.startswith('133'):  return f"{code} — Tenant AR Billback"
                if code.startswith('135'):  return f"{code} — Prepaids"
                if code.startswith('213'):  return f"{code} — Accrued"
                if code.startswith('44'):   return f"{code} — Revenue Recovery"
                if code.startswith('61'):   return f"{code} — Expense Reclassification"
                return f"{code}"

            # ── Group DR lines by CR account ────────────────────────────────
            _groups: dict = {}
            for _dl in dr_lines:
                _cr_code = _cr_lookup.get(_dl.get('je_number', ''), {}).get('code', 'unknown')
                _groups.setdefault(_cr_code, []).append(_dl)

            _sorted_cr_codes = sorted(_groups.keys(), key=lambda c: c.zfill(10))

            _run_key = st.session_state.get('pass1_run_count', 0)

            # ── Summary metrics row ──────────────────────────────────────────
            _src_totals: dict = {}
            for _l in dr_lines:
                _s = _l.get('source', 'other')
                _src_totals[_s] = _src_totals.get(_s, 0) + (_l.get('debit') or 0)
            _total_je_count = len(set(_l.get('je_number', '') for _l in dr_lines))
            _total_amount   = sum(_l.get('debit') or 0 for _l in dr_lines)
            _metric_items = [('Total JEs', str(_total_je_count)),
                             ('Total Amount', f"${_total_amount:,.0f}")] + \
                            [(_SOURCE_FILE_LABEL.get(s, s), f"${t:,.0f}")
                             for s, t in _src_totals.items()]
            _n_cols = min(len(_metric_items), 6)
            _metric_cols = st.columns(_n_cols)
            for _mi, (_lbl, _val) in enumerate(_metric_items[:_n_cols]):
                with _metric_cols[_mi]:
                    st.metric(_lbl, _val)

            st.write("")

            # ── Description override state — keyed by run so fresh run resets ─
            if st.session_state.get('_je_desc_run') != _run_key:
                st.session_state.je_desc_overrides = {}
                st.session_state._je_desc_run = _run_key
            _all_desc_edits: dict = {}   # (je_num, acct_code) → edited description

            # ── One expander per CR account ──────────────────────────────────
            for _cr_code in _sorted_cr_codes:
                _group_lines = sorted(
                    _groups[_cr_code],
                    key=lambda _l: str(_l.get('account_code') or ''),
                )
                _group_total = sum(_l.get('debit') or 0 for _l in _group_lines)
                _group_count = len(set(_l.get('je_number', '') for _l in _group_lines))
                _expander_label = (
                    f"CR {_cr_section_label(_cr_code)}  ·  "
                    f"{_group_count} JE{'s' if _group_count != 1 else ''}  ·  "
                    f"${_group_total:,.0f}"
                )

                with st.expander(_expander_label, expanded=True):
                    st.caption(f"Credit account: **{_cr_code}** — all entries below post to this account")

                    _rows = []
                    for _l in _group_lines:
                        _okey = (_l.get('je_number', ''), _l.get('account_code', ''))
                        _desc = (st.session_state.je_desc_overrides.get(_okey)
                                 or _clean_je_desc(_l.get('description') or ''))
                        _acct_display = _l.get('account_code', '')
                        if _l.get('account_name'):
                            _acct_display = f"{_acct_display}  {_l['account_name']}"
                        _rows.append({
                            "JE #":        _l.get('je_number', ''),
                            "File Source": _SOURCE_FILE_LABEL.get(_l.get('source', ''), _l.get('source', '')),
                            "GL Account":  _acct_display,
                            "Description": _desc,
                            "Amount":      _l.get('debit') or 0,
                        })

                    _edited = st.data_editor(
                        _rows,
                        num_rows="fixed",
                        use_container_width=True,
                        column_config={
                            "JE #":        st.column_config.TextColumn(width="small",  disabled=True),
                            "File Source": st.column_config.TextColumn(width="medium", disabled=True),
                            "GL Account":  st.column_config.TextColumn(width="medium", disabled=True),
                            "Description": st.column_config.TextColumn(width="large"),   # ← editable
                            "Amount":      st.column_config.NumberColumn(
                                               format="$%,.2f", width="small", disabled=True),
                        },
                        hide_index=True,
                        key=f"je_ed_{_cr_code}_{_run_key}",
                    )

                    # Collect description edits
                    import pandas as _pd_jed
                    _edit_rows = (_edited.to_dict('records')
                                  if isinstance(_edited, _pd_jed.DataFrame) else list(_edited))
                    for _orig, _edit in zip(_rows, _edit_rows):
                        _k = (
                            _orig['JE #'],
                            # account_code is the first token of GL Account display string
                            str(_orig['GL Account']).split()[0],
                        )
                        if _edit.get('Description', '') != _orig.get('Description', ''):
                            _all_desc_edits[_k] = _edit['Description']

                    # Subtotal
                    _sub_cols = st.columns([4, 1])
                    with _sub_cols[1]:
                        st.markdown(
                            f"<div style='text-align:right;font-weight:bold;padding-top:4px'>"
                            f"Subtotal: ${_group_total:,.2f}</div>",
                            unsafe_allow_html=True,
                        )

            # ── Apply description edits → update CSV ─────────────────────────
            if _all_desc_edits:
                st.session_state.je_desc_overrides = _all_desc_edits
                _updated_lines = []
                for _l in p1.get("all_je_lines", []):
                    _k = (_l.get('je_number', ''), _l.get('account_code', ''))
                    if _k in _all_desc_edits and (_l.get('debit') or 0) > 0:
                        _l = dict(_l, description=_all_desc_edits[_k])
                    _updated_lines.append(_l)
                p1["all_je_lines"] = _updated_lines
                _p1_er = st.session_state.pass1_engine_result
                _p1_prop = (
                    (_p1_er.parsed.get('gl') and _p1_er.parsed['gl'].metadata.property_code)
                    if _p1_er else None
                ) or 'revlabspm'
                try:
                    from accrual_entry_generator import generate_etl_csv as _gen_etl_ed
                    _ed_csv = os.path.join(st.session_state.temp_dir, "GA_Accruals_JE.csv")
                    _gen_etl_ed(_updated_lines, _ed_csv,
                                period=result.period, property_code=_p1_prop,
                                auto_reverse=True)
                    p1["accrual_je_csv"] = _ed_csv
                except Exception:
                    pass

            # ── Add Missed Entry ──────────────────────────────────────────────
            # Lets you append a DR/CR pair to the Accruals CSV after JEs are
            # generated — e.g. a forgotten one-off accrual entry.
            st.markdown("#### ➕  Add a Missed Entry")

            # Counter drives input key changes so fields clear after each submit
            _add_counter_key = f"je_add_count_{_run_key}"
            if _add_counter_key not in st.session_state:
                st.session_state[_add_counter_key] = 0
            _add_n = st.session_state[_add_counter_key]

            with st.expander("Add entry to Accruals CSV", expanded=False):
                _ac1, _ac2, _ac3, _ac4, _ac5 = st.columns([1.5, 1.5, 4, 1.8, 1])
                with _ac1:
                    _add_dr_raw = st.text_input(
                        "DR Account", placeholder="e.g. 637150",
                        key=f"add_dr_{_run_key}_{_add_n}",
                    )
                with _ac2:
                    _add_cr_raw = st.text_input(
                        "CR Account", value="213100",
                        key=f"add_cr_{_run_key}_{_add_n}",
                    )
                with _ac3:
                    _add_desc_raw = st.text_input(
                        "Description", placeholder="e.g. Tenant Relations accrual",
                        key=f"add_desc_{_run_key}_{_add_n}",
                    )
                with _ac4:
                    _add_amt_raw = st.number_input(
                        "Amount ($)", min_value=0.0, step=100.0, format="%.2f",
                        key=f"add_amt_{_run_key}_{_add_n}",
                    )
                with _ac5:
                    st.write("")   # vertical align
                    st.write("")
                    _add_submit = st.button("Add", key=f"add_btn_{_run_key}_{_add_n}",
                                           type="primary", use_container_width=True)

                if _add_submit:
                    _dr = (_add_dr_raw or '').strip()
                    _cr = (_add_cr_raw or '213100').strip()
                    _desc = (_add_desc_raw or '').strip()
                    _amt  = float(_add_amt_raw or 0.0)
                    if not _dr:
                        st.warning("Please enter a DR Account code.", icon="⚠️")
                    elif not _desc:
                        st.warning("Please enter a Description.", icon="⚠️")
                    elif _amt <= 0:
                        st.warning("Amount must be greater than zero.", icon="⚠️")
                    else:
                        # Determine next ADD-XXXX number
                        _prev_adds = [
                            l for l in p1.get("all_je_lines", [])
                            if str(l.get('je_number', '')).startswith('ADD-')
                        ]
                        _next_add_num = (len(_prev_adds) // 2) + 1
                        _new_je_id    = f"ADD-{_next_add_num:04d}"

                        _new_je_lines = [
                            {
                                'je_number':      _new_je_id, 'line': 1, 'date': '',
                                'account_code':   _dr,
                                'account_name':   '',
                                'description':    _desc,
                                'reference':      'MANUAL-ADD',
                                'debit':          round(_amt, 2), 'credit': 0,
                                'vendor':         '[Manual Addition]',
                                'invoice_number': '',
                                'source':         'manual_addition',
                                'confidence':     'high',
                            },
                            {
                                'je_number':      _new_je_id, 'line': 2, 'date': '',
                                'account_code':   _cr,
                                'account_name':   '',
                                'description':    _desc,
                                'reference':      'MANUAL-ADD',
                                'debit':          0, 'credit': round(_amt, 2),
                                'vendor':         '[Manual Addition]',
                                'invoice_number': '',
                                'source':         'manual_addition',
                                'confidence':     'high',
                            },
                        ]

                        _updated_all = p1.get("all_je_lines", []) + _new_je_lines
                        p1["all_je_lines"] = _updated_all

                        # Regenerate CSV
                        _p1_er_add = st.session_state.pass1_engine_result
                        _p1_prop_add = (
                            (_p1_er_add.parsed.get('gl') and
                             _p1_er_add.parsed['gl'].metadata.property_code)
                            if _p1_er_add else None
                        ) or 'revlabspm'
                        try:
                            from accrual_entry_generator import generate_etl_csv as _gen_etl_add
                            _add_csv_path = os.path.join(
                                st.session_state.temp_dir, "GA_Accruals_JE.csv"
                            )
                            _gen_etl_add(
                                _updated_all, _add_csv_path,
                                period=result.period, property_code=_p1_prop_add,
                                auto_reverse=True
                            )
                            p1["accrual_je_csv"] = _add_csv_path
                            st.success(
                                f"✅  **{_new_je_id}** added — "
                                f"DR {_dr} / CR {_cr}  ${_amt:,.2f}  ·  {_desc}  ·  CSV updated.",
                                icon="✅",
                            )
                        except Exception as _add_ex:
                            st.warning(
                                f"Entry added to session but CSV regeneration failed: {_add_ex}",
                                icon="⚠️",
                            )

                        # Bump counter → clears input fields on next render
                        st.session_state[_add_counter_key] += 1
                        st.rerun()

                # ── Previously added entries ──────────────────────────────────
                _manual_adds = [
                    l for l in p1.get("all_je_lines", [])
                    if l.get('source') == 'manual_addition' and (l.get('debit') or 0) > 0
                ]
                if _manual_adds:
                    st.caption(
                        f"**{len(_manual_adds)} manually added "
                        f"entr{'ies' if len(_manual_adds) != 1 else 'y'} in this session:**"
                    )
                    for _ma in _manual_adds:
                        _ma_cr = next(
                            (l['account_code'] for l in p1.get("all_je_lines", [])
                             if l.get('je_number') == _ma['je_number'] and (l.get('credit') or 0) > 0),
                            '?'
                        )
                        st.caption(
                            f"• **{_ma['je_number']}** · "
                            f"DR {_ma['account_code']} / CR {_ma_cr} · "
                            f"${_ma.get('debit', 0):,.2f} · "
                            f"{_ma.get('description', '')}"
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
                                      'nexus', 'historical', 'management_fee',
                                      'management_fee_catchup', 'invoice_proration',
                                      'prepaid_amortization', 'contract_supplement',
                                      'tenant_utility_billing', 'bonus_accrual', 'prepaid_ledger',
                                      'manual_addition',
                                  }]
        _src_label_map = {
            'nexus':                  'Nexus AP',
            'invoice_proration':      'Invoice Proration',
            'historical':             'Historical Pattern',
            'prepaid_amortization':   'Prepaid Amort.',
            'prepaid_ledger':         'Prepaid Release',
            'management_fee':         'Management Fee',
            'management_fee_catchup': 'Mgmt Fee Catch-up',
            'contract_supplement':    'One-Off Accrual',
            'tenant_utility_billing': 'Tenant Utility',
            'bonus_accrual':          'Bonus Accrual',
            'manual_addition':        'Manually Added',
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

        # ── Prior Month Accrual vs Actuals ────────────────────────────────────
        _gl_for_check = result.parsed.get('gl') if result.parsed else None
        _prior_check  = check_prior_accrual_vs_actual(_gl_for_check) if _gl_for_check else []

        if _prior_check:
            _not_billed   = [r for r in _prior_check if r['status'] == 'NOT YET BILLED']
            _needs_review = [r for r in _prior_check if r['status'] in ('PARTIAL', 'OVER INVOICED')]
            _matched      = [r for r in _prior_check if r['status'] == 'MATCHED']

            _status_icon = '⚠️' if (_not_billed or _needs_review) else '✅'
            _review_note = ''
            if _not_billed:
                _review_note += f' · {len(_not_billed)} not yet billed (re-accrued)'
            if _needs_review:
                _review_note += f' · {len(_needs_review)} variance(s) to review'

            _check_label = (
                f"{_status_icon} Prior Month Accrual vs Actuals — "
                f"{len(_prior_check)} accounts{_review_note}"
            )
            with st.expander(_check_label, expanded=bool(_not_billed or _needs_review)):
                st.caption(
                    "Auto-reversals of last month's pipeline accruals compared to actual invoices "
                    "received this period. **NOT YET BILLED** = invoice hasn't arrived; pipeline "
                    "has re-accrued it. **MATCHED** = actual within 5% of accrual. "
                    "**PARTIAL / OVER INVOICED** = material variance — review before close."
                )

                _STATUS_EMOJI = {
                    'MATCHED':        '✅ MATCHED',
                    'NOT YET BILLED': '🔄 NOT YET BILLED',
                    'PARTIAL':        '⚠️ PARTIAL',
                    'OVER INVOICED':  '⚠️ OVER INVOICED',
                }
                _check_rows = [{
                    'Account':        f"{r['account_code']}  {r['account_name']}",
                    'Prior Accrual':  f"${r['reversal_amount']:,.2f}",
                    'Actual Billed':  f"${r['actual_amount']:,.2f}",
                    'Variance':       (f"+${r['variance']:,.2f}" if r['variance'] >= 0
                                       else f"-${abs(r['variance']):,.2f}"),
                    'Status':         _STATUS_EMOJI.get(r['status'], r['status']),
                    'JE Ref':         r['je_refs'],
                } for r in _prior_check]

                st.dataframe(
                    _check_rows,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'Account':       st.column_config.TextColumn('Account',       width='medium'),
                        'Prior Accrual': st.column_config.TextColumn('Prior Accrual', width='small'),
                        'Actual Billed': st.column_config.TextColumn('Actual Billed', width='small'),
                        'Variance':      st.column_config.TextColumn('Variance',      width='small'),
                        'Status':        st.column_config.TextColumn('Status',        width='medium'),
                        'JE Ref':        st.column_config.TextColumn('JE Ref',        width='small'),
                    },
                )

                if _not_billed:
                    _nb_accts = ', '.join(
                        f"{r['account_code']} {r['account_name']}"
                        for r in _not_billed
                    )
                    st.warning(
                        f"**{len(_not_billed)} account(s) not yet billed:** {_nb_accts}. "
                        f"The pipeline has included re-accruals in the Accruals JE CSV. "
                        f"Confirm each was also re-accrued last month before posting.",
                        icon="🔄",
                    )
                if _needs_review:
                    _rv_parts = [
                        f"{r['account_code']} ({r['status']}: "
                        f"accrued ${r['reversal_amount']:,.0f}, "
                        f"billed ${r['actual_amount']:,.0f})"
                        for r in _needs_review
                    ]
                    st.warning(
                        f"**{len(_needs_review)} account(s) with material variance:** "
                        + ', '.join(_rv_parts) + '. Review before close.',
                        icon="⚠️",
                    )

        st.divider()

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

    st.markdown(
        "<div style='background:#E3F2FD;border-left:4px solid #1565C0;"
        "border-radius:5px;padding:8px 16px;margin-bottom:14px;font-size:0.85rem;color:#1565C0;'>"
        "⬤ &nbsp;<strong>Pass 2 — Post-Close</strong>&nbsp; Upload final Yardi exports, "
        "generate QC workbook, workpaper, variance comments, audit trail, and management fee invoice."
        "</div>",
        unsafe_allow_html=True,
    )

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

    # ── Close Process Tracker (always visible) ────────────────────────────────
    st.markdown("### 📋 Close Process Tracker")
    st.caption(
        "Tracks every step of the monthly close lifecycle from JLL handoff through "
        "Lauren's package release. Auto-detected steps are set by the pipeline automatically. "
        "Manual steps require your confirmation."
    )

    from close_tracker_generator import CLOSE_TRACKER_STEPS as _CT_STEPS

    _ct = st.session_state.close_tracker
    _ct_complete_count = sum(1 for i in range(len(_CT_STEPS)) if i in _ct)
    _ct_total = len(_CT_STEPS)

    # Progress bar
    st.progress(_ct_complete_count / _ct_total,
                text=f"{_ct_complete_count} of {_ct_total} steps complete")

    _ct_reviewers = (_active_cfg.team_members
                     if _active_cfg.team_members
                     else ["Ryan Walsh", "Natasha Parker", "Lauren Sullivan"])

    for _ct_idx, _ct_desc, _ct_type in _CT_STEPS:
        _ct_entry = _ct.get(_ct_idx)
        _ct_done  = bool(_ct_entry)

        _ct_col_step, _ct_col_desc, _ct_col_by, _ct_col_btn, _ct_col_status = st.columns(
            [0.5, 4, 2, 1.2, 3]
        )
        with _ct_col_step:
            st.markdown(f"**{_ct_idx + 1}**")
        with _ct_col_desc:
            _ct_badge = " _(auto)_" if _ct_type == 'auto' else ""
            st.markdown(f"{_ct_desc}{_ct_badge}")

        if _ct_done:
            with _ct_col_status:
                _ct_auto_label = " _(auto-detected)_" if _ct_entry.get('auto') else ""
                st.markdown(
                    f"<span style='color:#2E7D32;font-weight:600;'>"
                    f"✅ {_ct_entry['completed_by']} &nbsp;·&nbsp; {_ct_entry['timestamp']}"
                    f"{_ct_auto_label}</span>",
                    unsafe_allow_html=True,
                )
        else:
            if _ct_type == 'manual':
                with _ct_col_by:
                    _ct_reviewer = st.selectbox(
                        "Completed by", _ct_reviewers,
                        key=f"ct_rev_{_ct_idx}",
                        label_visibility="collapsed",
                    )
                with _ct_col_btn:
                    if st.button("✔ Mark Complete", key=f"ct_btn_{_ct_idx}",
                                 use_container_width=True):
                        _ct[_ct_idx] = {
                            "completed_by": _ct_reviewer,
                            "timestamp":    datetime.now().strftime("%m/%d/%Y %H:%M"),
                            "auto":         False,
                        }
                        # Step 7 auto-detect: QC review complete when Ryan/Natasha Parker mark it
                        if _ct_idx == 7:
                            pass  # step 7 is manual here — already handled
                        # Step 8: generate close tracker xlsx when final package is released
                        if _ct_idx == 8:
                            try:
                                from close_tracker_generator import generate_close_tracker_xlsx as _gen_ct
                                _ct_xlsx_path = os.path.join(
                                    st.session_state.temp_dir, "GA_Close_Tracker.xlsx"
                                )
                                _p2_result = st.session_state.pass2_engine_result
                                _ct_period  = (_p2_result.period if _p2_result
                                               else st.session_state.get('close_period_input', 'Period'))
                                _ct_prop    = (_p2_result.property_name if _p2_result
                                               else 'Revolution Labs')
                                _gen_ct(
                                    output_path   = _ct_xlsx_path,
                                    close_tracker = _ct,
                                    period        = _ct_period,
                                    property_name = _ct_prop,
                                )
                                st.session_state.pass2_output_files["close_tracker"] = _ct_xlsx_path
                                st.success(
                                    "Close Tracker exported — included in the ZIP package.",
                                    icon="✅"
                                )
                            except Exception as _ct_e:
                                st.warning(f"Close Tracker export failed: {_ct_e}")
                        st.rerun()
            else:
                with _ct_col_status:
                    st.markdown(
                        "<span style='color:#9E9E9E;'>Pending (pipeline will auto-detect)</span>",
                        unsafe_allow_html=True,
                    )

    # Export Close Tracker button (available any time)
    st.markdown("")
    _ct_exp_col, _ = st.columns([2, 5])
    with _ct_exp_col:
        if st.button("📄 Export Close Tracker", use_container_width=True,
                     help="Generates GA_Close_Tracker.xlsx and adds it to the ZIP"):
            try:
                from close_tracker_generator import generate_close_tracker_xlsx as _gen_ct2
                _ct_xlsx_path2 = os.path.join(
                    st.session_state.temp_dir, "GA_Close_Tracker.xlsx"
                )
                _p2r = st.session_state.pass2_engine_result
                _ct_period2 = (_p2r.period if _p2r
                               else st.session_state.get('close_period_input', 'Period'))
                _ct_prop2   = (_p2r.property_name if _p2r else 'Revolution Labs')
                _gen_ct2(
                    output_path   = _ct_xlsx_path2,
                    close_tracker = st.session_state.close_tracker,
                    period        = _ct_period2,
                    property_name = _ct_prop2,
                )
                st.session_state.pass2_output_files["close_tracker"] = _ct_xlsx_path2
                st.success("Close Tracker exported — included in the ZIP package.", icon="✅")
                st.rerun()
            except Exception as _ct_e2:
                st.error(f"Close Tracker export failed: {_ct_e2}")

    _ct_dl_path = st.session_state.pass2_output_files.get("close_tracker")
    if _ct_dl_path and os.path.exists(_ct_dl_path):
        with open(_ct_dl_path, "rb") as _ct_f:
            st.download_button(
                label="⬇️ Download Close Tracker",
                data=_ct_f.read(),
                file_name="GA_Close_Tracker.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
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
        "t12_statement_pass2", "loan_pass2", "prior_workpaper",
        "bank_rec_dev_xlsx", "unknown",
    ]
    _P2_SLOT_LABELS = [_FILE_LABELS.get(k, k) for k in _P2_SLOT_KEYS]

    _bulk_p2 = st.file_uploader(
        "Drop all Pass 2 files here",
        accept_multiple_files=True,
        type=["xlsx", "xls", "pdf"],
        key=f"bulk_upload_p2_{st.session_state.upload_key_p2}",
    )

    # Clear Pass 2 slots so stale entries don't persist after file removal
    for _clr_k2 in set(_P2_SLOT_KEYS) - {"unknown"}:
        st.session_state.uploaded_files.pop(_clr_k2, None)

    if _bulk_p2:
        _loan_paths_p2: list = []

        for _uf2 in _bulk_p2:
            _raw2 = bytes(_uf2.getbuffer())
            _det_key2, _conf2, _det_label2 = _classify_file(_uf2.name, _raw2, pass2=True, property_config=_active_cfg)

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

        # File count for Pass 2 — list slots (e.g. loan_pass2 with multiple PDFs)
        # are unfolded so 3 Berkadia PDFs count as 3, not 1.
        _p2_count_keys = set(_P2_SLOT_KEYS) - {"unknown"}
        _p2_file_count = sum(
            len(v) if isinstance(v, list) else 1
            for k, v in st.session_state.uploaded_files.items()
            if k in _p2_count_keys and v is not None
        )
        if _p2_file_count > 0:
            st.caption(f"**{_p2_file_count} file(s) uploaded**")

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

    _rl_upload = st.file_uploader(
        "Prior Run Log (optional)",
        type=["csv"],
        key=f"run_log_upload_{st.session_state.upload_key_p2}",
        help="Upload the GA_Run_Log.csv from last month to carry forward the history.",
    )
    if _rl_upload:
        _rl_save = os.path.join(st.session_state.temp_dir, "prior_run_log.csv")
        with open(_rl_save, "wb") as _f:
            _f.write(_rl_upload.read())
        st.session_state.uploaded_files["run_log"] = _rl_save

    # ── Additional workpaper source files (raw Yardi reports per account) ──────
    # These replace the generated GL transaction tabs for the named accounts with
    # the raw Yardi export pasted directly into the workpaper.
    with st.expander("📋 Workpaper raw report overrides (optional)", expanded=False):
        st.caption(
            "Drop raw Yardi Excel exports here — each file is assigned a type and copied "
            "directly into the matching workpaper tab. AR Aging and Capital Schedule "
            "auto-source from Pass 1 when available."
        )

        # ── WP bulk uploader session state ────────────────────────────────────
        if "bulk_overrides_wp" not in st.session_state:
            st.session_state.bulk_overrides_wp = {}

        _WP_SLOT_KEYS = [
            "ar_aging_pass2", "ap_aging", "bank_rec_xlsx", "daca_bank_rec_xlsx",
            "capital_schedule_pass2", "prepaid_ledger_p2", "capital_seed", "unknown",
        ]
        _WP_SLOT_LABELS = [
            "AR Aging Detail — 133100 AR Control",
            "AP Aging Detail — 211300 AP Control",
            "Bank Rec Excel — 111100 PNC Operating",
            "Bank Rec Excel — 115100 DACA",
            "Capital Accounts Schedule",
            "Prepaid Ledger Updated",
            "Capital Schedule Seed",
            "Unknown — select type",
        ]

        # Clear WP slots so stale entries don't persist after file removal
        for _clr_kwp in set(_WP_SLOT_KEYS) - {"unknown"}:
            st.session_state.uploaded_files.pop(_clr_kwp, None)

        _bulk_wp = st.file_uploader(
            "Drop all workpaper override files here",
            accept_multiple_files=True,
            type=["xlsx", "xls"],
            key=f"bulk_upload_wp_{st.session_state.get('upload_key_p2', 0)}",
        )

        if _bulk_wp:
            for _ufwp in _bulk_wp:
                _raw_wp = bytes(_ufwp.getbuffer())
                # These are custom Yardi reports — auto-detection is unreliable;
                # always show the type selectbox for explicit user assignment.
                _eff_key_wp = st.session_state.bulk_overrides_wp.get(_ufwp.name, "unknown")

                _ic_wp, _fn_wp, _tp_wp = st.columns([1, 3, 4])
                _ic_wp.markdown("✅" if _eff_key_wp != "unknown" else "⚠️")

                _short_wp = _ufwp.name if len(_ufwp.name) <= 22 else _ufwp.name[:19] + "…"
                _fn_wp.caption(_short_wp)

                _cur_idx_wp = (
                    _WP_SLOT_KEYS.index(_eff_key_wp)
                    if _eff_key_wp in _WP_SLOT_KEYS
                    else len(_WP_SLOT_KEYS) - 1
                )
                _sel_label_wp = _tp_wp.selectbox(
                    "type", _WP_SLOT_LABELS, index=_cur_idx_wp,
                    key=f"ovr_wp_{_ufwp.name}", label_visibility="collapsed",
                )
                _eff_key_wp = _WP_SLOT_KEYS[_WP_SLOT_LABELS.index(_sel_label_wp)]
                st.session_state.bulk_overrides_wp[_ufwp.name] = _eff_key_wp

                if _eff_key_wp != "unknown":
                    _wp_path = os.path.join(st.session_state.temp_dir, f"wp_{_ufwp.name}")
                    if not os.path.exists(_wp_path) or os.path.getsize(_wp_path) != _ufwp.size:
                        with open(_wp_path, "wb") as _f_wp:
                            _f_wp.write(_raw_wp)
                    st.session_state.uploaded_files[_eff_key_wp] = _wp_path

            # Auto-source status for optional slots not yet uploaded
            _wp_loaded = set(st.session_state.uploaded_files.keys())
            if "ar_aging_pass2" not in _wp_loaded:
                _p1_ar = st.session_state.uploaded_files.get("ar_aging")
                if _p1_ar and os.path.exists(_p1_ar):
                    st.caption("↳ AR Aging: auto-sourced from Pass 1")
                else:
                    st.caption("⚠️ AR Aging not uploaded — 133100 tab will show GL transactions")
            if "capital_schedule_pass2" not in _wp_loaded:
                _p1_cap = st.session_state.uploaded_files.get("capital_schedule")
                if _p1_cap and os.path.exists(_p1_cap):
                    st.caption("↳ Capital Schedule: auto-sourced from Pass 1")
                else:
                    st.caption("⚠️ Capital Schedule not uploaded — capital tabs show GL transactions")

            # File count
            _wp_count = sum(
                1 for k in _WP_SLOT_KEYS
                if k != "unknown" and st.session_state.uploaded_files.get(k)
            )
            if _wp_count > 0:
                st.caption(f"**{_wp_count} file(s) assigned**")

            # Clean up overrides for files no longer in uploader
            _active_wp = {_ufwp.name for _ufwp in _bulk_wp}
            st.session_state.bulk_overrides_wp = {
                k: v for k, v in st.session_state.bulk_overrides_wp.items()
                if k in _active_wp
            }

        st.caption(
            "AR Aging auto-sources from Pass 1 in the same session — only re-upload above "
            "if starting Pass 2 in a new browser session.  "
            "Prepaid Ledger falls back to same-session Pass 1 data if not re-uploaded.  "
            "Capital Seed (Book3.xlsx) bootstraps January capital tabs — ignored once a "
            "prior workpaper or current-period Capital Schedule is uploaded."
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
        if not st.session_state.confirm_reset_p2:
            if st.button("🔄 Reset Pass 2", use_container_width=True, key="reset_pass2"):
                st.session_state.confirm_reset_p2 = True
                st.rerun()
        else:
            st.warning("⚠️ Clears results — uploaded files stay loaded.")
            _rp2_col1, _rp2_col2 = st.columns(2)
            if _rp2_col1.button("✅ Confirm", use_container_width=True, key="confirm_reset_p2_btn"):
                st.session_state.confirm_reset_p2 = False
                st.session_state.pass2_complete = False
                st.session_state.pass2_engine_result = None
                st.session_state.pass2_output_files = {}
                st.session_state.signoff_state = {}
                import pandas as _pd_r2
                st.session_state.post_close_je_df = _pd_r2.DataFrame({
                    "JE #": ["PC-001", "PC-001"], "Description": ["", ""],
                    "Account Code": ["", ""],
                    "Debit ($)": [0.0, 0.0], "Credit ($)": [0.0, 0.0],
                    "Line Description": ["", ""],
                })
                st.session_state.pop("_pcje_latest", None)
                # Clear Pass 2 close tracker steps (5=files uploaded, 6=reports generated,
                # 7=QC review complete, 8=package released). Steps 0-4 are pre-Pass-2 and
                # should be preserved since the work already happened.
                for _ct_step in (5, 6, 7, 8):
                    st.session_state.close_tracker.pop(_ct_step, None)
                # NOTE: upload_key_p2 is NOT incremented — uploaded files stay in
                # the uploader widgets so Generate Reports can be re-run immediately
                # without re-dropping any files.
                st.rerun()
            if _rp2_col2.button("❌ Cancel", use_container_width=True, key="cancel_reset_p2_btn"):
                st.session_state.confirm_reset_p2 = False
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

                # Auto-load committed Kardin budget if not uploaded this session
                if not files_dict.get("kardin_budget") and _COMMITTED_BUDGET:
                    files_dict["kardin_budget"] = _COMMITTED_BUDGET

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

                # Initialise variables that may be assigned later in conditional
                # parse blocks — ensures they are always defined if those blocks
                # are skipped due to missing files or upstream exceptions.
                _ar_aging_parsed_p2   = None
                _capital_schedule_data = None
                tb_result              = None
                t12_result             = None
                bank_rec_data          = None
                daca_bank_data         = None
                dev_bank_rec_data      = None
                gl_cash_balance        = None
                daca_gl_balance        = None

                # ── Loan statement date validation (Pass 2) ───────────────────
                _loan_stmts_p2 = engine_result.parsed.get('loan') or []
                if not isinstance(_loan_stmts_p2, list):
                    _loan_stmts_p2 = [_loan_stmts_p2]
                _cp_mo_p2 = next(
                    (v for k, v in dict(Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,
                                        Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12).items()
                     if k in close_period), 0
                )
                _exp_due_mo_p2 = (_cp_mo_p2 % 12) + 1
                _cp_yr_p2 = re.search(r'\d{4}', close_period)
                _cp_yr_p2 = int(_cp_yr_p2.group()) if _cp_yr_p2 else 0
                _exp_due_yr_p2 = _cp_yr_p2 + 1 if _cp_mo_p2 == 12 else _cp_yr_p2
                for _ls2 in _loan_stmts_p2:
                    if not isinstance(_ls2, dict):
                        continue
                    _due2 = str(_ls2.get('payment_due_date') or '')
                    if not _due2:
                        continue
                    _due_parts2 = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', _due2)
                    if not _due_parts2:
                        continue
                    _due_mo2, _due_yr2 = int(_due_parts2.group(1)), int(_due_parts2.group(3))
                    if _due_mo2 != _exp_due_mo_p2 or _due_yr2 != _exp_due_yr_p2:
                        _ln2 = _ls2.get('loan_number', 'unknown')
                        engine_result.exceptions.append(Exception_(
                            severity='warning',
                            category='loan',
                            source='berkadia_loan',
                            description=(
                                f"Loan {_ln2}: statement due date {_due2} does not match "
                                f"the {close_period} close. For {close_period}, upload the "
                                f"statement due {_exp_due_mo_p2:02d}/07/{_exp_due_yr_p2} "
                                f"(interest accrued in {close_period} is paid on the 7th of "
                                f"the following month)."
                            ),
                            details={
                                'loan_number': _ln2,
                                'uploaded_due_date': _due2,
                                'expected_due_month': f'{_exp_due_mo_p2:02d}/{_exp_due_yr_p2}',
                            },
                        ))

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
                        from parsers.yardi_daca_rec import (
                            is_yardi_daca_rec as _is_yardi_daca2,
                            parse as _parse_yardi_daca2,
                        )
                        from parsers.keybank_daca import parse as _parse_daca2
                        if _is_yardi_daca2(_daca_file):
                            daca_bank_data = _parse_yardi_daca2(_daca_file)
                        else:
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

                # Parse AR Aging and Capital Schedule before workpaper generation
                # (both are passed into bs_workpaper_generator.generate())
                # Pass 2-specific upload takes priority; falls back to Pass 1 sidebar file.
                _ar_aging_file_p2 = (
                    st.session_state.uploaded_files.get("ar_aging_pass2")
                    or st.session_state.uploaded_files.get("ar_aging")
                )
                _ar_aging_parsed_p2 = None
                if _ar_aging_file_p2 and os.path.exists(_ar_aging_file_p2):
                    try:
                        from parsers.yardi_ar_aging import parse as _parse_ar_aging2
                        _ar_aging_parsed_p2 = _parse_ar_aging2(_ar_aging_file_p2)
                    except Exception:
                        _ar_aging_parsed_p2 = None

                _capital_file = (
                    st.session_state.uploaded_files.get("capital_schedule_pass2")
                    or st.session_state.uploaded_files.get("capital_schedule")
                )
                _capital_schedule_data = None
                if _capital_file and os.path.exists(_capital_file):
                    try:
                        from parsers.capital_schedule import parse as _parse_capital
                        _capital_schedule_data = _parse_capital(_capital_file)
                    except Exception:
                        _capital_schedule_data = None

                # Seed fallback: use Book3.xlsx when no current-period schedule uploaded
                # (January only — prior workpaper carry-forward supersedes this from Feb onward)
                if not _capital_schedule_data:
                    _capital_seed_file = st.session_state.uploaded_files.get("capital_seed")
                    if _capital_seed_file and os.path.exists(_capital_seed_file):
                        try:
                            from parsers.capital_seed import parse as _parse_capital_seed
                            _capital_schedule_data = _parse_capital_seed(_capital_seed_file)
                        except Exception:
                            pass

                if gl_parsed:
                    # tb_result is optional — generator writes "No TB data" in the TB tab
                    # when None; never block the whole workpaper just because TB is absent.
                    if not tb_result:
                        st.info(
                            "Trial Balance not uploaded — workpaper TB tab will show 'No TB data available'. "
                            "Upload a Trial Balance in the Pass 2 section to enable the full tie-out.",
                            icon="ℹ️",
                        )
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

                        # ── Prepaid ledger active items for workpaper ─────────────
                        # Priority: Pass 2 upload → same-session Pass 1 output
                        _prepaid_active = []
                        _prepaid_p2_path = st.session_state.uploaded_files.get("prepaid_ledger_p2")
                        if _prepaid_p2_path and os.path.exists(_prepaid_p2_path):
                            try:
                                _prepaid_active, _ = prepaid_ledger.load(_prepaid_p2_path)
                            except Exception as _pe:
                                st.caption(f"⚠️ Could not read Pass 2 prepaid ledger: {_pe}")
                        if not _prepaid_active:
                            # Fall back to same-session Pass 1 data (already post-advance)
                            _p1_data = st.session_state.get('pass1_output', {})
                            _prepaid_active = _p1_data.get('ledger_active', [])

                        bs_workpaper_generator.generate(
                            gl_result=gl_parsed,
                            tb_result=tb_result,
                            output_path=bs_wp_path,
                            period=close_period,
                            property_name=engine_result.property_name or 'Revolution Labs',
                            prepaid_ledger_active=_prepaid_active,
                            bank_rec_data=bank_rec_data,
                            gl_cash_balance=gl_cash_balance,
                            daca_bank_data=daca_bank_data,
                            daca_gl_balance=daca_gl_balance,
                            prior_workpaper_path=_prior_wp_path,
                            prior_period=_prior_period,
                            berkadia_loans=_berkadia_loans,
                            dev_bank_rec_data=dev_bank_rec_data,
                            ar_aging_data=_ar_aging_parsed_p2,
                            capital_schedule_data=_capital_schedule_data,
                            tb_filepath=_tb_file,
                            ar_aging_filepath=_ar_aging_file_p2,
                            ap_aging_filepath=st.session_state.uploaded_files.get("ap_aging"),
                            bank_rec_xlsx_filepath=st.session_state.uploaded_files.get("bank_rec_xlsx"),
                            daca_bank_rec_xlsx_filepath=st.session_state.uploaded_files.get("daca_bank_rec_xlsx"),
                            dev_bank_rec_xlsx_filepath=st.session_state.uploaded_files.get("bank_rec_dev_xlsx"),
                            prepared_by=st.session_state.get("prepared_by", "Ryan Walsh"),
                        )
                        st.session_state.pass2_output_files["bs_workpaper"] = bs_wp_path
                    except Exception as _e:
                        import traceback as _tb
                        st.warning(f"Workpaper generation skipped: {_e}")
                        st.code(_tb.format_exc(), language="text")
                else:
                    st.warning("Workpaper skipped — no GL parsed. Upload a GL in Pass 2.", icon="⚠️")

                # Step 3: (Institutional workpapers removed — not needed)

                # Step 4: Management fee (informational — already in GL)
                status_text.text("Step 4/6: Verifying management fee...")
                progress_bar.progress(58)
                try:
                    _rs_file_p2 = st.session_state.uploaded_files.get("receivable_summary")
                    _rs_parsed_p2 = None
                    if _rs_file_p2 and os.path.exists(_rs_file_p2):
                        try:
                            from parsers.yardi_receivable_summary import parse as _parse_rs2
                            _rs_parsed_p2 = _parse_rs2(_rs_file_p2)
                        except Exception:
                            _rs_parsed_p2 = None

                    _rd_file_p2 = st.session_state.uploaded_files.get("receivable_detail")
                    _rd_parsed_p2 = None
                    if _rd_file_p2 and os.path.exists(_rd_file_p2):
                        try:
                            from parsers.yardi_receivable_detail import parse as _parse_rd2
                            _rd_parsed_p2 = _parse_rd2(_rd_file_p2)
                        except Exception:
                            _rd_parsed_p2 = None

                    fee_result = calculate_mgmt_fee(
                        gl_parsed=gl_parsed,
                        budget_rows=bc_parsed or [],
                        daca_parsed=daca_bank_data,
                        receivable_summary=_rs_parsed_p2,
                        receivable_detail=_rd_parsed_p2,
                        ar_aging=_ar_aging_parsed_p2,
                    )
                    st.session_state.pass2_output_files["fee_result"] = fee_result

                    # Generate management fee invoice PDF
                    if fee_result and fee_result.cash_received > 0:
                        try:
                            _inv_path = os.path.join(
                                st.session_state.temp_dir,
                                f"GA_MgmtFee_Invoice_{close_period.replace('-', '')}.pdf",
                            )
                            generate_mgmt_fee_invoice(
                                period=close_period,
                                cash_received=fee_result.cash_received,
                                output_path=_inv_path,
                                property_config=_active_cfg,
                            )
                            st.session_state.pass2_output_files["fee_invoice"] = _inv_path
                        except Exception as _inv_e:
                            st.warning(f"Management fee invoice skipped: {_inv_e}")
                            st.session_state.pass2_output_files["fee_invoice"] = None
                    else:
                        st.session_state.pass2_output_files["fee_invoice"] = None

                except Exception:
                    fee_result = None
                    st.session_state.pass2_output_files["fee_result"] = None
                    st.session_state.pass2_output_files["fee_invoice"] = None

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

                # Audit Trail — comprehensive pass-1+pass-2 record for auditors
                try:
                    _at_path = os.path.join(
                        st.session_state.temp_dir,
                        f"GA_Audit_Trail_{close_period.replace('-', '')}.xlsx",
                    )
                    # Pull Pass 1 JE lines from session state if available
                    _p1_out = st.session_state.get('pass1_output_files', {})
                    _at_je_lines = _p1_out.get('all_je_lines') or []
                    _at_fee      = fee_result   # Pass 2 fee verification result
                    _at_qc       = st.session_state.pass2_output_files.get('qc_report')

                    # Prior-month accrual check against the PASS 2 (final) GL
                    _gl_for_at = engine_result.parsed.get('gl') if engine_result.parsed else None
                    _at_prior  = check_prior_accrual_vs_actual(_gl_for_at) if _gl_for_at else []

                    generate_audit_trail(
                        output_path         = _at_path,
                        period              = close_period,
                        property_name       = engine_result.property_name or 'Revolution Labs',
                        all_je_lines        = _at_je_lines,
                        fee_result          = _at_fee,
                        qc_report           = _at_qc,
                        prior_accrual_check = _at_prior,
                        files_uploaded      = st.session_state.uploaded_files,
                    )
                    st.session_state.pass2_output_files["audit_trail"] = _at_path
                except Exception as _ate:
                    st.warning(f"Audit trail skipped: {_ate}")
                    st.session_state.pass2_output_files["audit_trail"] = None

                # ── Auto-detect Close Tracker Steps 5 & 6 ────────────────────
                _ct = st.session_state.close_tracker
                _p2_ts = datetime.now().strftime("%m/%d/%Y %H:%M")
                _p2_by = st.session_state.get('prepared_by', 'Ryan Walsh')
                _ck_changed = False
                if 5 not in _ct:
                    _ct[5] = {"completed_by": _p2_by, "timestamp": _p2_ts, "auto": True}
                    _ck_changed = True
                if 6 not in _ct:
                    _ct[6] = {"completed_by": _p2_by, "timestamp": _p2_ts, "auto": True}
                    _ck_changed = True
                if _ck_changed:
                    _save_checklist_now()

                # ── Run Log ───────────────────────────────────────────────────
                try:
                    from run_log import append_run_log as _append_run_log
                    _rl_path  = os.path.join(st.session_state.temp_dir, "GA_Run_Log.csv")
                    # Use Pass 1 run log as prior if it exists, else uploaded prior
                    _rl_prior = (
                        st.session_state.pass1_output_files.get("run_log")
                        or st.session_state.uploaded_files.get("run_log")
                    )
                    _rl_qc    = _at_qc if '_at_qc' in dir() else None
                    _rl_pass  = sum(1 for c in (_rl_qc.checks if _rl_qc else []) if c.status == 'PASS')
                    _rl_fail  = sum(1 for c in (_rl_qc.checks if _rl_qc else []) if c.status in ('FLAG', 'FAIL'))
                    _rl_files = [k for k, v in st.session_state.pass2_output_files.items() if v]
                    _append_run_log(
                        output_path            = _rl_path,
                        prior_log_path         = _rl_prior,
                        timestamp              = datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        prepared_by            = st.session_state.get('prepared_by', 'Ryan Walsh'),
                        property_name          = engine_result.property_name or 'Revolution Labs',
                        period                 = close_period,
                        files_generated        = _rl_files,
                        qc_checks_passed       = _rl_pass,
                        qc_checks_failed       = _rl_fail,
                        close_tracker_complete = (len(st.session_state.close_tracker) == 9),
                    )
                    st.session_state.pass2_output_files["run_log"] = _rl_path
                except Exception as _rle:
                    pass   # run log is non-critical; never block report generation

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

        # Period-state indicator (shown first — informational, not an error)
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

        # Status banner (CLEAN / WARNINGS / ERRORS from engine validation)
        status = result.status
        status_color = {"CLEAN": "#2ecc71", "WARNINGS": "#f39c12"}.get(status, "#e74c3c")
        status_text_str = {"CLEAN": "✅ CLEAN", "WARNINGS": "⚠️ WARNINGS"}.get(status, "❌ ERRORS")
        st.markdown(f"""
        <div style="background-color: {status_color}20; border-left: 5px solid {status_color};
             padding: 15px; border-radius: 5px; margin: 15px 0;">
            <h3 style="color: {status_color}; margin: 0;">{status_text_str}</h3>
        </div>
        """, unsafe_allow_html=True)

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

        # ── Download Section ───────────────────────────────────────────────
        st.markdown("### Download Reports")

        import zipfile, io
        period_label = (result.period or 'Period').replace('-', '_')

        p2_zip_files = {
            f"RevLabs_{period_label}_Workpapers.xlsx":      p2.get("bs_workpaper"),
            f"RevLabs_{period_label}_QC_Workbook.xlsx":     p2.get("qc_workbook"),
            f"RevLabs_{period_label}_Exceptions.xlsx":      p2.get("exception_report"),
            f"RevLabs_{period_label}_BC_Internal.xlsx":     p2.get("annotated_bc"),
            f"RevLabs_{period_label}_Audit_Trail.xlsx":     p2.get("audit_trail"),
            f"RevLabsPM_Invoice_{period_label}.pdf":        p2.get("fee_invoice"),
            f"RevLabs_{period_label}_Run_Log.csv":          p2.get("run_log"),
            f"RevLabs_{period_label}_Signoff_Record.xlsx":  p2.get("signoff_record"),
            f"RevLabs_{period_label}_Close_Tracker.xlsx":   p2.get("close_tracker"),
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
        _dc1, _dc2, _dc3 = st.columns(3)
        _dl_cols = [_dc1, _dc2, _dc3]

        _dl_items = [
            ("bs_workpaper",    "📋 Workpapers",
             f"GA_Workpapers_{datetime.now().strftime('%Y%m%d')}.xlsx",      None),
            ("qc_workbook",     "✅ QC Workbook",
             f"GA_QC_Workbook_{datetime.now().strftime('%Y%m%d')}.xlsx",     None),
            ("exception_report","⚠️ Exception Report",
             f"GA_Exceptions_{datetime.now().strftime('%Y%m%d')}.xlsx",      None),
            ("annotated_bc",    "💬 Budget Comparison",
             f"GA_BC_Internal_{datetime.now().strftime('%Y%m%d')}.xlsx",     None),
            ("audit_trail",     "🔍 Audit Trail",
             f"GA_Audit_Trail_{datetime.now().strftime('%Y%m%d')}.xlsx",     None),
            ("fee_invoice",     "🧾 Management Fee Invoice",
             f"RevLabsPM_Invoice_{(result.period or '').replace('-','')}.pdf",
             "application/pdf"),
        ]

        for i, (key, label, fname, mime) in enumerate(_dl_items):
            fpath = p2.get(key)
            if fpath and os.path.exists(fpath):
                with _dl_cols[i % 3]:
                    with open(fpath, "rb") as f:
                        kw = dict(label=label, data=f.read(), file_name=fname,
                                  use_container_width=True)
                        if mime:
                            kw["mime"] = mime
                        st.download_button(**kw)

        # ── Sign-off Checklist ─────────────────────────────────────────────────
        st.divider()
        st.markdown("### Sign-off Checklist")
        st.caption(
            "Review each section below and sign off when complete. "
            "Sign-offs are locked for this session. Export the sign-off sheet "
            "before downloading the full package — it will be included automatically."
        )

        _SIGNOFF_ITEMS = [
            "Bank Reconciliation — Operating",
            "Bank Reconciliation — DACA",
            "Management Fee Invoice",
            "GL vs TB Workpaper Tie-out",
            "Variance Commentary",
            "QC Checklist (7-point)",
            "Equity Tabs (311100 / 331100 / 381100)",
            "Exception Report",
        ]
        _SIGNOFF_REVIEWERS = (_active_cfg.team_members
                              if _active_cfg.team_members
                              else ["Ryan Walsh", "Natasha Parker", "Lauren Sullivan"])

        for _so_idx, _so_item in enumerate(_SIGNOFF_ITEMS):
            _so_existing = st.session_state.signoff_state.get(_so_idx)
            _col_item, _col_rev, _col_btn, _col_status = st.columns([4, 2, 1.2, 3])
            with _col_item:
                st.markdown(f"**{_so_idx + 1}. {_so_item}**")
            if _so_existing:
                with _col_status:
                    st.markdown(
                        f"<span style='color:#2E7D32;font-weight:600;'>"
                        f"✅ {_so_existing['signed_by']} &nbsp;·&nbsp; {_so_existing['timestamp']}"
                        f"</span>",
                        unsafe_allow_html=True,
                    )
            else:
                with _col_rev:
                    _so_reviewer = st.selectbox(
                        "Reviewer", _SIGNOFF_REVIEWERS,
                        key=f"so_rev_{_so_idx}",
                        label_visibility="collapsed",
                    )
                with _col_btn:
                    if st.button("Sign Off", key=f"so_btn_{_so_idx}",
                                 use_container_width=True):
                        st.session_state.signoff_state[_so_idx] = {
                            "signed_by": _so_reviewer,
                            "timestamp": datetime.now().strftime("%m/%d/%Y %H:%M"),
                        }
                        # Auto-detect Close Tracker Step 7 when all sign-offs complete
                        _so_total = len(_SIGNOFF_ITEMS)
                        _so_done  = len(st.session_state.signoff_state)
                        # +1 because we just added one above
                        if (_so_done >= _so_total and
                                7 not in st.session_state.close_tracker):
                            st.session_state.close_tracker[7] = {
                                "completed_by": _so_reviewer,
                                "timestamp":    datetime.now().strftime("%m/%d/%Y %H:%M"),
                                "auto":         True,
                            }
                            _save_checklist_now()
                        st.rerun()
                with _col_status:
                    st.markdown(
                        "<span style='color:#9E9E9E;'>Pending</span>",
                        unsafe_allow_html=True,
                    )

        # Export sign-off sheet
        st.markdown("")
        _so_exp_col, _ = st.columns([2, 5])
        with _so_exp_col:
            if st.button("📄 Export Sign-off Sheet", use_container_width=True,
                         help="Generates GA_Signoff_Record.xlsx and adds it to the ZIP"):
                try:
                    from signoff_generator import generate_signoff_xlsx as _gen_so
                    _so_path = os.path.join(st.session_state.temp_dir,
                                            "GA_Signoff_Record.xlsx")
                    _gen_so(
                        output_path   = _so_path,
                        signoff_state = st.session_state.signoff_state,
                        items         = _SIGNOFF_ITEMS,
                        period        = result.period or close_period,
                        property_name = result.property_name or 'Revolution Labs',
                    )
                    st.session_state.pass2_output_files["signoff_record"] = _so_path
                    st.success("Sign-off sheet exported — included in the ZIP.", icon="✅")
                    st.rerun()
                except Exception as _soe:
                    st.error(f"Sign-off export failed: {_soe}")

        _so_dl_path = p2.get("signoff_record")
        if _so_dl_path and os.path.exists(_so_dl_path):
            with open(_so_dl_path, "rb") as _so_f:
                st.download_button(
                    label="⬇️ Download Sign-off Record",
                    data=_so_f.read(),
                    file_name=f"RevLabs_{period_label}_Signoff_Record.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    # ── Post-Close Adjustments (always visible in Pass 2 tab) ─────────────────
    st.divider()
    st.markdown("### Closing JE Entries")
    st.caption(
        "After reviewing the QC workbook you may identify corrections, missed re-accruals, "
        "or reclassifications that still need to post. Enter them here and download as a "
        "Yardi-import CSV. "
        "Use **Add JE Lines** to append a pre-numbered pair. "
        "Debits must equal Credits for each **JE #** before export."
    )

    # ── Add JE Lines button ────────────────────────────────────────────────────
    # Use _pcje_latest (saved from the previous render's editor output) so that
    # any account codes / amounts the user already typed are preserved when new
    # rows are appended.  Falls back to post_close_je_df on first render.
    if st.button("➕ Add JE Lines", key="pcje_add_btn"):
        import pandas as _pd_pcje_add
        _existing_pcje = st.session_state.get("_pcje_latest", st.session_state.post_close_je_df)
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
            "JE #":             [_next_lbl, _next_lbl],
            "Description":      ["", ""],
            "Account Code":     ["", ""],
            "Debit ($)":        [0.0, 0.0],
            "Credit ($)":       [0.0, 0.0],
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
            "Debit ($)":        st.column_config.NumberColumn("Debit ($)", format="%.2f", min_value=0.0, width="small"),
            "Credit ($)":       st.column_config.NumberColumn("Credit ($)", format="%.2f", min_value=0.0, width="small"),
            "Line Description": st.column_config.TextColumn("Line Description", width="large"),
        },
        key="post_close_je_editor",
    )
    # Save latest edits for use by Add JE Lines on next render.
    # Do NOT feed _pcje_edited back into post_close_je_df here — doing so changes
    # the base DataFrame on every rerender, which causes Streamlit's data_editor
    # to reset its internal delta state and lose whatever the user just typed.
    st.session_state["_pcje_latest"] = _pcje_edited

    _pcje_valid = _pcje_edited[
        _pcje_edited["Account Code"].fillna("").str.strip().astype(bool) &
        ((_pcje_edited["Debit ($)"] != 0) | (_pcje_edited["Credit ($)"] != 0))
    ]

    if not _pcje_valid.empty:
        # ── Validation: each JE # debits must equal credits ──────────────────
        # Only validate a JE when ALL its rows have both account code and amount
        # filled — skip JEs still being entered to avoid mid-entry errors.
        _pcje_errors = []
        for _jn, _grp in _pcje_valid.groupby("JE #"):
            _all_rows = _pcje_edited[_pcje_edited["JE #"] == _jn]
            _incomplete = _all_rows[
                ~_all_rows["Account Code"].fillna("").str.strip().astype(bool) |
                ((_all_rows["Debit ($)"].fillna(0) == 0) &
                 (_all_rows["Credit ($)"].fillna(0) == 0))
            ]
            if not _incomplete.empty:
                continue  # still being filled in — don't flag yet
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

            # ── Build JE lines for CSV export ─────────────────────────────────
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

            # ── Generate CSV ───────────────────────────────────────────────────
            _pcje_csv_path = os.path.join(
                st.session_state.temp_dir, "GA_PostClose_JE.csv"
            )
            _p2er = st.session_state.pass2_engine_result
            _pcje_period   = (_p2er.period        if _p2er else '') or ''
            _pcje_propname = (_p2er.property_name if _p2er else '') or 'revlabspm'
            try:
                from accrual_entry_generator import generate_etl_csv as _gen_etl_pc
                _gen_etl_pc(
                    _pcje_lines,
                    _pcje_csv_path,
                    period=_pcje_period,
                    property_code=_pcje_propname,
                    auto_reverse=False,  # post-close JEs are permanent — no reversal
                )
                st.session_state.pass2_output_files["post_close_je_csv"] = _pcje_csv_path
            except Exception as _pcje_err:
                st.warning(f"Post-close JE CSV generation skipped: {_pcje_err}")

            # ── Download button ────────────────────────────────────────────────
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
| **Yardi Budget Comparison** | Reports → Budget Comparison → PTD + YTD columns, same period | Drives historical pattern accruals and variance commentary |
| **12-Month Income Statement** | Reports → 12-Month Statement → trailing 12 months | Used for historical recurring accruals |
| **Nexus Invoice Detail** | Nexus AP → Export open invoices → .xls format | Open invoices not yet in the GL |
| **Kardin Budget** | Kardin → Export → qryExportData format | Annual budget; used for payroll bonus accruals |
| **Yardi Receivable Detail** | Reports → Receivable Detail → current period | Used to calculate management fee on cash received |
| **Yardi AR Detail Aging** | Reports → AR Aging Detail → current period | Used alongside Receivable Detail to exclude prepayments from the fee basis |
""")
        st.markdown("#### Bank Statements")
        st.markdown("""
| File | Where to get it | Notes |
|------|-----------------|-------|
| **Yardi / PNC Bank Rec** | Yardi Reports → Bank Reconciliation → export as PDF | Preferred source — pipeline reads the pre-computed reconciled balance |
| **KeyBank DACA Statement** | KeyBank online → account x5132 → monthly statement PDF | Used as management fee cash-received basis |
| **BofA Development Statement** | BofA online → development account → monthly PDF | Balance only; development account is dormant |
| **Berkadia Loan Statement(s)** | Berkadia portal → monthly loan statements → PDF (all 3 loans) | ⚠️ Upload the statement **due the 7th of the following month** — e.g. for the January close, upload the Feb 7 statement. Interest is paid in arrears: the Feb 7 payment covers January's interest. |
""")
        st.markdown("#### Reference Files")
        st.markdown("""
| File | Where to get it | Notes |
|------|-----------------|-------|
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
| **GA_Accruals_JE.csv** | All accrual entries: Nexus invoices, utility proration, service accruals, historical recurring, management fee, contract supplements, payroll bonus accruals, tenant utility billings | **Import into Yardi** as a journal batch |
| **GA_Prepaid_Ledger.xlsx** | Updated prepaid amortization schedule with this month's releases applied | **Save** — upload as the prior-month ledger next month |

> The pipeline also shows a **summary table** of every entry generated, grouped by layer
> (Layer 1 Nexus, Layer 2 Proration, Layer 3 Historical, Layer 4 Bonus, etc.) so you can review before posting.
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
| **Berkadia Loan Statements** | Same file as Pass 1 (statement due the 7th of the following month) — re-upload or the pipeline reuses the Pass 1 version |

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

**RE Tax — what to enter each month**
→ Enter the quarterly bill amount every month (all 3 months in each cycle use the same number).
Payment months (Jan/Apr/Jul/Oct): pipeline defers 2/3 → DR 135120 Prepaid RE Taxes / CR 641110.
Release months (Feb/Mar/May/Jun/Aug/Sep/Nov/Dec): pipeline releases 1/3 → DR 641110 / CR 135120.
Leave $0 only if the RE Tax JE has already been posted manually in Yardi.

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


# ──────────────────────────────────────────────────────────────
# TAB 4 — PROPERTIES SETUP
# ──────────────────────────────────────────────────────────────
with tab4:
    from property_writer import (
        build_config_dict, config_to_yaml,
        save_local, save_to_github, github_configured,
    )
    from property_config import discover_properties as _disc_props

    st.markdown("## ⚙️ Property Setup")
    st.markdown(
        "Add or edit properties here. Each property is stored as a YAML config file — "
        "no GitHub access or code changes required."
    )

    if github_configured():
        st.success("✅ GitHub connected — saved configs deploy automatically in ~2 min.", icon="🔗")
    else:
        st.warning(
            "⚠️ GitHub not connected. Configs will be saved locally (dev only) and available "
            "to download. To enable auto-deploy: add `[github]` token + repo to Streamlit secrets.",
            icon="⚠️"
        )

    # ── Existing properties ───────────────────────────────────────────────────
    _existing = _disc_props(str(_DATA_DIR))
    if _existing:
        with st.expander(f"📋 Existing properties ({len(_existing)})", expanded=False):
            for _ep in _existing:
                _ec = _ep['cfg']
                st.markdown(
                    f"**{_ec.display()}** &nbsp; `{_ec.property_code}` &nbsp;|&nbsp; "
                    f"{_ec.property_address} &nbsp;|&nbsp; "
                    f"{'  ·  '.join(f'{fl.name} {fl.rate:.2%}' for fl in _ec.management_fees)}"
                )
    st.markdown("---")

    # ── Add / Edit property form ──────────────────────────────────────────────
    _edit_code = st.selectbox(
        "Edit existing or create new",
        options=["➕ Create new property"] + [p['code'] for p in _existing],
        key="prop_setup_edit_select",
    )
    _is_new = _edit_code == "➕ Create new property"
    _edit_cfg = None if _is_new else next(
        (p['cfg'] for p in _existing if p['code'] == _edit_code), None
    )

    def _ef(field, default=''):
        """Return existing config value or default."""
        if _edit_cfg is None:
            return default
        return getattr(_edit_cfg, field, default) or default

    # ── Building photo upload (outside form — file_uploader can't live in st.form) ──
    st.markdown("### 🏙️ Building Photo")
    st.caption(
        "Upload a photo of the building — displayed in the hero banner for this property. "
        "JPG or PNG, landscape orientation works best (approx 4:1 ratio)."
    )
    _photo_target_code = ('' if _is_new else _edit_code)
    _photo_col1, _photo_col2 = st.columns([2, 1])
    with _photo_col1:
        _hero_upload = st.file_uploader(
            "Building photo (JPG / PNG)",
            type=['jpg', 'jpeg', 'png', 'webp'],
            key='prop_hero_photo_upload',
            help="Saved to GitHub as data/{property_code}/hero.jpg — updates hero banner after ~2 min redeploy.",
        )
        if _hero_upload is not None:
            if not _photo_target_code:
                st.warning("Enter the Property Code above and save the config first, then re-upload the photo.")
            else:
                from property_writer import save_image_to_github as _save_img_gh, save_image_local as _save_img_loc
                _img_bytes = _hero_upload.read()
                _img_ext   = _hero_upload.name.rsplit('.', 1)[-1].lower()
                _img_fname = f'hero.{_img_ext}'
                _loc_ok, _loc_msg = _save_img_loc(
                    _photo_target_code, _img_bytes, _img_fname, str(_DATA_DIR))
                if github_configured():
                    _gh_ok, _gh_msg = _save_img_gh(_photo_target_code, _img_bytes, _img_fname)
                    if _gh_ok:
                        st.success(f"✅ Photo saved to GitHub — hero banner updates after redeploy (~2 min).")
                    else:
                        st.warning(f"GitHub save failed: {_gh_msg}. Saved locally.")
                else:
                    st.info("Photo saved locally. Set up GitHub secrets to persist to Streamlit Cloud.")
    with _photo_col2:
        # Preview current photo for this property
        if not _is_new and _photo_target_code:
            _prev_src = _prop_hero_src(_photo_target_code)
            if _prev_src:
                st.image(_prev_src, caption="Current photo", use_container_width=True)
            else:
                st.caption("No photo yet")

    st.markdown("---")

    with st.form("property_setup_form", clear_on_submit=False):
        st.markdown("### 1 · Basic Information")
        _c1, _c2 = st.columns(2)
        _prop_code    = _c1.text_input("Yardi Property Code *",
                                        value='' if _is_new else _edit_cfg.property_code,
                                        placeholder="e.g. lexlabspm",
                                        help="Short code Yardi uses in GL exports. Lowercase, no spaces.")
        _display_name = _c2.text_input("Display Name *",
                                        value=_ef('property_display_name'),
                                        placeholder="e.g. Lex Labs")
        _prop_name    = st.text_input("Full Legal Entity Name",
                                       value=_ef('property_name'),
                                       placeholder="e.g. Lex Labs Owner, LLC")
        _address      = st.text_input("Property Address",
                                       value=_ef('property_address'),
                                       placeholder="e.g. 100 Main Street, Boston, MA 02101")
        _c3, _c4 = st.columns(2)
        _prop_type = _c3.text_input("Property Type",
                                     value=_ef('property_type'),
                                     placeholder="e.g. Life Science, Office, Industrial")
        _size_sf   = _c4.number_input("Size (SF)", min_value=0,
                                       value=int(_ef('property_size_sf') or 0), step=1000)

        st.markdown("### 2 · Ownership")
        _c5, _c6 = st.columns(2)
        _investor   = _c5.text_input("Investor / Capital Partner",
                                      value=_ef('investor_name'),
                                      placeholder="e.g. Singerman Real Estate")
        _mgmt_co    = _c6.text_input("Management Company",
                                      value=_ef('management_company'),
                                      placeholder="e.g. Greatland Realty Partners")
        _c7, _c8 = st.columns(2)
        _mgmt_code  = _c7.text_input("Management Code (short)",
                                      value=_ef('management_code'),
                                      placeholder="e.g. GRP")
        _inv_prefix = _c8.text_input("Invoice Number Prefix",
                                      value=_ef('invoice_prefix'),
                                      placeholder="e.g. LexLabsPM  →  LexLabsPM012026")

        st.markdown("### 3 · Team Members")
        st.caption(
            "Everyone who works on this property's monthly close. "
            "These names appear in the Dashboard selector, close tracker, and sign-off sheet. "
            "One name per line."
        )
        _default_members = '\n'.join(
            _edit_cfg.team_members if (_edit_cfg and _edit_cfg.team_members)
            else ['Ryan Walsh', 'Natasha Parker', 'Lauren Sullivan']
        )
        _team_text = st.text_area(
            "Team members (one per line)",
            value=_default_members,
            height=120,
            placeholder="Ryan Walsh\nNatasha Parker\nLauren Sullivan\nNew Hire Name",
            label_visibility="collapsed",
        )

        st.markdown("### 4 · Management Fee Lines")
        st.caption("One row per PM agreement line. Leave Name blank to skip a row.")
        _default_fees = [
            {'Name': fl.name, 'Rate (decimal)': fl.rate, 'Minimum ($)': fl.minimum,
             'DR Account': fl.dr_account, 'CR Account': fl.cr_account, 'Ref Prefix': fl.ref_prefix}
            for fl in (_edit_cfg.management_fees if _edit_cfg else [])
        ] or [
            {'Name': 'PM', 'Rate (decimal)': 0.03, 'Minimum ($)': 0.0,
             'DR Account': '637130', 'CR Account': '213100', 'Ref Prefix': 'MGMT-FEE-PM'},
        ]
        _fees_df = pd.DataFrame(_default_fees)
        _fees_edited = st.data_editor(
            _fees_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                'Name':           st.column_config.TextColumn("PM Name", width="small"),
                'Rate (decimal)': st.column_config.NumberColumn("Rate", format="%.4f", min_value=0.0, max_value=1.0),
                'Minimum ($)':    st.column_config.NumberColumn("Min ($)", format="$%.0f", min_value=0.0),
                'DR Account':     st.column_config.TextColumn("DR Acct", width="small"),
                'CR Account':     st.column_config.TextColumn("CR Acct", width="small"),
                'Ref Prefix':     st.column_config.TextColumn("Ref Prefix"),
            },
            key="prop_fees_editor",
        )

        st.markdown("### 5 · Bank Accounts")
        st.caption(
            "One row per bank account. **Slug** = unique key (lowercase, underscores). "
            "**Bank Name** = text that appears in PDF statements. "
            "Account type detected from slug: contains `operat` → operating, `dev` → development, `daca` → DACA."
        )
        _default_banks = [
            {'Slug': slug,
             'Label': ba.label, 'Bank Name': ba.bank_name,
             'Last 4': ba.last4, 'Full Account': ba.full_account, 'GL Account': ba.gl_account}
            for slug, ba in (_edit_cfg.bank_accounts.items() if _edit_cfg else {}.items())
        ] or [
            {'Slug': 'pnc_operating',    'Label': 'PNC Operating',    'Bank Name': 'PNC',           'Last 4': '', 'Full Account': '', 'GL Account': '111100'},
            {'Slug': 'bofa_development', 'Label': 'BofA Development', 'Bank Name': 'Bank of America','Last 4': '', 'Full Account': '', 'GL Account': '111210'},
            {'Slug': 'keybank_daca',     'Label': 'KeyBank DACA',     'Bank Name': 'KeyBank',        'Last 4': '', 'Full Account': '', 'GL Account': '115100'},
        ]
        _banks_edited = st.data_editor(
            pd.DataFrame(_default_banks),
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                'Slug':         st.column_config.TextColumn("Slug *", width="medium"),
                'Label':        st.column_config.TextColumn("Label"),
                'Bank Name':    st.column_config.TextColumn("Bank Name (PDF match)"),
                'Last 4':       st.column_config.TextColumn("Last 4", width="small"),
                'Full Account': st.column_config.TextColumn("Full Account #"),
                'GL Account':   st.column_config.TextColumn("GL Acct", width="small"),
            },
            key="prop_banks_editor",
        )

        st.markdown("### 6 · Payment Instructions (Invoice PDF)")
        _ca, _cb = st.columns(2)
        with _ca:
            st.markdown("**ACH / Wire**")
            _ach = _ef('payment_ach') or {}
            _ach_acct_name  = st.text_input("Account Name",  value=_ach.get('account_name', ''), key="ach_acct_name")
            _ach_bank       = st.text_input("Bank Name",      value=_ach.get('bank_name', ''),    key="ach_bank")
            _ach_acct_num   = st.text_input("Account Number", value=_ach.get('account_number', ''), key="ach_acct_num")
            _ach_routing    = st.text_input("Routing (ABA)",  value=_ach.get('routing_number', ''), key="ach_routing")
            _ach_addr       = st.text_input("Bank Address",   value=_ach.get('bank_address', ''), key="ach_addr")
        with _cb:
            st.markdown("**Check**")
            _chk = _ef('payment_check') or {}
            _chk_payable    = st.text_input("Payable To",      value=_chk.get('payable_to', ''),    key="chk_payable")
            _chk_addr1      = st.text_input("Address Line 1",  value=_chk.get('address_line1', ''), key="chk_addr1")
            _chk_addr2      = st.text_input("Address Line 2",  value=_chk.get('address_line2', ''), key="chk_addr2")
            _chk_attn       = st.text_input("Attention",       value=_chk.get('attention', ''),     key="chk_attn")

        st.markdown("### 7 · RE Tax & Other")
        _c9, _c10 = st.columns(2)
        _retax_months_str = _c9.text_input(
            "RE Tax Payment Months (comma-separated)",
            value=', '.join(str(m) for m in (_edit_cfg.re_tax_payment_months if _edit_cfg else [1, 4, 7, 10])),
            help="Months (1-12) when the quarterly RE tax bill is paid. Typically Jan/Apr/Jul/Oct."
        )
        _parcel_str = _c10.text_input(
            "Parcel IDs (comma-separated, optional)",
            value=', '.join(_edit_cfg.parcel_ids if _edit_cfg else []),
        )
        _c11, _c12 = st.columns(2)
        _kardin_file = _c11.text_input("Kardin Budget Filename",
                                        value=_ef('kardin_budget_file', 'GA_Kardin_Budget_FY2026.xlsx'))
        _file_pfx_del = _c12.text_input("Deliverable File Prefix",
                                          value=_ef('file_prefix_deliverable'),
                                          placeholder="e.g. LexLabs  → LexLabs_Jan2026_Workpapers.xlsx",
                                          help="Leave blank to auto-derive from display name.")

        st.markdown("---")
        _submitted = st.form_submit_button("💾 Save Property Config", type="primary",
                                            use_container_width=True)

    # ── Handle form submission ────────────────────────────────────────────────
    if _submitted:
        _prop_code = (_prop_code or '').strip().lower().replace(' ', '')
        if not _prop_code:
            st.error("Property Code is required.")
        elif not _display_name:
            st.error("Display Name is required.")
        else:
            # Parse fee rows
            _fee_list = []
            for _, _frow in _fees_edited.iterrows():
                _fname = str(_frow.get('Name', '') or '').strip()
                if not _fname:
                    continue
                _fee_list.append({
                    'name':       _fname,
                    'rate':       float(_frow.get('Rate (decimal)', 0) or 0),
                    'minimum':    float(_frow.get('Minimum ($)', 0) or 0),
                    'dr_account': str(_frow.get('DR Account', '637130') or '637130'),
                    'cr_account': str(_frow.get('CR Account', '213100') or '213100'),
                    'ref_prefix': str(_frow.get('Ref Prefix', '') or ''),
                })

            # Parse bank account rows
            _bank_list = []
            for _, _brow in _banks_edited.iterrows():
                _bslug = str(_brow.get('Slug', '') or '').strip().lower().replace(' ', '_')
                if not _bslug:
                    continue
                _bank_list.append({
                    'slug':         _bslug,
                    'label':        str(_brow.get('Label', '') or ''),
                    'bank_name':    str(_brow.get('Bank Name', '') or ''),
                    'last4':        str(_brow.get('Last 4', '') or ''),
                    'full_account': str(_brow.get('Full Account', '') or ''),
                    'gl_account':   str(_brow.get('GL Account', '') or ''),
                })

            # Parse RE tax months
            try:
                _retax_months = [int(m.strip()) for m in _retax_months_str.split(',') if m.strip()]
            except Exception:
                _retax_months = [1, 4, 7, 10]

            # Parse parcel IDs
            _parcels = [p.strip() for p in _parcel_str.split(',') if p.strip()]

            # Build payment dicts
            _payment_ach   = {k: v for k, v in {
                'account_name':   _ach_acct_name,
                'bank_name':      _ach_bank,
                'account_number': _ach_acct_num,
                'routing_number': _ach_routing,
                'bank_address':   _ach_addr,
            }.items() if v}
            _payment_check = {k: v for k, v in {
                'payable_to':    _chk_payable,
                'address_line1': _chk_addr1,
                'address_line2': _chk_addr2,
                'attention':     _chk_attn,
            }.items() if v}

            # Build config dict and render YAML
            # Parse team members from text area (one per line)
            _team_members_parsed = [
                m.strip() for m in _team_text.splitlines() if m.strip()
            ] or ['Ryan Walsh', 'Natasha Parker', 'Lauren Sullivan']

            _cfg_dict = build_config_dict(
                property_code          = _prop_code,
                property_name          = _prop_name,
                property_display_name  = _display_name,
                property_address       = _address,
                property_type          = _prop_type,
                property_size_sf       = int(_size_sf) if _size_sf else None,
                investor_name          = _investor,
                management_company     = _mgmt_co,
                management_code        = _mgmt_code,
                invoice_prefix         = _inv_prefix,
                team_members           = _team_members_parsed,
                management_fees        = _fee_list,
                gl_accounts            = {},
                bank_accounts          = _bank_list,
                payment_ach            = _payment_ach,
                payment_check          = _payment_check,
                re_tax_payment_months  = _retax_months,
                parcel_ids             = _parcels,
                kardin_budget_file     = _kardin_file,
                fiscal_year_start_month = 1,
                file_prefix_internal   = 'GA',
                file_prefix_deliverable = _file_pfx_del,
            )
            _yaml_str = config_to_yaml(_cfg_dict)

            # Save local
            _loc_ok, _loc_msg = save_local(_prop_code, _yaml_str, str(_DATA_DIR))

            # Save to GitHub
            _gh_ok, _gh_msg = False, 'GitHub not configured'
            if github_configured():
                _action = 'Update' if not _is_new else 'Add'
                _gh_ok, _gh_msg = save_to_github(
                    _prop_code, _yaml_str,
                    commit_message=f'{_action} property config: {_prop_code} ({_display_name})'
                )

            # Results
            if _loc_ok:
                st.success(f"✅ Saved locally: `{_loc_msg}`")
            if _gh_ok:
                st.success(f"✅ {_gh_msg}")
            elif github_configured():
                st.error(f"GitHub save failed: {_gh_msg}")

            # Tell the user how to switch to the new property
            if _is_new:
                if _gh_ok:
                    st.info(
                        f"**{_display_name}** will appear in the **sidebar property selector** "
                        f"after Streamlit finishes redeploying (~2 min). "
                        f"Refresh the page and it will be ready to use.",
                        icon="🏢",
                    )
                elif _loc_ok:
                    st.info(
                        f"**{_display_name}** is now available — select it from the "
                        f"**sidebar property selector** at the top of the sidebar. "
                        f"If you don't see it yet, click the refresh button in your browser.",
                        icon="🏢",
                    )
            else:
                st.info(
                    f"**{_display_name}** config updated. "
                    f"{'Reload in ~2 min after Streamlit redeploys.' if _gh_ok else 'Changes are live locally.'}",
                    icon="✅",
                )

            # Always offer download
            st.download_button(
                label="⬇️ Download config.yaml",
                data=_yaml_str.encode('utf-8'),
                file_name=f"{_prop_code}_config.yaml",
                mime="text/yaml",
                help="Upload this file to data/{property_code}/ in GitHub if auto-save failed.",
            )

            # Preview
            with st.expander("📄 Preview generated config.yaml"):
                st.code(_yaml_str, language="yaml")
