"""
property_config.py — Per-asset configuration for the GA Automation Pipeline
===========================================================================
Properties are configured via YAML files at:
    data/{property_code}/config.yaml

The pipeline auto-discovers any folder under data/ that contains a config.yaml.
Adding a new property requires only that file — no code changes.

Usage
-----
    from property_config import PropertyConfig, get_config, list_properties

    cfg = get_config('revlabspm')          # returns PropertyConfig or None
    cfg = get_config_or_default('xyz')     # always returns a PropertyConfig

    # Discover all configured properties (for the sidebar selector)
    props = discover_properties(data_dir)  # list of {'code', 'display_name', 'address', ...}

YAML schema (data/{code}/config.yaml)
--------------------------------------
See data/revlabspm/config.yaml for a fully-annotated example.
Required fields: property_code, property_name
Everything else has sensible defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any


# ── Sub-config dataclasses ────────────────────────────────────────────────────

@dataclass
class ManagementFeeLineConfig:
    """One fee line in the management agreement (e.g. JLL 1.25% or GRP 1.75%)."""
    name:       str                  # e.g. 'JLL' or 'GRP'
    rate:       float                # decimal (0.0125 = 1.25%)
    minimum:    float = 0.0          # dollar minimum (0 = no minimum)
    dr_account: str   = '637130'     # debit account (expense)
    cr_account: str   = '213100'     # credit account (accrued liabilities)
    ref_prefix: str   = ''           # JE reference prefix (e.g. 'MGMT-FEE-JLL')


@dataclass
class BankAccountConfig:
    """One bank account linked to this property."""
    label:        str = ''           # human-readable label (e.g. 'PNC Operating')
    last4:        str = ''           # display suffix (e.g. 'x3993')
    full_account: str = ''           # full account number (for PDF classifier)
    gl_account:   str = ''          # corresponding GL account code


# ── Main PropertyConfig dataclass ─────────────────────────────────────────────

@dataclass
class PropertyConfig:
    """All per-property constants consumed by the pipeline."""

    # ── Yardi identifiers ─────────────────────────────────────────────────────
    property_code:         str       # Yardi short code, e.g. 'revlabspm'
    property_name:         str = ''  # Full legal entity, e.g. 'Revolution Labs Owner, LLC'
    property_display_name: str = ''  # Short UI name, e.g. 'Revolution Labs'
    property_address:      str = ''  # Street address for workpaper headers
    property_type:         str = ''  # e.g. 'Life Science', 'Office', 'Industrial'
    property_size_sf:      Optional[int] = None

    # ── Ownership / branding ──────────────────────────────────────────────────
    investor_name:      str = ''     # Capital partner name
    management_company: str = ''     # e.g. 'Greatland Realty Partners'
    management_code:    str = ''     # Short code for display (e.g. 'GRP')
    invoice_prefix:     str = ''     # Invoice number prefix (e.g. 'RevLabsPM')

    # ── Management fee lines ──────────────────────────────────────────────────
    # Flexible: single-PM properties have one entry; RevLabs has JLL + GRP.
    # Replaces the old management_fee_jll_rate / management_fee_grp_rate fields
    # (those are preserved as computed properties for backward compatibility).
    management_fees: List[ManagementFeeLineConfig] = field(default_factory=list)

    # ── Key GL accounts ───────────────────────────────────────────────────────
    # None = use pipeline defaults.
    gl_accounts: Dict[str, str] = field(default_factory=dict)

    # ── Bank accounts ─────────────────────────────────────────────────────────
    # Keyed by a slug (e.g. 'pnc_operating'). Used by file classifier and
    # workpaper generators for tab labels / account reconciliation.
    bank_accounts: Dict[str, BankAccountConfig] = field(default_factory=dict)

    # ── Invoice payment instructions ──────────────────────────────────────────
    payment_ach:   Dict[str, str] = field(default_factory=dict)
    payment_check: Dict[str, str] = field(default_factory=dict)

    # ── RE tax ───────────────────────────────────────────────────────────────
    # Months where the quarterly tax bill is paid (pipeline defers in these months).
    re_tax_payment_months: List[int] = field(default_factory=lambda: [1, 4, 7, 10])

    # ── Property identifiers ──────────────────────────────────────────────────
    parcel_ids: List[str] = field(default_factory=list)

    # ── Accrual engine settings ───────────────────────────────────────────────
    accrual_materiality_floor: float = 500.00

    # ── Output file prefixes ──────────────────────────────────────────────────
    file_prefix_internal:    str = 'GA'       # e.g. GA_Accruals_JE.csv
    file_prefix_deliverable: str = ''         # e.g. RevLabs_Jan2026_Workpapers.xlsx

    # ── Reference files ───────────────────────────────────────────────────────
    kardin_budget_file:    str = 'GA_Kardin_Budget_FY2026.xlsx'
    fiscal_year_start_month: int = 1

    # ── Chart of Accounts classification (override for non-standard COA) ──────
    coa_revenue_prefixes:    tuple = field(default_factory=lambda: ('4',))
    coa_expense_prefixes:    tuple = field(default_factory=lambda: ('5', '6', '7', '8'))
    coa_bs_asset_prefixes:   tuple = field(default_factory=lambda: ('1',))
    coa_bs_liability_prefixes: tuple = field(default_factory=lambda: ('2',))
    coa_bs_equity_prefixes:  tuple = field(default_factory=lambda: ('3',))

    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, property_code: str, data_dir: str = 'data') -> 'PropertyConfig':
        """
        Load a PropertyConfig from data/{property_code}/config.yaml.

        Raises FileNotFoundError if the config file doesn't exist.
        """
        import yaml
        config_path = os.path.join(data_dir, property_code, 'config.yaml')
        if not os.path.exists(config_path):
            raise FileNotFoundError(f'Property config not found: {config_path}')
        with open(config_path, 'r', encoding='utf-8') as f:
            raw = yaml.safe_load(f) or {}
        return cls._from_dict(raw)

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> 'PropertyConfig':
        """Build a PropertyConfig from a raw YAML dict."""
        # Management fee lines
        fees = []
        for fl in (d.get('management_fees') or []):
            fees.append(ManagementFeeLineConfig(
                name       = str(fl.get('name', '')),
                rate       = float(fl.get('rate', 0.0)),
                minimum    = float(fl.get('minimum', 0.0)),
                dr_account = str(fl.get('dr_account', '637130')),
                cr_account = str(fl.get('cr_account', '213100')),
                ref_prefix = str(fl.get('ref_prefix', '')),
            ))

        # Bank accounts
        banks: Dict[str, BankAccountConfig] = {}
        for slug, ba in (d.get('bank_accounts') or {}).items():
            banks[slug] = BankAccountConfig(
                label        = str(ba.get('label', '')),
                last4        = str(ba.get('last4', '')),
                full_account = str(ba.get('full_account', '')),
                gl_account   = str(ba.get('gl_account', '')),
            )

        # GL accounts (values are strings; YAML may parse as int)
        gl: Dict[str, str] = {
            k: str(v) for k, v in (d.get('gl_accounts') or {}).items()
        }

        return cls(
            property_code         = str(d.get('property_code', '')),
            property_name         = str(d.get('property_name', '')),
            property_display_name = str(d.get('property_display_name', '')),
            property_address      = str(d.get('property_address', '')),
            property_type         = str(d.get('property_type', '')),
            property_size_sf      = d.get('property_size_sf'),
            investor_name         = str(d.get('investor_name', '')),
            management_company    = str(d.get('management_company', '')),
            management_code       = str(d.get('management_code', '')),
            invoice_prefix        = str(d.get('invoice_prefix', '')),
            management_fees       = fees,
            gl_accounts           = gl,
            bank_accounts         = banks,
            payment_ach           = d.get('payment_ach') or {},
            payment_check         = d.get('payment_check') or {},
            re_tax_payment_months = list(d.get('re_tax_payment_months') or [1, 4, 7, 10]),
            parcel_ids            = list(d.get('parcel_ids') or []),
            accrual_materiality_floor = float(d.get('accrual_materiality_floor', 500.0)),
            file_prefix_internal    = str(d.get('file_prefix_internal', 'GA')),
            file_prefix_deliverable = str(d.get('file_prefix_deliverable', '')),
            kardin_budget_file      = str(d.get('kardin_budget_file', 'GA_Kardin_Budget_FY2026.xlsx')),
            fiscal_year_start_month = int(d.get('fiscal_year_start_month', 1)),
        )

    # ── Computed properties (backward compatibility + convenience) ────────────

    @property
    def total_management_fee_rate(self) -> float:
        """Sum of all management fee rates."""
        return sum(f.rate for f in self.management_fees)

    @property
    def management_fee_jll_rate(self) -> float:
        """Backward-compat: JLL rate (first fee line named 'JLL', else 0)."""
        for f in self.management_fees:
            if f.name.upper() == 'JLL':
                return f.rate
        return 0.0

    @property
    def management_fee_grp_rate(self) -> float:
        """Backward-compat: GRP rate (first fee line named 'GRP', else 0)."""
        for f in self.management_fees:
            if f.name.upper() == 'GRP':
                return f.rate
        return 0.0

    @property
    def gl(self) -> Dict[str, str]:
        """Shorthand for gl_accounts dict."""
        return self.gl_accounts

    def gl_account(self, key: str, default: str = '') -> str:
        """Look up a GL account code by key (e.g. 'cash_operating')."""
        return self.gl_accounts.get(key, default)

    def display(self) -> str:
        """Best available display name."""
        return self.property_display_name or self.property_name or self.property_code

    def deliverable_prefix(self) -> str:
        """Display-name-derived prefix if file_prefix_deliverable not set."""
        if self.file_prefix_deliverable:
            return self.file_prefix_deliverable
        # Derive from display name: 'Revolution Labs' → 'RevLabs'
        parts = (self.property_display_name or self.property_code or 'Property').split()
        return ''.join(p[:4].capitalize() for p in parts[:2])

    @property
    def coa_bs_prefixes(self) -> tuple:
        return self.coa_bs_asset_prefixes + self.coa_bs_liability_prefixes + self.coa_bs_equity_prefixes

    # ── COA classification helpers ────────────────────────────────────────────

    def is_revenue(self, account_code: str) -> bool:
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_revenue_prefixes)

    def is_expense(self, account_code: str) -> bool:
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_expense_prefixes)

    def is_balance_sheet(self, account_code: str) -> bool:
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_bs_prefixes)

    def is_income_statement(self, account_code: str) -> bool:
        return self.is_revenue(account_code) or self.is_expense(account_code)


# ── Property discovery ────────────────────────────────────────────────────────

def discover_properties(data_dir: str) -> List[Dict[str, Any]]:
    """
    Scan data_dir for subfolders containing config.yaml.
    Returns a list of dicts with 'code', 'display_name', 'address', 'cfg'.
    Sorted alphabetically by display name.

    Usage (sidebar selector):
        props = discover_properties(str(_DATA_DIR))
        codes = [p['code'] for p in props]
        names = {p['code']: p['display_name'] for p in props}
    """
    results = []
    if not os.path.isdir(data_dir):
        return results
    for entry in sorted(os.scandir(data_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        cfg_path = os.path.join(entry.path, 'config.yaml')
        if not os.path.exists(cfg_path):
            continue
        try:
            cfg = PropertyConfig.load(entry.name, data_dir)
            results.append({
                'code':         cfg.property_code or entry.name,
                'display_name': cfg.display(),
                'address':      cfg.property_address,
                'property_type': cfg.property_type,
                'size_sf':      cfg.property_size_sf,
                'cfg':          cfg,
            })
        except Exception:
            pass
    return results


def load_property_config(property_code: str, data_dir: str = 'data') -> PropertyConfig:
    """
    Load config from data/{property_code}/config.yaml.
    Falls back to a minimal default if the file doesn't exist.
    """
    try:
        return PropertyConfig.load(property_code, data_dir)
    except Exception:
        return _legacy_registry_fallback(property_code)


def _legacy_registry_fallback(property_code: str) -> PropertyConfig:
    """
    Return a PropertyConfig for known properties even without a config.yaml.
    This preserves backward compatibility during the transition period.
    """
    if str(property_code).lower() == 'revlabspm':
        return PropertyConfig(
            property_code         = 'revlabspm',
            property_name         = 'Revolution Labs Owner, LLC',
            property_display_name = 'Revolution Labs',
            property_address      = '1050 Waltham Street, Lexington, MA',
            investor_name         = 'Singerman Real Estate',
            management_company    = 'Greatland Realty Partners',
            management_code       = 'GRP',
            invoice_prefix        = 'RevLabsPM',
            management_fees       = [
                ManagementFeeLineConfig('JLL', 0.0125, 5000.0, '637130', '213100', 'MGMT-FEE-JLL'),
                ManagementFeeLineConfig('GRP', 0.0175, 0.0,    '637130', '213100', 'MGMT-FEE-GRP'),
            ],
        )
    return PropertyConfig(property_code=property_code or 'unknown')


# ── Public API (backward-compatible) ─────────────────────────────────────────

# Module-level data_dir for get_config() — resolved relative to this file.
_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DATA_DIR = os.path.join(_HERE, '..', 'data')


def get_config(property_code: str) -> Optional[PropertyConfig]:
    """Return the PropertyConfig for property_code, or None if not found."""
    if not property_code:
        return None
    return load_property_config(str(property_code).strip().lower(), _DEFAULT_DATA_DIR)


def get_config_or_default(property_code: str) -> PropertyConfig:
    """Return PropertyConfig, falling back to a minimal default."""
    return get_config(property_code) or PropertyConfig(property_code=property_code or 'unknown')


def list_properties(data_dir: Optional[str] = None) -> List[str]:
    """Return list of property codes discovered from data_dir."""
    d = data_dir or _DEFAULT_DATA_DIR
    props = discover_properties(d)
    return [p['code'] for p in props] if props else ['revlabspm']


_DEFAULT_CONFIG = PropertyConfig(property_code='unknown')


# ── COA convenience functions ─────────────────────────────────────────────────

def is_revenue_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    return (cfg or _DEFAULT_CONFIG).is_revenue(account_code)


def is_expense_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    return (cfg or _DEFAULT_CONFIG).is_expense(account_code)


def is_balance_sheet_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    return (cfg or _DEFAULT_CONFIG).is_balance_sheet(account_code)


def is_income_statement_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    return (cfg or _DEFAULT_CONFIG).is_income_statement(account_code)
