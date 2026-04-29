"""
property_config.py — Per-asset configuration for the GA Automation Pipeline
===========================================================================
Every property managed by GRP has its own configuration block here.
Callers retrieve the config via ``get_config(property_code)`` and use the
returned ``PropertyConfig`` object instead of hardcoding property-specific
values.

Adding a new property
---------------------
1. Copy the ``revlabpm`` entry below and update every field.
2. Set the management fee rates to whatever the PM/AM agreement specifies.
3. Run a test close to verify the bank-rec parser can detect the GL section
   (the ``property_code`` prefix must match what Yardi prints in the GL PDF).

Callers should always fall back gracefully when config is absent:
    cfg = get_config(code) or PropertyConfig(property_code=code)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict


# ── PropertyConfig dataclass ──────────────────────────────────────────────────

@dataclass
class PropertyConfig:
    """All per-property constants consumed by the pipeline."""

    # Yardi identifiers
    property_code: str                  # Yardi short code printed in GL/bank-rec PDFs
    property_name: str = ''            # Full legal entity name (e.g. "Revolution Labs Owner, LLC")
    property_display_name: str = ''    # Short UI/header name (e.g. "Revolution Labs")
    property_address: str = ''         # Street address for workpaper headers

    # Investor / deliverable branding
    investor_name: str = 'Singerman Real Estate'   # Capital partner name

    # Management fee rates
    management_fee_jll_rate: float = 0.0125   # 1.25% JLL (current PM)
    management_fee_grp_rate: float = 0.0175   # 1.75% GRP (replacement PM)

    # Key GL account overrides (None = use pipeline defaults)
    # Set these only when a property uses non-standard chart-of-accounts numbering.
    cash_operating_account: Optional[str] = None    # default: '111100'
    mgmt_fee_expense_account: Optional[str] = None  # default: '637130'
    ap_accrual_account: Optional[str] = None        # default: '211300'

    # Accrual engine settings
    accrual_materiality_floor: float = 500.00   # suppress entries below this dollar amount

    # ── Chart of Accounts classification ranges ───────────────────────────────
    # These control how the pipeline classifies account codes as revenue, expense,
    # or balance-sheet.  The defaults match the standard Yardi COA used at
    # Revolution Labs.  Override for properties with non-standard numbering.
    #
    # Each tuple contains the leading digit(s) that identify that account type.
    # Matching is done via str.startswith(), so ('4',) matches 4xxxxx accounts.
    coa_revenue_prefixes: tuple = field(default_factory=lambda: ('4',))
    coa_expense_prefixes: tuple = field(default_factory=lambda: ('5', '6', '7', '8'))
    coa_bs_asset_prefixes: tuple = field(default_factory=lambda: ('1',))
    coa_bs_liability_prefixes: tuple = field(default_factory=lambda: ('2',))
    coa_bs_equity_prefixes: tuple = field(default_factory=lambda: ('3',))

    @property
    def coa_bs_prefixes(self) -> tuple:
        """All balance-sheet account prefixes (assets + liabilities + equity)."""
        return self.coa_bs_asset_prefixes + self.coa_bs_liability_prefixes + self.coa_bs_equity_prefixes

    @property
    def total_management_fee_rate(self) -> float:
        return self.management_fee_jll_rate + self.management_fee_grp_rate

    def display(self) -> str:
        """Return the best available display name."""
        return self.property_display_name or self.property_name or self.property_code

    # ── COA classification helpers ────────────────────────────────────────────

    def is_revenue(self, account_code: str) -> bool:
        """Return True if account_code is a revenue account for this property."""
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_revenue_prefixes)

    def is_expense(self, account_code: str) -> bool:
        """Return True if account_code is an expense account for this property."""
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_expense_prefixes)

    def is_balance_sheet(self, account_code: str) -> bool:
        """Return True if account_code is a BS account (asset, liability, or equity)."""
        code = str(account_code or '').strip()
        return any(code.startswith(p) for p in self.coa_bs_prefixes)

    def is_income_statement(self, account_code: str) -> bool:
        """Return True if account_code is a P&L account (revenue or expense)."""
        return self.is_revenue(account_code) or self.is_expense(account_code)


# ── Property registry ─────────────────────────────────────────────────────────

_REGISTRY: Dict[str, PropertyConfig] = {

    'revlabpm': PropertyConfig(
        property_code         = 'revlabpm',
        property_name         = 'Revolution Labs Owner, LLC',
        property_display_name = 'Revolution Labs',
        property_address      = '275 Grove Street, Newton, MA 02466',
        investor_name         = 'Singerman Real Estate',
        management_fee_jll_rate = 0.0125,   # 1.25%
        management_fee_grp_rate = 0.0175,   # 1.75%
    ),

    # ── Add new properties below this line ────────────────────────────────────
    # Example:
    # 'nextproppm': PropertyConfig(
    #     property_code         = 'nextproppm',
    #     property_name         = 'Next Property Owner, LLC',
    #     property_display_name = 'Next Property',
    #     property_address      = '123 Main Street, Boston, MA 02101',
    #     investor_name         = 'Singerman Real Estate',
    #     management_fee_jll_rate = 0.0125,
    #     management_fee_grp_rate = 0.0175,
    # ),

}


# ── Public API ────────────────────────────────────────────────────────────────

_DEFAULT_CONFIG = PropertyConfig(property_code='unknown')


def get_config(property_code: str) -> Optional[PropertyConfig]:
    """
    Return the ``PropertyConfig`` for *property_code*, or ``None`` if not found.

    Usage::

        cfg = get_config(property_code) or PropertyConfig(property_code=property_code)
    """
    if not property_code:
        return None
    return _REGISTRY.get(str(property_code).strip().lower())


def get_config_or_default(property_code: str) -> PropertyConfig:
    """
    Return the ``PropertyConfig`` for *property_code*, falling back to a minimal
    default with only the property_code set (no rates, no display names).

    Use this when the pipeline must continue even for unconfigured properties.
    """
    return get_config(property_code) or PropertyConfig(property_code=property_code or 'unknown')


def list_properties() -> list[str]:
    """Return the list of registered property codes."""
    return list(_REGISTRY.keys())


# ── COA convenience functions (use default config when no property is known) ──
#
# These replace the hardcoded first-digit checks scattered throughout the
# pipeline (e.g. ``startswith('4')``, ``first_digit in ('5','6','7','8')``).
# Call with an explicit cfg when the property is known; fall back to defaults
# when only the account code is available.

def is_revenue_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    """True if *account_code* is a revenue account under *cfg* (or default COA)."""
    return (cfg or _DEFAULT_CONFIG).is_revenue(account_code)


def is_expense_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    """True if *account_code* is an expense account under *cfg* (or default COA)."""
    return (cfg or _DEFAULT_CONFIG).is_expense(account_code)


def is_balance_sheet_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    """True if *account_code* is a balance-sheet account under *cfg* (or default COA)."""
    return (cfg or _DEFAULT_CONFIG).is_balance_sheet(account_code)


def is_income_statement_account(account_code: str, cfg: Optional[PropertyConfig] = None) -> bool:
    """True if *account_code* is a P&L account (revenue or expense) under *cfg* (or default COA)."""
    return (cfg or _DEFAULT_CONFIG).is_income_statement(account_code)
