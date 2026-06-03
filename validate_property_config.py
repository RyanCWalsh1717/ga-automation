#!/usr/bin/env python3
"""
validate_property_config.py — GRP Pipeline Property Config Validator
======================================================================
Checks a property config.yaml for errors before it goes anywhere near
a live close cycle.  Run this after creating or editing a config, and
before pushing to GitHub.

Usage:
    python validate_property_config.py data/lexlabspm/config.yaml
    python validate_property_config.py lexlabspm
    python validate_property_config.py          # validates ALL properties in data/

Exit codes:
    0  — no errors (warnings are OK)
    1  — one or more errors found

Output:
    Each check prints ✓ (pass), ✗ (error), or ⚠ (warning).
    Errors must be fixed before first close.
    Warnings should be reviewed but won't block the pipeline.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Force UTF-8 output on Windows so box-drawing / emoji chars render correctly
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


# ── ANSI colours ──────────────────────────────────────────────────────────────

NO_COLOR = '--no-color' in sys.argv or os.environ.get('NO_COLOR')

def _c(text: str, code: str) -> str:
    return text if NO_COLOR else f'\033[{code}m{text}\033[0m'

GREEN  = lambda t: _c(t, '32')
RED    = lambda t: _c(t, '31')
YELLOW = lambda t: _c(t, '33')
CYAN   = lambda t: _c(t, '36')
BOLD   = lambda t: _c(t, '1')
DIM    = lambda t: _c(t, '2')


# ── Finding accumulator ───────────────────────────────────────────────────────

class Findings:
    def __init__(self):
        self._items: List[Tuple[str, str, str]] = []   # (level, section, message)

    def ok(self, section: str, msg: str):
        self._items.append(('ok', section, msg))

    def warn(self, section: str, msg: str):
        self._items.append(('warn', section, msg))

    def error(self, section: str, msg: str):
        self._items.append(('error', section, msg))

    def info(self, section: str, msg: str):
        self._items.append(('info', section, msg))

    def errors(self) -> List[str]:
        return [msg for level, _, msg in self._items if level == 'error']

    def warnings(self) -> List[str]:
        return [msg for level, _, msg in self._items if level == 'warn']

    def print_section(self, section: str):
        items = [(lv, msg) for lv, sec, msg in self._items if sec == section]
        if not items:
            return
        print(f'\n  {BOLD(CYAN(f"[{section}]"))}')
        for level, msg in items:
            if level == 'ok':
                print(f'    {GREEN("✓")} {msg}')
            elif level == 'warn':
                print(f'    {YELLOW("⚠")} {msg}')
            elif level == 'error':
                print(f'    {RED("✗")} {msg}')
            else:
                print(f'    {DIM("·")} {msg}')

    def print_summary(self) -> bool:
        """Print totals. Returns True if no errors."""
        nerr  = len(self.errors())
        nwarn = len(self.warnings())
        print()
        print('  ' + '─' * 54)
        if nerr == 0:
            status = GREEN('✓ Config is valid — safe to push to GitHub')
        else:
            status = RED(f'✗ Fix {nerr} error(s) before first close')
        err_s  = RED(f'{nerr} error(s)') if nerr else DIM('0 errors')
        warn_s = YELLOW(f'{nwarn} warning(s)') if nwarn else DIM('0 warnings')
        print(f'  {err_s}   {warn_s}')
        print(f'  {status}')
        print('  ' + '─' * 54)
        return nerr == 0


# ── Helpers ───────────────────────────────────────────────────────────────────

_GL_RE = re.compile(r'^\d{6}$')

def _is_valid_gl(code: Any) -> bool:
    return bool(_GL_RE.match(str(code or '').strip()))


def _check_gl(f: Findings, section: str, field_name: str, code: Any):
    s = str(code or '').strip()
    if not s:
        f.warn(section, f'{field_name}: not set (pipeline will use default)')
    elif not _GL_RE.match(s):
        f.error(section, f'{field_name}: "{s}" is not a valid 6-digit GL account')
    else:
        f.ok(section, f'{field_name}: {s}')


# ── Individual validators ─────────────────────────────────────────────────────

def _check_identity(f: Findings, d: Dict, folder_code: str):
    sec = 'IDENTITY'

    code = str(d.get('property_code', '') or '').strip()
    if not code:
        f.error(sec, 'property_code: MISSING — required field')
    elif code == 'CHANGE_ME':
        f.error(sec, 'property_code: still set to CHANGE_ME placeholder')
    elif code != folder_code:
        f.error(sec, f'property_code "{code}" does not match folder name "{folder_code}"')
    else:
        f.ok(sec, f'property_code: {code}')

    name = str(d.get('property_name', '') or '').strip()
    if not name:
        f.error(sec, 'property_name: MISSING — required field (legal entity name)')
    else:
        f.ok(sec, f'property_name: {name}')

    display = str(d.get('property_display_name', '') or '').strip()
    f.ok(sec, f'property_display_name: {display or DIM("(not set — will use property_name)")}')

    etl = str(d.get('yardi_etl_code', '') or '').strip()
    if etl and len(etl) > 8:
        f.error(sec, f'yardi_etl_code: "{etl}" is {len(etl)} chars — Yardi max is 8')
    elif etl:
        f.ok(sec, f'yardi_etl_code: {etl} ({len(etl)} chars)')
    else:
        f.warn(sec, 'yardi_etl_code: not set — property_code will be used (truncated to 8)')

    investor = str(d.get('investor_name', '') or '').strip()
    if not investor:
        f.warn(sec, 'investor_name: not set — variance comments will use generic "capital partner"')
    else:
        f.ok(sec, f'investor_name: {investor}')

    prefix = str(d.get('file_prefix_deliverable', '') or '').strip()
    if not prefix:
        f.warn(sec, 'file_prefix_deliverable: not set — deliverable files will use "GA_" prefix')
    else:
        f.ok(sec, f'file_prefix_deliverable: {prefix}')


def _check_management_fees(f: Findings, d: Dict):
    sec = 'MANAGEMENT FEES'
    fees = d.get('management_fees') or []
    if not fees:
        f.warn(sec, 'management_fees: empty — no automated fee calculation will run')
        return

    total_rate = 0.0
    for i, fl in enumerate(fees, 1):
        name = str(fl.get('name', f'Fee {i}'))
        rate = fl.get('rate', 0)
        try:
            rate = float(rate)
        except (TypeError, ValueError):
            f.error(sec, f'{name}: rate "{rate}" is not a number')
            rate = 0.0

        if rate <= 0:
            f.error(sec, f'{name}: rate must be > 0 (got {rate})')
        elif rate > 0.10:
            f.warn(sec, f'{name}: rate {rate:.2%} seems unusually high (> 10%) — double check')
        total_rate += rate   # accumulate regardless of warning so total is always correct

        minimum = fl.get('minimum', 0)
        try:
            minimum = float(minimum)
        except (TypeError, ValueError):
            f.error(sec, f'{name}: minimum "{minimum}" is not a number')

        dr = str(fl.get('dr_account', '') or '').strip()
        cr = str(fl.get('cr_account', '') or '').strip()
        if dr and not _is_valid_gl(dr):
            f.error(sec, f'{name}: dr_account "{dr}" is not a valid 6-digit GL account')
        if cr and not _is_valid_gl(cr):
            f.error(sec, f'{name}: cr_account "{cr}" is not a valid 6-digit GL account')

    if total_rate > 0:
        names = ' + '.join(
            f'{fl.get("name", "?")} ({float(fl.get("rate", 0)):.2%})'
            for fl in fees
            if float(fl.get('rate', 0)) > 0
        )
        f.ok(sec, f'{len(fees)} fee line(s): {names} = {total_rate:.2%} total')


def _check_gl_accounts(f: Findings, d: Dict):
    sec = 'GL ACCOUNTS'
    gl = d.get('gl_accounts') or {}
    if not gl:
        f.info(sec, 'gl_accounts: not set — pipeline uses standard COA defaults')
        return

    bad = []
    for key, val in gl.items():
        if not _is_valid_gl(val):
            bad.append(f'{key}: "{val}"')
    if bad:
        for b in bad:
            f.error(sec, f'{b} is not a valid 6-digit GL account')
    else:
        f.ok(sec, f'{len(gl)} GL account override(s) — all 6-digit format')


def _check_bank_accounts(f: Findings, d: Dict):
    sec = 'BANK ACCOUNTS'
    banks = d.get('bank_accounts') or {}
    if not banks:
        f.warn(sec, 'bank_accounts: empty — bank rec and file classification may not work')
        return

    for slug, ba in banks.items():
        last4 = str(ba.get('last4', '') or '').strip()
        gl    = str(ba.get('gl_account', '') or '').strip()
        label = str(ba.get('label', slug))

        if last4 in ('x0000', ''):
            f.warn(sec, f'{label}: last4 is still placeholder "{last4}" — update with real account suffix')
        else:
            ok_parts = [label, last4]
            if gl:
                if not _is_valid_gl(gl):
                    f.error(sec, f'{label}: gl_account "{gl}" is not 6 digits')
                else:
                    ok_parts.append(f'→ {gl}')
            f.ok(sec, '  '.join(ok_parts))


def _check_retax(f: Findings, d: Dict):
    sec = 'RE TAX'
    months = d.get('re_tax_payment_months') or []
    if not months:
        f.warn(sec, 're_tax_payment_months: not set — using default [1, 4, 7, 10] (quarterly)')
        return

    try:
        months = [int(m) for m in months]
    except (TypeError, ValueError):
        f.error(sec, 're_tax_payment_months: must be a list of integers (e.g. [1, 4, 7, 10])')
        return

    invalid = [m for m in months if m < 1 or m > 12]
    if invalid:
        f.error(sec, f're_tax_payment_months: invalid months {invalid} — must be 1–12')
        return

    if len(months) != len(set(months)):
        f.error(sec, 're_tax_payment_months: contains duplicate months')
        return

    schedules = {4: 'quarterly', 2: 'semi-annual', 12: 'monthly', 1: 'annual'}
    sched = schedules.get(len(months), f'{len(months)}-times/year')
    f.ok(sec, f'payment months: {sorted(months)} — {sched} schedule')


def _check_insurance(f: Findings, d: Dict):
    sec = 'INSURANCE'
    policies = d.get('insurance_policies') or []
    if not policies:
        f.info(sec, 'insurance_policies: not set — pipeline uses budget-driven detection (Mode C)')
        return

    total = 0.0
    zero_policies = []
    for p in policies:
        name    = str(p.get('name', '?'))
        acct    = str(p.get('expense_account', '') or '').strip()
        monthly = float(p.get('monthly_amount', 0) or 0)

        if not _is_valid_gl(acct):
            f.error(sec, f'"{name}": expense_account "{acct}" is not a valid 6-digit GL account')
        if monthly <= 0:
            zero_policies.append(name)
        else:
            total += monthly

    if zero_policies:
        f.warn(sec, f'monthly_amount is $0.00 for: {", ".join(zero_policies)} — update before first close')
    if total > 0:
        policy_strs = [
            f'{p.get("name", "?")} ${float(p.get("monthly_amount", 0)):,.2f}'
            for p in policies
            if float(p.get('monthly_amount', 0) or 0) > 0
        ]
        f.ok(sec, f'{len(policies)} polic(ies): {" + ".join(policy_strs)} = ${total:,.2f}/mo')


def _check_accrual_settings(f: Findings, d: Dict):
    sec = 'ACCRUAL SETTINGS'

    floor = d.get('accrual_materiality_floor')
    if floor is None:
        f.info(sec, 'accrual_materiality_floor: not set — using default ($2,500)')
    else:
        try:
            floor = float(floor)
            if floor < 0:
                f.error(sec, f'accrual_materiality_floor: must be >= 0 (got {floor})')
            elif floor < 500:
                f.warn(sec, f'accrual_materiality_floor: ${floor:,.0f} seems very low — Layer 3 may generate many small accruals')
            else:
                f.ok(sec, f'accrual_materiality_floor: ${floor:,.2f}')
        except (TypeError, ValueError):
            f.error(sec, f'accrual_materiality_floor: "{floor}" is not a number')

    excludes = d.get('layer3_exclude_accounts') or []
    bad_excl = [str(c) for c in excludes if not _is_valid_gl(c)]
    if bad_excl:
        f.error(sec, f'layer3_exclude_accounts: invalid GL codes {bad_excl}')
    elif excludes:
        f.ok(sec, f'layer3_exclude_accounts: {len(excludes)} account(s) — {", ".join(str(c) for c in excludes)}')

    periodic = d.get('periodic_contract_accounts') or {}
    if isinstance(periodic, dict):
        bad_p = [k for k in periodic if not _is_valid_gl(k)]
        if bad_p:
            f.error(sec, f'periodic_contract_accounts: invalid GL codes {bad_p}')
        elif periodic:
            f.ok(sec, f'periodic_contract_accounts: {len(periodic)} account(s) override')
    elif periodic:
        f.error(sec, 'periodic_contract_accounts: must be a dict (see TEMPLATE for format)')

    payroll = d.get('payroll_accounts') or []
    bad_pr = [str(c) for c in payroll if not _is_valid_gl(c)]
    if bad_pr:
        f.error(sec, f'payroll_accounts: invalid GL codes {bad_pr}')
    elif payroll:
        f.ok(sec, f'payroll_accounts: {len(payroll)} account(s) — {", ".join(str(c) for c in payroll)}')

    metered = d.get('metered_utility_accounts') or []
    bad_mu = [str(c) for c in metered if not _is_valid_gl(c)]
    if bad_mu:
        f.error(sec, f'metered_utility_accounts: invalid GL codes {bad_mu}')
    elif metered:
        f.ok(sec, f'metered_utility_accounts: {metered}')


def _check_qc_thresholds(f: Findings, d: Dict):
    sec = 'QC THRESHOLDS'
    thr = d.get('qc_thresholds') or {}
    if not thr:
        f.info(sec, 'qc_thresholds: not set — firm-wide defaults apply ($5K / 5% / $2.5K / $10K MoM)')
        return

    t1a = thr.get('tier1_abs', 5000.0)
    t1p = thr.get('tier1_pct', 0.05)
    t2m = thr.get('tier2_min', 2500.0)
    mom = thr.get('mom_swing', 10000.0)

    try:
        t1a, t1p, t2m, mom = float(t1a), float(t1p), float(t2m), float(mom)
    except (TypeError, ValueError) as e:
        f.error(sec, f'qc_thresholds: non-numeric value — {e}')
        return

    _thr_errors_before = len(f.errors())
    if t2m >= t1a:
        f.error(sec, f'qc_thresholds: tier2_min (${t2m:,.0f}) must be < tier1_abs (${t1a:,.0f})')
    if t1p > 1.0:
        f.error(sec, f'qc_thresholds: tier1_pct {t1p} should be a decimal (0.05 = 5%), not a percentage (5.0)')
    if any(v < 0 for v in [t1a, t1p, t2m, mom]):
        f.error(sec, 'qc_thresholds: all values must be >= 0')

    if len(f.errors()) == _thr_errors_before:   # no NEW errors in this section
        f.ok(sec, f'Tier 1: ≥${t1a:,.0f} or ≥{t1p:.0%} | Tier 2: ${t2m:,.0f}–${t1a:,.0f} | MoM swing: ${mom:,.0f}')


def _check_ai_context(f: Findings, d: Dict):
    sec = 'AI CONTEXT'
    ctx = d.get('ai_account_context') or {}
    if not ctx:
        f.info(sec, 'ai_account_context: not set — AI will use global account notes only')
        return

    bad = [k for k in ctx if not _is_valid_gl(k)]
    if bad:
        f.error(sec, f'ai_account_context: invalid GL account keys {bad}')
    else:
        f.ok(sec, f'ai_account_context: {len(ctx)} property-specific account hint(s) — {", ".join(ctx.keys())}')


def _check_kardin(f: Findings, d: Dict, config_dir: Path):
    sec = 'KARDIN BUDGET'
    fname = str(d.get('kardin_budget_file', '') or '').strip()
    if not fname:
        f.warn(sec, 'kardin_budget_file: not set — Layer 3 historical accruals will have no Kardin cross-check')
        return

    budget_path = config_dir / fname
    if budget_path.exists():
        size_kb = budget_path.stat().st_size // 1024
        f.ok(sec, f'{fname} found ({size_kb} KB)')
    else:
        f.error(sec, f'{fname}: file not found at {budget_path} — upload it before first close')


def _check_pipeline_load(f: Findings, config_path: Path):
    """Actually load the config via property_config.py to catch deserialization errors."""
    sec = 'PIPELINE LOAD TEST'
    try:
        # Find pipeline/ directory relative to this script
        here = Path(__file__).parent
        pipeline_dir = here / 'pipeline'
        if str(pipeline_dir) not in sys.path:
            sys.path.insert(0, str(pipeline_dir))

        from property_config import PropertyConfig
        import yaml
        with open(config_path, 'r', encoding='utf-8') as fh:
            raw = yaml.safe_load(fh) or {}
        cfg = PropertyConfig._from_dict(raw)
        f.ok(sec, f'PropertyConfig loaded — {cfg.property_code} / {cfg.property_name}')
        if cfg.management_fees:
            total = sum(float(getattr(fl, 'rate', 0) or 0) for fl in cfg.management_fees)
            f.ok(sec, f'Total management fee rate: {total:.2%}')
    except ImportError:
        f.warn(sec, 'property_config.py not on path — skipping pipeline load test (run from ga-automation/ root)')
    except Exception as e:
        f.error(sec, f'PropertyConfig._from_dict() raised: {e}')


# ── Main validator ────────────────────────────────────────────────────────────

def validate_config(config_path: Path) -> bool:
    """
    Validate one config.yaml.  Returns True if no errors.
    """
    folder_code = config_path.parent.name

    print()
    print('  ' + '═' * 54)
    print(f'  {BOLD("GRP Property Config Validator")} — {CYAN(folder_code)}')
    print(f'  {DIM(str(config_path))}')
    print('  ' + '═' * 54)

    # ── Parse YAML ────────────────────────────────────────────
    try:
        import yaml
        with open(config_path, 'r', encoding='utf-8') as fh:
            d = yaml.safe_load(fh) or {}
    except ImportError:
        print(f'\n  {RED("✗")} pyyaml not installed — run: pip install pyyaml')
        return False
    except Exception as e:
        print(f'\n  {RED("✗")} YAML parse error: {e}')
        return False

    if not d:
        print(f'\n  {RED("✗")} Config file is empty')
        return False

    # ── Skip TEMPLATE folder ───────────────────────────────────
    if folder_code.upper() == 'TEMPLATE':
        print(f'\n  {YELLOW("⚠")} Skipping TEMPLATE folder — not a real property')
        return True

    # ── Run all checks ─────────────────────────────────────────
    f = Findings()
    _check_identity(f, d, folder_code)
    _check_management_fees(f, d)
    _check_gl_accounts(f, d)
    _check_bank_accounts(f, d)
    _check_retax(f, d)
    _check_insurance(f, d)
    _check_accrual_settings(f, d)
    _check_qc_thresholds(f, d)
    _check_ai_context(f, d)
    _check_kardin(f, d, config_path.parent)
    _check_pipeline_load(f, config_path)

    # ── Print results by section ──────────────────────────────
    for section in [
        'IDENTITY', 'MANAGEMENT FEES', 'GL ACCOUNTS', 'BANK ACCOUNTS',
        'RE TAX', 'INSURANCE', 'ACCRUAL SETTINGS', 'QC THRESHOLDS',
        'AI CONTEXT', 'KARDIN BUDGET', 'PIPELINE LOAD TEST',
    ]:
        f.print_section(section)

    return f.print_summary()


def find_configs(data_dir: Path) -> List[Path]:
    """Return all config.yaml files under data_dir."""
    return sorted(data_dir.glob('*/config.yaml'))


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    here = Path(__file__).parent
    data_dir = here / 'data'

    if not args:
        # Validate all properties
        configs = find_configs(data_dir)
        if not configs:
            print(f'{RED("✗")} No config.yaml files found under {data_dir}')
            sys.exit(1)
        all_ok = True
        for cp in configs:
            ok = validate_config(cp)
            all_ok = all_ok and ok
        sys.exit(0 if all_ok else 1)

    target = args[0]
    # Accept either a file path or a property code
    p = Path(target)
    if p.suffix in ('.yaml', '.yml') and p.exists():
        config_path = p
    elif (data_dir / target / 'config.yaml').exists():
        config_path = data_dir / target / 'config.yaml'
    elif (here / target).exists():
        candidates = list(Path(here / target).glob('config.yaml'))
        if candidates:
            config_path = candidates[0]
        else:
            print(f'{RED("✗")} No config.yaml found in {here / target}')
            sys.exit(1)
    else:
        print(f'{RED("✗")} Cannot find config for "{target}"')
        print(f'  Try:  python validate_property_config.py data/revlabspm/config.yaml')
        print(f'  Or:   python validate_property_config.py revlabspm')
        sys.exit(1)

    ok = validate_config(config_path)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
