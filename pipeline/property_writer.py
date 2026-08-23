"""
property_writer.py — Save PropertyConfig to YAML (local + GitHub API)
======================================================================
Used by the in-app Properties setup tab to persist new/edited property
configs without anyone needing to touch GitHub directly.

Save priority:
  1. GitHub API  — if token + repo in st.secrets or environment vars.
                   Triggers an automatic Streamlit Cloud redeploy.
  2. Local disk  — always attempted; persists for dev/self-hosted installs.
  3. Download    — YAML bytes always returned so the UI can offer a download
                   as a fallback regardless of whether saves succeed.

Secrets expected in .streamlit/secrets.toml or Streamlit Cloud settings:
    [github]
    token = "ghp_..."
    repo  = "RyanCWalsh1717/ga-automation"   # owner/repo
"""

from __future__ import annotations

import os
import yaml
from typing import Optional


# ── YAML generation ───────────────────────────────────────────────────────────

def config_to_yaml(data: dict) -> str:
    """
    Render a property config dict as a YAML string.
    Uses block style for readability; sorts keys=False to preserve field order.
    """
    return yaml.dump(data, default_flow_style=False, allow_unicode=True,
                     sort_keys=False, width=80)


def build_config_dict(
    property_code:          str,
    property_name:          str,
    property_display_name:  str,
    property_address:       str,
    property_type:          str,
    property_size_sf:       Optional[int],
    investor_name:          str,
    management_company:     str,
    invoice_prefix:         str,
    team_members:           list[str],
    tenants:                list[dict],   # [{'key','name'}]
    default_accruals:       list[dict],   # [{'account_code','account_name','vendor'}]
    building_splits:        list[dict],   # [{'schedule','name','yardi_code','share_pct','notes'}]
    default_split_schedule: str,
    management_fees:        list[dict],   # [{'name','rate','minimum','dr_account','cr_account','ref_prefix'}]
    gl_accounts:            dict,
    bank_accounts:          list[dict],   # [{'slug','label','bank_name','last4','full_account','gl_account'}]
    payment_ach:            dict,
    payment_check:          dict,
    re_tax_payment_months:  list[int],
    parcel_ids:             list[str],
    kardin_budget_file:     str,
    fiscal_year_start_month: int,
    file_prefix_internal:   str,
    file_prefix_deliverable: str,
    active:                 bool = True,
    property_system:        str = 'yardi',
    uses_grp_coa:           bool = False,
    consolidated_buildings: list[dict] = None,   # [{'name','yardi_code','size_sf'}]
    investor_legal_name:    str = '',
    yardi_subset_code:      str = '',
) -> dict:
    """Build the ordered dict that becomes the YAML config file."""
    banks = {}
    for ba in bank_accounts:
        slug = (ba.get('slug') or '').strip()
        if not slug:
            continue
        banks[slug] = {k: v for k, v in {
            'label':        ba.get('label', ''),
            'bank_name':    ba.get('bank_name', ''),
            'last4':        ba.get('last4', ''),
            'full_account': str(ba.get('full_account', '')),
            'gl_account':   str(ba.get('gl_account', '')),
        }.items() if v}

    fees = []
    for fl in management_fees:
        name = (fl.get('name') or '').strip()
        if not name:
            continue
        fees.append({k: v for k, v in {
            'name':       name,
            'rate':       round(float(fl.get('rate', 0)), 6),
            'minimum':    float(fl.get('minimum', 0)),
            'dr_account': str(fl.get('dr_account', '637130')),
            'cr_account': str(fl.get('cr_account', '213100')),
            'ref_prefix': fl.get('ref_prefix', f"MGMT-FEE-{name.upper()}"),
        }.items() if v or v == 0})

    cfg: dict = {
        'property_code':          property_code,
        'property_name':          property_name,
        'property_display_name':  property_display_name,
        'property_address':       property_address,
    }
    # Always write property_system so the UI can detect it reliably.
    # 'yardi' is the default; MRI and future systems are stored explicitly.
    cfg['property_system'] = (property_system or 'yardi').lower()
    if uses_grp_coa:         cfg['uses_grp_coa']     = True   # omit when False (defaults to False on load)
    if property_type:        cfg['property_type']    = property_type
    if property_size_sf:     cfg['property_size_sf'] = property_size_sf
    if not active:           cfg['active']           = False   # omit when True (defaults to True on load)

    _clean_buildings = [
        {
            'name':       (b.get('name') or '').strip(),
            'yardi_code': (b.get('yardi_code') or '').strip(),
            'size_sf':    int(b.get('size_sf', 0) or 0),
        }
        for b in (consolidated_buildings or [])
        if (b.get('name') or '').strip()
    ]
    if _clean_buildings:
        cfg['consolidated_buildings'] = _clean_buildings
    if yardi_subset_code.strip():
        cfg['yardi_subset_code'] = yardi_subset_code.strip()

    cfg['investor_name']      = investor_name
    if investor_legal_name.strip():
        cfg['investor_legal_name'] = investor_legal_name.strip()
    cfg['management_company'] = management_company
    cfg['invoice_prefix']     = invoice_prefix

    _clean_members = [m.strip() for m in (team_members or []) if (m or '').strip()]
    if _clean_members:
        cfg['team_members'] = _clean_members

    _clean_tenants = [
        {'key': t.get('key', '').strip(), 'name': t.get('name', '').strip()}
        for t in (tenants or [])
        if (t.get('key') or '').strip() and (t.get('name') or '').strip()
    ]
    if _clean_tenants:
        cfg['tenants'] = _clean_tenants

    _clean_accruals = [
        {k: v for k, v in {
            'account_code': str(a.get('account_code', '')).strip(),
            'account_name': str(a.get('account_name', '')).strip(),
            'vendor':       str(a.get('vendor', '')).strip(),
        }.items() if v}
        for a in (default_accruals or [])
        if (a.get('account_code') or '').strip()
    ]
    if _clean_accruals:
        cfg['default_accruals'] = _clean_accruals

    _splits = []
    for bs in (building_splits or []):
        _bname = (bs.get('name') or '').strip()
        if not _bname:
            continue
        _entry: dict = {
            'schedule':  (bs.get('schedule') or 'default').strip(),
            'name':      _bname,
            'share_pct': round(float(bs.get('share_pct', 0)), 6),
        }
        if bs.get('yardi_code', '').strip():
            _entry['yardi_code'] = bs['yardi_code'].strip()
        if bs.get('notes', '').strip():
            _entry['notes'] = bs['notes'].strip()
        _splits.append(_entry)
    if _splits:
        cfg['building_splits'] = _splits
    if (default_split_schedule or '').strip():
        cfg['default_split_schedule'] = default_split_schedule.strip()

    if fees:    cfg['management_fees'] = fees
    if gl_accounts: cfg['gl_accounts'] = {k: str(v) for k, v in gl_accounts.items() if v}
    if banks:   cfg['bank_accounts']   = banks
    if payment_ach:   cfg['payment_ach']   = payment_ach
    if payment_check: cfg['payment_check'] = payment_check

    cfg['re_tax_payment_months']  = re_tax_payment_months or [1, 4, 7, 10]
    if parcel_ids: cfg['parcel_ids'] = [str(p) for p in parcel_ids if p]

    import datetime as _dt
    _fy_start = fiscal_year_start_month or 1
    _today    = _dt.date.today()
    # Current fiscal year: if today is before FY start month, FY = current year - 1
    _cur_fy   = _today.year if _today.month >= _fy_start else _today.year - 1
    _default_budget = f'{file_prefix_internal or "GA"}_Kardin_Budget_FY{_cur_fy}.xlsx'
    cfg['kardin_budget_file']      = kardin_budget_file or _default_budget
    cfg['fiscal_year_start_month'] = fiscal_year_start_month or 1
    cfg['file_prefix_internal']    = (file_prefix_internal or '').strip() or 'GA'
    if file_prefix_deliverable:
        cfg['file_prefix_deliverable'] = file_prefix_deliverable

    return cfg


# ── Local filesystem save ─────────────────────────────────────────────────────

def save_local(property_code: str, yaml_content: str, data_dir: str) -> tuple[bool, str]:
    """
    Write config.yaml to data/{property_code}/ on local disk.
    Returns (success, message).
    """
    try:
        folder = os.path.join(data_dir, property_code)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, 'config.yaml')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(yaml_content)
        return True, path
    except Exception as e:
        return False, str(e)


# ── GitHub API save ───────────────────────────────────────────────────────────

def _github_credentials() -> tuple[str, str]:
    """
    Return (token, repo) from Streamlit secrets or environment variables.
    Returns ('', '') if neither is configured.
    """
    token, repo = '', ''
    try:
        import streamlit as st
        token = st.secrets.get('github', {}).get('token', '')
        repo  = st.secrets.get('github', {}).get('repo',  '')
    except Exception:
        pass
    token = token or os.environ.get('GITHUB_TOKEN', '')
    repo  = repo  or os.environ.get('GITHUB_REPO',  '')
    return token, repo


def save_to_github(property_code: str, yaml_content: str,
                   commit_message: str = '') -> tuple[bool, str]:
    """
    Write config.yaml to the GitHub repo via the REST API.

    Reads credentials from st.secrets['github']['token'] and
    st.secrets['github']['repo'], or GITHUB_TOKEN / GITHUB_REPO env vars.

    Returns (success, message).
    """
    import base64
    try:
        import requests
    except ImportError:
        return False, 'requests library not available'

    token, repo = _github_credentials()
    if not token or not repo:
        return False, 'GitHub token/repo not configured in secrets'

    path = f'data/{property_code}/config.yaml'
    url  = f'https://api.github.com/repos/{repo}/contents/{path}'
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }

    # GET existing file to retrieve SHA (required for updates)
    sha = None
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            sha = r.json().get('sha')
    except Exception:
        pass

    payload: dict = {
        'message': commit_message or f'Add/update property config: {property_code}',
        'content': base64.b64encode(yaml_content.encode('utf-8')).decode('ascii'),
    }
    if sha:
        payload['sha'] = sha

    try:
        r = requests.put(url, json=payload, headers=headers, timeout=15)
        if r.status_code in (200, 201):
            action = 'updated' if sha else 'created'
            return True, f'Config {action} in GitHub. Streamlit will redeploy in ~2 minutes.'
        return False, f'GitHub API returned {r.status_code}: {r.text[:200]}'
    except Exception as e:
        return False, str(e)


def github_configured() -> bool:
    """Return True if GitHub credentials are available."""
    token, repo = _github_credentials()
    return bool(token and repo)


def save_image_to_github(
    property_code: str,
    image_bytes: bytes,
    filename: str = 'hero.jpg',
) -> tuple[bool, str]:
    """
    Upload a building photo to data/{property_code}/{filename} in the GitHub repo.
    Returns (success, message).
    """
    import base64
    try:
        import requests
    except ImportError:
        return False, 'requests library not available'

    token, repo = _github_credentials()
    if not token or not repo:
        return False, 'GitHub token/repo not configured in secrets'

    path    = f'data/{property_code}/{filename}'
    url     = f'https://api.github.com/repos/{repo}/contents/{path}'
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }

    sha = None
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            sha = r.json().get('sha')
    except Exception:
        pass

    payload: dict = {
        'message': f'Upload building photo: {property_code}/{filename}',
        'content': base64.b64encode(image_bytes).decode('ascii'),
    }
    if sha:
        payload['sha'] = sha

    try:
        r = requests.put(url, json=payload, headers=headers, timeout=30)
        if r.status_code in (200, 201):
            return True, f'Photo saved to GitHub. Hero banner updates after ~2 min redeploy.'
        return False, f'GitHub API returned {r.status_code}: {r.text[:200]}'
    except Exception as e:
        return False, str(e)


def save_image_local(
    property_code: str,
    image_bytes: bytes,
    filename: str,
    data_dir: str,
) -> tuple[bool, str]:
    """Save image bytes to data/{property_code}/{filename} on local disk."""
    try:
        folder = os.path.join(data_dir, property_code)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, filename)
        with open(path, 'wb') as f:
            f.write(image_bytes)
        return True, path
    except Exception as e:
        return False, str(e)


# ── Deactivate / Reactivate property ─────────────────────────────────────────

def _set_active_flag(property_code: str, active: bool, data_dir: str) -> tuple[bool, str]:
    """
    Toggle the `active:` flag in config.yaml without rewriting the whole file.
    Reads the YAML, flips the flag, saves locally + GitHub.
    Returns (success, message).
    """
    import yaml as _yaml
    path = os.path.join(data_dir, property_code, 'config.yaml')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw = _yaml.safe_load(f) or {}
    except FileNotFoundError:
        return False, f'config.yaml not found for {property_code}'
    except Exception as e:
        return False, str(e)

    raw['active'] = active
    yaml_content = _yaml.dump(raw, default_flow_style=False, allow_unicode=True,
                              sort_keys=False, width=80)

    local_ok, local_msg = save_local(property_code, yaml_content, data_dir)
    gh_ok, gh_msg = save_to_github(
        property_code, yaml_content,
        commit_message=f"{'Activate' if active else 'Deactivate'} property: {property_code}",
    )
    if local_ok or gh_ok:
        return True, gh_msg if gh_ok else local_msg
    return False, f'Local: {local_msg} | GitHub: {gh_msg}'


def deactivate_property(property_code: str, data_dir: str) -> tuple[bool, str]:
    """Set active: false on a property config. Hides from the property selector."""
    return _set_active_flag(property_code, active=False, data_dir=data_dir)


def reactivate_property(property_code: str, data_dir: str) -> tuple[bool, str]:
    """Set active: true on a property config. Restores it to the selector."""
    return _set_active_flag(property_code, active=True, data_dir=data_dir)


# ── Permanent delete ──────────────────────────────────────────────────────────

def _delete_from_github(property_code: str) -> tuple[bool, str]:
    """
    Delete every file under data/{property_code}/ in the GitHub repo.

    The Contents API has no recursive folder delete — list the directory,
    then DELETE each file individually (each needs its own current SHA).
    Returns (success, message). Success if the folder is gone or was already
    absent on GitHub (e.g. a locally-created property never pushed).
    """
    import requests

    token, repo = _github_credentials()
    if not token or not repo:
        return False, 'GitHub token/repo not configured in secrets'

    dir_path = f'data/{property_code}'
    url      = f'https://api.github.com/repos/{repo}/contents/{dir_path}'
    headers  = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
    except Exception as e:
        return False, str(e)

    if r.status_code == 404:
        return True, 'Not present on GitHub (nothing to delete there).'
    if r.status_code != 200:
        return False, f'GitHub API returned {r.status_code} listing {dir_path}: {r.text[:200]}'

    entries = r.json()
    if not isinstance(entries, list):
        return False, f'{dir_path} is not a folder on GitHub — refusing to delete.'

    deleted, failed = [], []
    for entry in entries:
        file_path = entry.get('path')
        sha       = entry.get('sha')
        if not file_path or not sha:
            continue
        file_url = f'https://api.github.com/repos/{repo}/contents/{file_path}'
        payload  = {
            'message': f'Delete property: {property_code}',
            'sha':     sha,
        }
        try:
            dr = requests.delete(file_url, json=payload, headers=headers, timeout=15)
        except Exception as e:
            failed.append(f'{file_path} ({e})')
            continue
        if dr.status_code in (200, 201):
            deleted.append(file_path)
        else:
            failed.append(f'{file_path} ({dr.status_code})')

    if failed:
        return False, f'Deleted {len(deleted)} file(s); failed: {", ".join(failed)}'
    return True, f'Deleted {len(deleted)} file(s) from GitHub.'


def delete_property(property_code: str, data_dir: str) -> tuple[bool, str]:
    """
    Permanently delete a property — removes data/{property_code}/ locally
    (config, workpaper template, Kardin budget, hero photo, everything) and
    every file under that path in the GitHub repo.

    Unlike deactivate_property(), this is NOT reversible — intended for a
    property that was entered by mistake, not a real property being wound
    down (use deactivate for that instead).

    Returns (success, message).
    """
    import shutil

    if not property_code or '/' in property_code or '..' in property_code:
        return False, f'Invalid property code: {property_code!r}'

    local_ok, local_msg = True, 'Not present locally.'
    folder = os.path.join(data_dir, property_code)
    if os.path.isdir(folder):
        try:
            shutil.rmtree(folder)
            local_ok, local_msg = True, f'Deleted local folder {folder}'
        except Exception as e:
            local_ok, local_msg = False, str(e)

    gh_ok, gh_msg = True, 'GitHub not configured — skipped.'
    if github_configured():
        gh_ok, gh_msg = _delete_from_github(property_code)

    if local_ok and gh_ok:
        return True, f'{local_msg} {gh_msg}'
    return False, f'Local: {local_msg} | GitHub: {gh_msg}'
