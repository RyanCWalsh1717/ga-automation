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
    management_code:        str,
    invoice_prefix:         str,
    team_members:           list[str],
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
    if property_type:        cfg['property_type']    = property_type
    if property_size_sf:     cfg['property_size_sf'] = property_size_sf

    cfg['investor_name']      = investor_name
    cfg['management_company'] = management_company
    cfg['management_code']    = management_code
    cfg['invoice_prefix']     = invoice_prefix

    _clean_members = [m.strip() for m in (team_members or []) if (m or '').strip()]
    if _clean_members:
        cfg['team_members'] = _clean_members

    if fees:    cfg['management_fees'] = fees
    if gl_accounts: cfg['gl_accounts'] = {k: str(v) for k, v in gl_accounts.items() if v}
    if banks:   cfg['bank_accounts']   = banks
    if payment_ach:   cfg['payment_ach']   = payment_ach
    if payment_check: cfg['payment_check'] = payment_check

    cfg['re_tax_payment_months']  = re_tax_payment_months or [1, 4, 7, 10]
    if parcel_ids: cfg['parcel_ids'] = [str(p) for p in parcel_ids if p]

    cfg['kardin_budget_file']      = kardin_budget_file or 'GA_Kardin_Budget_FY2026.xlsx'
    cfg['fiscal_year_start_month'] = fiscal_year_start_month or 1
    cfg['file_prefix_internal']    = file_prefix_internal or 'GA'
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
