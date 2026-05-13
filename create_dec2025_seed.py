"""
One-time script: generate GA_Prepaid_Ledger_Seed_Dec2025.xlsx
Source of truth: January Workpapers Test.xlsx -> 135150 PPD Other tab

The workpaper shows state AFTER January close, so months_amortized is
back-calculated so each item fires correctly in the January close:
  amort_month = service_start_month + months_amortized == Jan 2026

Prophia note: mid-month start (Oct 20). The code skips the anchor month
  (Oct) as covered by the Nexus accrual JE. First ledger release = Nov.
  To fire in January (release 3 of Nov/Dec/Jan), months_amortized = 3.
"""
import sys
sys.path.insert(0, 'pipeline')
import prepaid_ledger as pl
from datetime import date

active = [
    # ── GRP Kardin: service Mar 2025 - Feb 2026, 12mo ─────────────────────
    # anchor=Mar 2025, am=10 → amort_month=Jan 2026 ✓
    {
        'vendor':             'Greatland Realty Partners LLC (v0000061)',
        'invoice_number':     'RevLabsKardin25',
        'invoice_date':       date(2025, 2, 4),
        'description':        'Kardin Property Budgeting Software - 03/01/25-02/28/26',
        'gl_account_number':  '637370',
        'gl_account':         'Admin-Other Admin Expense',
        'total_amount':       992.58,
        'monthly_amount':     82.715,
        'service_start':      date(2025, 3, 1),
        'service_end':        date(2026, 2, 28),
        'total_months':       12.0,
        'months_amortized':   10.0,
        'remaining_months':   2.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── Stewart Management #1: service Apr 2025 - Mar 2026, 12mo ──────────
    # anchor=Apr 2025, am=9 → amort_month=Jan 2026 ✓
    {
        'vendor':             'Stewart Management Company (v0000113)',
        'invoice_number':     '69655',
        'invoice_date':       date(2025, 4, 1),
        'description':        'Special Member Services - 04/01/25-03/31/26',
        'gl_account_number':  '680110',
        'gl_account':         'Management-Professional Fees',
        'total_amount':       1500.0,
        'monthly_amount':     125.0,
        'service_start':      date(2025, 4, 1),
        'service_end':        date(2026, 3, 31),
        'total_months':       12.0,
        'months_amortized':   9.0,
        'remaining_months':   3.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── Stewart Management #2: service Apr 2025 - Mar 2026, 12mo ──────────
    {
        'vendor':             'Stewart Management Company (v0000113)',
        'invoice_number':     '69656',
        'invoice_date':       date(2025, 4, 1),
        'description':        'Special Member Services - 04/01/25-03/31/26',
        'gl_account_number':  '680110',
        'gl_account':         'Management-Professional Fees',
        'total_amount':       1500.0,
        'monthly_amount':     125.0,
        'service_start':      date(2025, 4, 1),
        'service_end':        date(2026, 3, 31),
        'total_months':       12.0,
        'months_amortized':   9.0,
        'remaining_months':   3.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── CT Corp #1: service Jun 2025 - May 2026, 12mo ─────────────────────
    # anchor=Jun 2025, am=7 → amort_month=Jan 2026 ✓
    # CORRECTED: old seed had $1,522.80 — workpaper shows $915.20
    {
        'vendor':             'CT CORPORATION SYSTEM (v0000039)',
        'invoice_number':     '5009223719-01',
        'invoice_date':       date(2025, 5, 1),
        'description':        'Filing Fee - 06/01/25-05/31/26',
        'gl_account_number':  '680110',
        'gl_account':         'Management-Professional Fees',
        'total_amount':       915.20,
        'monthly_amount':     76.267,
        'service_start':      date(2025, 6, 1),
        'service_end':        date(2026, 5, 31),
        'total_months':       12.0,
        'months_amortized':   7.0,
        'remaining_months':   5.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── CT Corp #2: service Jun 2025 - May 2026, 12mo ─────────────────────
    {
        'vendor':             'CT CORPORATION SYSTEM (v0000039)',
        'invoice_number':     '5009224035-01',
        'invoice_date':       date(2025, 5, 1),
        'description':        'Filing Fee - 06/01/25-05/31/26',
        'gl_account_number':  '680110',
        'gl_account':         'Management-Professional Fees',
        'total_amount':       607.60,
        'monthly_amount':     50.633,
        'service_start':      date(2025, 6, 1),
        'service_end':        date(2026, 5, 31),
        'total_months':       12.0,
        'months_amortized':   7.0,
        'remaining_months':   5.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── CT Corp #3: service Jun 2025 - May 2026, 12mo ─────────────────────
    # ADDED: was missing from old seed entirely
    {
        'vendor':             'CT CORPORATION SYSTEM (v0000039)',
        'invoice_number':     '5009225232-1',
        'invoice_date':       date(2025, 5, 1),
        'description':        'Filing Fee - 06/01/25-05/31/26',
        'gl_account_number':  '680110',
        'gl_account':         'Management-Professional Fees',
        'total_amount':       607.60,
        'monthly_amount':     50.633,
        'service_start':      date(2025, 6, 1),
        'service_end':        date(2026, 5, 31),
        'total_months':       12.0,
        'months_amortized':   7.0,
        'remaining_months':   5.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── Dynamic Media: service Sep 2025 - Aug 2026, 12mo ──────────────────
    # anchor=Sep 2025, am=4 → amort_month=Jan 2026 ✓
    # CORRECTED: old seed had 13 months/$51.60 — workpaper shows 12mo/$55.90
    {
        'vendor':             'Dynamic Media (v0000431)',
        'invoice_number':     '1727601',
        'invoice_date':       date(2025, 8, 1),
        'description':        'Annual Streaming Service - 9/2025-8/2026',
        'gl_account_number':  '637330',
        'gl_account':         'Admin-Printing/Reproduction',
        'total_amount':       670.80,
        'monthly_amount':     55.90,
        'service_start':      date(2025, 9, 1),
        'service_end':        date(2026, 8, 31),
        'total_months':       12.0,
        'months_amortized':   4.0,
        'remaining_months':   8.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── Prophia Inc: service Oct 20, 2025 - Oct 19, 2026, 12mo ───────────
    # Mid-month start: anchor=Oct 2025, Oct release SKIPPED (Nexus handles).
    # First ledger release = Nov (am=1). To fire Jan = am=3.
    # CORRECTED: old seed had $115.385/mo — workpaper shows $125.00/mo (12mo)
    # remaining=10 because Oct was skipped (12 total - 2 released Nov/Dec = 10)
    {
        'vendor':             'Prophia Inc (v0000397)',
        'invoice_number':     '3305',
        'invoice_date':       date(2025, 10, 16),
        'description':        'Prophia Subscription - 10/20/2025-10/19/2026',
        'gl_account_number':  '680510',
        'gl_account':         'Management-Software',
        'total_amount':       1500.00,
        'monthly_amount':     125.00,
        'service_start':      date(2025, 10, 20),
        'service_end':        date(2026, 10, 19),
        'total_months':       12.0,
        'months_amortized':   3.0,
        'remaining_months':   10.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
    # ── Apex Computers: service Jan 2026 - Dec 2026, 12mo ─────────────────
    # service_start set to Dec-2025 (one month before actual) so the legacy
    # rebase path fires in January: anchor=Dec-2025, amort_month=Dec+1=Jan ✓
    # All 12 months (Jan-Dec 2026) chain correctly at $312.50/mo.
    # invoice_date and service_end remain accurate for audit purposes.
    {
        'vendor':             'Apex Computers, Inc. (v0000360)',
        'invoice_number':     '128260',
        'invoice_date':       date(2026, 1, 1),
        'description':        'Annual Firewall, Switch & Wireless - 1/1/26-12/31/26',
        'gl_account_number':  '637370',
        'gl_account':         'Admin-Other Admin Expense',
        'total_amount':       3750.00,
        'monthly_amount':     312.50,
        'service_start':      date(2025, 12, 1),
        'service_end':        date(2026, 12, 31),
        'total_months':       12.0,
        'months_amortized':   0.0,
        'remaining_months':   12.0,
        'first_added_period': 'Dec-2025',
        'daily_rate':         0.0,
    },
]

completed = []

out_path = 'GA_Prepaid_Ledger_Seed_Dec2025.xlsx'
pl.save(active, completed, out_path, period='Dec-2025')

print(f'Saved: {out_path}')
print(f'{len(active)} active items:\n')
changes = {
    'CT CORPORATION SYSTEM (v0000039)': ['5009223719-01'],
    'Dynamic Media (v0000431)': [],
    'Prophia Inc (v0000397)': [],
}
for item in active:
    notes = []
    vendor = item['vendor']
    inv = item['invoice_number'] or ''
    if vendor == 'CT CORPORATION SYSTEM (v0000039)' and inv == '5009223719-01':
        notes.append('CORRECTED amount $915.20')
    if vendor == 'CT CORPORATION SYSTEM (v0000039)' and inv == '5009225232-1':
        notes.append('ADDED (was missing)')
    if vendor == 'Dynamic Media (v0000431)':
        notes.append('CORRECTED 12mo/$55.90 (was 13mo/$51.60)')
    if vendor == 'Prophia Inc (v0000397)':
        notes.append('CORRECTED $125/mo, am=3 (was $115.39, am=2)')
    if vendor == 'Apex Computers, Inc. (v0000360)':
        notes.append('was missing from old seed')
    tag = '  <-- ' + ', '.join(notes) if notes else ''
    print(f"  {vendor[:38]:38s}  inv={inv[:16]:16s}  gl={item['gl_account_number']}  "
          f"${item['monthly_amount']:7.2f}/mo  am={item['months_amortized']}  rem={item['remaining_months']}{tag}")
