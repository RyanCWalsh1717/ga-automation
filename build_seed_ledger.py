"""
Build GA_Prepaid_Ledger_Mar2026_Seed.xlsx
Complete seed from JLL 135150 PPD Other tab (03.26 Workpapers REVLABS NP Reviewed.xlsx).
"""
import sys
sys.path.insert(0, r'C:\Users\RyanCWalsh\.claude\ga-automation\pipeline')
import prepaid_ledger
from datetime import date

# ── ACTIVE ITEMS as of end of March 2026 ─────────────────────────────────────
# months_amortized = months released THROUGH March 2026 (after advance_period)
# first_added_period drives timing: amort_month = first_added + months_amortized
# → must produce Apr-2026 for next release to fire correctly in April run.

active_items = [

    # ── Already in initial seed (from Nexus paid file) ───────────────────────

    # Openpath INV198171: 12.19.25–12.18.26, 12 mo, $637.50/mo
    # Dec=1, Jan=2, Feb=3, Mar=4 → 4 elapsed. JLL balance: $5,491 (day-based).
    {
        'vendor':             'Openpath Security Inc',
        'invoice_number':     'INV198171',
        'invoice_date':       date(2025, 12, 23),
        'description':        '12.19.25-12.18.26 Annual Openpath Subscription',
        'gl_account_number':  '615320',
        'gl_account':         'RM-Doors/Locks',
        'total_amount':       7650.00,
        'monthly_amount':     637.50,
        'service_start':      date(2025, 12, 19),
        'service_end':        date(2026, 12, 18),
        'total_months':       12.0,
        'months_amortized':   4.0,
        'remaining_months':   8.0,
        'first_added_period': 'Dec-2025',
    },

    # Openpath INV207256: 02.10.26–02.09.27, 12 mo, $42.50/mo
    # Feb=1, Mar=2.
    {
        'vendor':             'Openpath Security Inc',
        'invoice_number':     'INV207256',
        'invoice_date':       date(2026, 2, 26),
        'description':        '02.10.26-02.09.27 Annual Openpath Subscription',
        'gl_account_number':  '615320',
        'gl_account':         'RM-Doors/Locks',
        'total_amount':       510.00,
        'monthly_amount':     42.50,
        'service_start':      date(2026, 2, 10),
        'service_end':        date(2027, 2, 9),
        'total_months':       12.0,
        'months_amortized':   2.0,
        'remaining_months':   10.0,
        'first_added_period': 'Feb-2026',
    },

    # Stewart 76105: 04/01/26–03/31/27, 12 mo, $125/mo — STARTS April 2026
    # months_amortized=0; first release fires Apr-2026.
    {
        'vendor':             'Stewart Management Company',
        'invoice_number':     '76105',
        'invoice_date':       date(2026, 3, 15),
        'description':        '04/01/2026-03/31/2027 Stewart Mgmt Annual',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       1500.00,
        'monthly_amount':     125.00,
        'service_start':      date(2026, 4, 1),
        'service_end':        date(2027, 3, 31),
        'total_months':       12.0,
        'months_amortized':   0.0,
        'remaining_months':   12.0,
        'first_added_period': 'Apr-2026',
    },

    # ── From JLL 135150 PPD Other tab ────────────────────────────────────────

    # CT Corporation System (Legal Regulatory): 06/01/25–05/31/26, 12 mo, $126.90/mo
    # JLL elapsed=10 (Jun–Mar inclusive). Remaining: Apr, May.
    {
        'vendor':             'CT Corporation System',
        'invoice_number':     'CT-LEGAL-2025',
        'invoice_date':       date(2025, 5, 1),
        'description':        '06/01/2025-05/31/2026 Legal Regulatory Filings',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       1522.80,
        'monthly_amount':     126.90,
        'service_start':      date(2025, 6, 1),
        'service_end':        date(2026, 5, 31),
        'total_months':       12.0,
        'months_amortized':   10.0,
        'remaining_months':   2.0,
        'first_added_period': 'Jun-2025',
    },

    # CT Corporation System (Filing Fees): same period, $50.63/mo
    {
        'vendor':             'CT Corporation System',
        'invoice_number':     'CT-FILINGFEES-2025',
        'invoice_date':       date(2025, 5, 1),
        'description':        '06/01/2025-05/31/2026 Annual Filing Fees',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       607.60,
        'monthly_amount':     50.63,
        'service_start':      date(2025, 6, 1),
        'service_end':        date(2026, 5, 31),
        'total_months':       12.0,
        'months_amortized':   10.0,
        'remaining_months':   2.0,
        'first_added_period': 'Jun-2025',
    },

    # Dynamic Media (SiriusXM): 09/2025–09/2026, 13 mo, $51.60/mo
    # JLL elapsed=7 (Sep–Mar). Remaining: Apr–Sep (6 mo).
    {
        'vendor':             'Dynamic Media',
        'invoice_number':     'DM-SXM-2025',
        'invoice_date':       date(2025, 8, 1),
        'description':        '09/2025-09/2026 SiriusXM Annual Subscription',
        'gl_account_number':  '637330',
        'gl_account':         'Admin-Entertainment',
        'total_amount':       670.80,
        'monthly_amount':     51.60,
        'service_start':      date(2025, 9, 1),
        'service_end':        date(2026, 9, 30),
        'total_months':       13.0,
        'months_amortized':   7.0,
        'remaining_months':   6.0,
        'first_added_period': 'Sep-2025',
    },

    # Prophia Inc: 10/20/25–10/19/26, 13-period calc, $115.38/mo
    # JLL elapsed=5 (through Feb); March is period 6 → months_amortized=6 after Mar close.
    {
        'vendor':             'Prophia Inc',
        'invoice_number':     'PROPHIA-2025',
        'invoice_date':       date(2025, 10, 16),
        'description':        '10/20/2025-10/19/2026 Prophia Subscription Renewal',
        'gl_account_number':  '680510',
        'gl_account':         'NR-Marketing & PR',
        'total_amount':       1500.00,
        'monthly_amount':     115.38,
        'service_start':      date(2025, 10, 20),
        'service_end':        date(2026, 10, 19),
        'total_months':       13.0,
        'months_amortized':   6.0,
        'remaining_months':   7.0,
        'first_added_period': 'Oct-2025',
    },

    # Apex Computers: 01/01/26–12/31/26, 12 mo, $312.50/mo
    # Jan=1, Feb=2, Mar=3. Remaining: Apr–Dec (9 mo).
    {
        'vendor':             'Apex Computers Inc',
        'invoice_number':     'APEX-2026',
        'invoice_date':       date(2026, 1, 1),
        'description':        '01/01/2026-12/31/2026 Annual Firewall/Switch/WiFi/Tech Support',
        'gl_account_number':  '637370',
        'gl_account':         'Admin-Computer/Software',
        'total_amount':       3750.00,
        'monthly_amount':     312.50,
        'service_start':      date(2026, 1, 1),
        'service_end':        date(2026, 12, 31),
        'total_months':       12.0,
        'months_amortized':   3.0,
        'remaining_months':   9.0,
        'first_added_period': 'Jan-2026',
    },

    # 128 Business Council (Shuttle): 01/26–06/26, 6 mo, $4,605.17/mo
    # Jan=1, Feb=2, Mar=3. Remaining: Apr, May, Jun (3 mo).
    {
        'vendor':             '128 Business Council',
        'invoice_number':     '128BC-2026',
        'invoice_date':       date(2026, 1, 1),
        'description':        '01/2026-06/2026 Shuttle Services Contract',
        'gl_account_number':  '637150',
        'gl_account':         'Admin-Tenant Relations',
        'total_amount':       27631.00,
        'monthly_amount':     4605.17,
        'service_start':      date(2026, 1, 1),
        'service_end':        date(2026, 6, 30),
        'total_months':       6.0,
        'months_amortized':   3.0,
        'remaining_months':   3.0,
        'first_added_period': 'Jan-2026',
    },

    # Greatland Realty Partners (Yardi 12/25–11/26): 12 mo, $384.90/mo
    # Two semi-annual payments; implied total $384.90 x 12 = $4,618.83.
    # Dec=1, Jan=2, Feb=3, Mar=4. Remaining: May–Nov (8 mo).
    {
        'vendor':             'Greatland Realty Partners LLC',
        'invoice_number':     'GRP-YARDI-1226',
        'invoice_date':       date(2025, 11, 30),
        'description':        '12/2025-11/2026 Yardi Accounting Software',
        'gl_account_number':  '637370',
        'gl_account':         'Admin-Computer/Software',
        'total_amount':       4618.83,
        'monthly_amount':     384.90,
        'service_start':      date(2025, 12, 1),
        'service_end':        date(2026, 11, 30),
        'total_months':       12.0,
        'months_amortized':   4.0,
        'remaining_months':   8.0,
        'first_added_period': 'Dec-2025',
    },

    # Equiem USA LLC: 01/26–12/26, 12 mo, $1,339.88/mo
    # Jan=1, Feb=2, Mar=3. Remaining: Apr–Dec (9 mo).
    {
        'vendor':             'Equiem USA LLC',
        'invoice_number':     'EQUIEM-2026',
        'invoice_date':       date(2026, 1, 1),
        'description':        '01/2026-12/2026 Equiem Tenant Engagement Platform',
        'gl_account_number':  '637150',
        'gl_account':         'Admin-Tenant Relations',
        'total_amount':       16078.54,
        'monthly_amount':     1339.88,
        'service_start':      date(2026, 1, 1),
        'service_end':        date(2026, 12, 31),
        'total_months':       12.0,
        'months_amortized':   3.0,
        'remaining_months':   9.0,
        'first_added_period': 'Jan-2026',
    },

    # Greatland Realty Partners (03/26–02/27): 11 mo, $92.17/mo
    # JLL elapsed=0 (workpaper before month-end); March IS month 1.
    # After Mar close: months_amortized=1; first release in Apr-2026.
    {
        'vendor':             'Greatland Realty Partners LLC',
        'invoice_number':     'GRP-MAR26-FEB27',
        'invoice_date':       date(2026, 1, 31),
        'description':        '03/2026-02/2027 GRP Annual Services',
        'gl_account_number':  '712210',
        'gl_account':         'NR-Other Expenses',
        'total_amount':       1013.85,
        'monthly_amount':     92.17,
        'service_start':      date(2026, 3, 1),
        'service_end':        date(2027, 2, 28),
        'total_months':       11.0,
        'months_amortized':   1.0,
        'remaining_months':   10.0,
        'first_added_period': 'Mar-2026',
    },
]

# ── COMPLETED items (fully amortized as of March 2026) ───────────────────────
completed_items = [
    {
        'vendor':             'Stewart Management Company',
        'invoice_number':     '76104',
        'invoice_date':       date(2026, 3, 15),
        'description':        '04/01/2025-03/31/2026 Stewart Mgmt Annual',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       1500.00,
        'monthly_amount':     125.00,
        'service_start':      date(2025, 4, 1),
        'service_end':        date(2026, 3, 31),
        'total_months':       12.0,
        'months_amortized':   12.0,
        'remaining_months':   0.0,
        'first_added_period': 'Apr-2025',
        'completed_period':   'Mar-2026',
    },
    {
        'vendor':             'Greatland Realty Partners LLC',
        'invoice_number':     'GRP-YARDI-0325',
        'invoice_date':       date(2025, 2, 4),
        'description':        '03/01/2025-02/28/2026 Yardi Accounting Software',
        'gl_account_number':  '637370',
        'gl_account':         'Admin-Computer/Software',
        'total_amount':       992.58,
        'monthly_amount':     82.72,
        'service_start':      date(2025, 3, 1),
        'service_end':        date(2026, 2, 28),
        'total_months':       12.0,
        'months_amortized':   12.0,
        'remaining_months':   0.0,
        'first_added_period': 'Mar-2025',
        'completed_period':   'Feb-2026',
    },
    {
        'vendor':             'Stewart Management Company',
        'invoice_number':     '65429',
        'invoice_date':       date(2025, 4, 1),
        'description':        '04/01/2025-03/31/2026 Background Checks - Shippam/Corra',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       1500.00,
        'monthly_amount':     125.00,
        'service_start':      date(2025, 4, 1),
        'service_end':        date(2026, 3, 31),
        'total_months':       12.0,
        'months_amortized':   12.0,
        'remaining_months':   0.0,
        'first_added_period': 'Apr-2025',
        'completed_period':   'Mar-2026',
    },
    {
        'vendor':             'Stewart Management Company',
        'invoice_number':     '65430',
        'invoice_date':       date(2025, 4, 1),
        'description':        '04/01/2025-03/31/2026 Background Checks - Steward/Harrison',
        'gl_account_number':  '680110',
        'gl_account':         'NR-Prof Fees-Legal',
        'total_amount':       1500.00,
        'monthly_amount':     125.00,
        'service_start':      date(2025, 4, 1),
        'service_end':        date(2026, 3, 31),
        'total_months':       12.0,
        'months_amortized':   12.0,
        'remaining_months':   0.0,
        'first_added_period': 'Apr-2025',
        'completed_period':   'Mar-2026',
    },
]

out_path = r'C:\Users\RyanCWalsh\Desktop\GA_Prepaid_Ledger_Mar2026_Seed.xlsx'
prepaid_ledger.save(active_items, completed_items, out_path)

print('Saved:', out_path)
print()
print(f'ACTIVE ({len(active_items)} items):')
total_monthly = 0
for item in active_items:
    mo = round(item['monthly_amount'], 2)
    rem = int(item['remaining_months'])
    total_monthly += mo
    print(f"  {item['vendor'][:38]:38s}  {item['invoice_number']:20s}  ${mo:>8,.2f}/mo  {rem} mo left")
print()
print(f'  Total April amortization: ${total_monthly:,.2f}/mo')
print()
print(f'COMPLETED ({len(completed_items)} items):')
for item in completed_items:
    print(f"  {item['vendor'][:38]:38s}  {item['invoice_number']:20s}  completed {item['completed_period']}")
