"""
File Classifier — Bulk Upload Auto-Detection
=============================================
Inspects uploaded file content to identify which pipeline input slot each file
belongs to, without relying on filenames.

Returns one of the FILE_CONFIG keys defined in app.py:
  gl, trial_balance, budget_comparison, kardin_budget, t12_statement,
  nexus_accrual, bank_rec, receivable_detail, ar_aging, bank_rec_dev,
  daca_bank, loan, prepaid_ledger
  + pass-2 variants: gl_pass2, budget_comparison_pass2, trial_balance_pass2,
                     t12_statement_pass2, loan_pass2, prior_workpaper

Usage:
  from file_classifier import classify_file
  key, confidence, label = classify_file(filename, file_bytes)
"""

from __future__ import annotations

import io
import re
from typing import Tuple

# ── Readable labels for each key (shown in the UI) ──────────────────────────
FILE_LABELS = {
    "gl":                    "Yardi GL Detail",
    "trial_balance":         "Yardi Trial Balance",
    "budget_comparison":     "Yardi Budget Comparison",
    "kardin_budget":         "Kardin Budget",
    "t12_statement":         "12-Month Income Statement",
    "nexus_accrual":         "Nexus Invoice Detail",
    "bank_rec":              "Yardi / PNC Bank Rec",
    "receivable_detail":     "Yardi Receivable Detail",
    "ar_aging":              "Yardi AR Detail Aging",
    "bank_rec_dev":          "BofA Development Statement",
    "daca_bank":             "KeyBank DACA Statement",
    "loan":                  "Berkadia Loan Statement(s) — due 7th of following month",
    "prepaid_ledger":        "Prior Month Prepaid Ledger",
    "prior_workpaper":       "Prior Month Workpaper",
    # Pass-2 overrides — same classifier routes here via pass2=True
    "gl_pass2":              "Final GL (Pass 2)",
    "budget_comparison_pass2": "Final Budget Comparison (Pass 2)",
    "trial_balance_pass2":   "Final Trial Balance (Pass 2)",
    "t12_statement_pass2":   "Post-Close T12 (Pass 2)",
    "loan_pass2":            "Berkadia Loan Statements (Pass 2) — due 7th of following month",
    "prior_workpaper":       "Prior Month Workpaper",
    "unknown":               "Unknown — select type",
}

# Pass-2 key remapping: classify_file returns the base key; caller passes pass2=True
# and we remap using this dict where a pass-2 override exists.
_PASS2_REMAP = {
    "gl":               "gl_pass2",
    "budget_comparison":"budget_comparison_pass2",
    "trial_balance":    "trial_balance_pass2",
    "t12_statement":    "t12_statement_pass2",
    "loan":             "loan_pass2",
}

# Keys that accept multiple files (list of paths, not a single path)
MULTI_FILE_KEYS = {"loan", "loan_pass2"}


def classify_file(filename: str, file_bytes: bytes,
                  pass2: bool = False) -> Tuple[str, float, str]:
    """
    Classify a single file by content inspection.

    Args:
        filename:   Original filename (used for extension detection only)
        file_bytes: Raw bytes of the file
        pass2:      If True, remap base keys to their pass-2 variants where applicable

    Returns:
        (key, confidence, label)
        key        — FILE_CONFIG slot name, or 'unknown'
        confidence — 0.0–1.0
        label      — Human-readable type description
    """
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

    if ext in ("xlsx", "xls"):
        key, conf = _classify_excel(filename, file_bytes, ext)
    elif ext == "pdf":
        key, conf = _classify_pdf(filename, file_bytes)
    else:
        key, conf = "unknown", 0.0

    if pass2 and key in _PASS2_REMAP:
        key = _PASS2_REMAP[key]

    return key, conf, FILE_LABELS.get(key, "Unknown — select type")


# ── Excel classifier ─────────────────────────────────────────────────────────

def _classify_excel(filename: str, file_bytes: bytes, ext: str) -> Tuple[str, float]:
    try:
        if ext == "xls":
            return _classify_xls(file_bytes)
        return _classify_xlsx(file_bytes)
    except Exception:
        return "unknown", 0.0


def _classify_xls(file_bytes: bytes) -> Tuple[str, float]:
    """Classify legacy .xls files — only Nexus uses .xls in this pipeline."""
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=file_bytes)
        ws = wb.sheet_by_index(0)
        # Read first 5 rows as flat text
        text = " ".join(
            str(ws.cell_value(r, c)).lower()
            for r in range(min(5, ws.nrows))
            for c in range(min(15, ws.ncols))
        )
        if "vendor" in text and "invoice" in text:
            return "nexus_accrual", 0.90
        if "nexus" in text:
            return "nexus_accrual", 0.85
    except Exception:
        pass
    return "nexus_accrual", 0.60  # Only .xls file type used is Nexus


def _classify_xlsx(file_bytes: bytes) -> Tuple[str, float]:
    """Classify .xlsx files by reading first rows of the active sheet."""
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)

    # ── Check sheet names first (prepaid ledger, workpaper) ──────────────
    sheet_names_lower = [s.lower() for s in wb.sheetnames]
    if any("prepaid" in s for s in sheet_names_lower):
        wb.close()
        return "prepaid_ledger", 0.92
    # Prepaid ledger seed file uses "Active" and "Completed" sheet names
    if "active" in sheet_names_lower and "completed" in sheet_names_lower:
        wb.close()
        return "prepaid_ledger", 0.90
    if any(s in ("gl vs tb", "bank rec", "debt service", "accruals") for s in sheet_names_lower):
        wb.close()
        return "prior_workpaper", 0.90
    # Workpaper sheets follow pattern "Jan-2026 GL vs TB" etc.
    if any(re.search(r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-\d{4}\b', s)
           for s in sheet_names_lower):
        wb.close()
        return "prior_workpaper", 0.88

    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i >= 12:
            break
        rows.append([str(v or "").strip() for v in (row or [])[:20]])
    wb.close()

    all_text = " ".join(cell for row in rows for cell in row).lower()

    # ── Kardin: distinctive sheet + header columns ────────────────────────
    # Kardin has 'qryExportData' sheet or PropID/M1–M12 headers
    if "propid" in all_text and ("m1" in all_text or "m12" in all_text):
        return "kardin_budget", 0.95
    if "kardin" in all_text:
        return "kardin_budget", 0.90

    # ── T12: "Statement (12 months)" + ≥10 month-label columns ──────────
    month_pattern = re.compile(
        r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{4}\b'
    )
    month_hits = sum(1 for row in rows for cell in row if month_pattern.search(cell.lower()))
    if month_hits >= 10:
        return "t12_statement", 0.95
    if "statement (12 months)" in all_text or ("period" in all_text and month_hits >= 6):
        return "t12_statement", 0.88

    # ── Trial Balance ─────────────────────────────────────────────────────
    if "trial balance" in all_text:
        return "trial_balance", 0.95

    # ── Budget Comparison ─────────────────────────────────────────────────
    if "budget comparison" in all_text:
        return "budget_comparison", 0.95
    if "ptd budget" in all_text and "ptd actual" in all_text:
        return "budget_comparison", 0.88

    # ── Receivable Detail / AR Aging ──────────────────────────────────────
    # Must come before the revlabpm→GL fallback: Yardi receivable reports
    # carry the property header ("revlabpm") just like the GL export.
    # AR Aging: Yardi titles it "AR Detail Aging" — "receivable" may not appear.
    if ("ar detail aging" in all_text or "ar aging" in all_text
            or "aging detail" in all_text):
        return "ar_aging", 0.92
    if ("aging" in all_text and (
        "charge code" in all_text or "tenant" in all_text
    ) and ("30" in all_text or "60" in all_text or "90" in all_text)):
        return "ar_aging", 0.85
    if "receivable" in all_text and (
        "charge code" in all_text or "tenant" in all_text
    ):
        if "aging" in all_text or "30" in all_text:
            return "ar_aging", 0.85
        return "receivable_detail", 0.85

    # ── GL: "General Ledger" OR property code + transaction structure ─────
    if "general ledger" in all_text:
        return "gl", 0.95
    # Yardi GL header: "Revolution Labs Owner, LLC (revlabpm)" in row 1
    if "revlabpm" in all_text:
        # Distinguish GL from TB/BC/T12 (those are already caught above)
        return "gl", 0.85

    # ── Prepaid Ledger (content check before Nexus — seed file has Vendor/Invoice cols) ──
    if ("monthly amt" in all_text or "months posted" in all_text
            or "service start" in all_text or "months left" in all_text):
        return "prepaid_ledger", 0.90

    # ── Nexus in xlsx form ────────────────────────────────────────────────
    if "nexus" in all_text or (
        "vendor" in all_text and "invoice" in all_text and "gl account" in all_text
    ):
        return "nexus_accrual", 0.88

    # ── Berkadia (xlsx version) ───────────────────────────────────────────
    if "berkadia" in all_text:
        return "loan", 0.92

    # ── Prepaid Ledger (column-name fallback) ─────────────────────────────
    if "monthly_amount" in all_text or (
        "monthly amt" in all_text and "months_amortized" in all_text
    ):
        return "prepaid_ledger", 0.88

    return "unknown", 0.0


# ── PDF classifier ───────────────────────────────────────────────────────────

def _classify_pdf(filename: str, file_bytes: bytes) -> Tuple[str, float]:
    try:
        import pdfplumber
        text = ""
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages[:3]:
                text += page.extract_text() or ""
    except Exception:
        # Fall back to filename heuristics if pdfplumber fails
        fn = filename.lower()
        if "bofa" in fn or "bankofamerica" in fn or "bank_of_america" in fn:
            return "bank_rec_dev", 0.60
        if "daca" in fn or "keybank" in fn:
            return "daca_bank", 0.60
        if "berkadia" in fn:
            return "loan", 0.60
        return "unknown", 0.0

    tl = text.lower()

    # ── Yardi DACA Bank Rec (Bank Rec Report for the DACA/KeyBank account) ─
    # Must check BEFORE the generic "bank reconciliation report" check below,
    # because the DACA rec PDF also contains that phrase on page 1.
    if "bank reconciliation report" in tl and (
        "deposit account" in tl
        or "daca" in tl
        or "keybank" in tl
        or "329681415132" in text
    ):
        return "daca_bank", 0.97

    # ── Yardi Bank Rec — PNC Operating (contains "Bank Reconciliation Report") ─
    if "bank reconciliation report" in tl:
        return "bank_rec", 0.97

    # ── KeyBank DACA statement (standalone, no Yardi header) ─────────────
    if "keybank" in tl and ("5132" in text or "daca" in tl):
        return "daca_bank", 0.95

    # ── Bank of America (development account) ────────────────────────────
    if "bank of america" in tl or "bofa" in tl:
        return "bank_rec_dev", 0.95

    # ── Berkadia loan statements ──────────────────────────────────────────
    if "berkadia" in tl:
        return "loan", 0.95

    # ── PNC statement as standalone (fallback bank rec) ───────────────────
    if "pnc" in tl and (
        "account summary" in tl or "corporate business" in tl or "ending balance" in tl
    ):
        return "bank_rec", 0.82

    return "unknown", 0.0
