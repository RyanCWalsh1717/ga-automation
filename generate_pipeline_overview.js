const {
  Document,
  Packer,
  Paragraph,
  TextRun,
  Header,
  Footer,
  AlignmentType,
  HeadingLevel,
  LevelFormat,
  PageNumber,
  TabStopType,
  TabStopPosition,
} = require("docx");
const fs = require("fs");

// ─── Helpers ────────────────────────────────────────────────────────────────

function calibri(text, opts = {}) {
  return new TextRun({ text, font: "Calibri", ...opts });
}

function body(text, spacing = { before: 60, after: 160 }) {
  return new Paragraph({
    children: [calibri(text, { size: 22 })],
    spacing,
  });
}

function bodySpace(text) {
  return body(text, { before: 0, after: 200 });
}

function stepLabel(label) {
  // Bold step label paragraph, slightly indented look via spacing
  return new Paragraph({
    children: [calibri(label, { bold: true, size: 22 })],
    spacing: { before: 160, after: 60 },
  });
}

function layerItem(boldPart, rest) {
  return new Paragraph({
    numbering: { reference: "layers", level: 0 },
    children: [
      calibri(boldPart, { bold: true, size: 22 }),
      calibri(rest, { size: 22 }),
    ],
    spacing: { before: 60, after: 60 },
  });
}

function bullet(text) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    children: [calibri(text, { size: 22 })],
    spacing: { before: 40, after: 40 },
  });
}

function numberedItem(text) {
  return new Paragraph({
    numbering: { reference: "numbered", level: 0 },
    children: [calibri(text, { size: 22 })],
    spacing: { before: 60, after: 60 },
  });
}

function sectionHeader(text) {
  return new Paragraph({
    children: [calibri(text, { bold: true, size: 26, color: "1F3864" })],
    spacing: { before: 320, after: 120 },
    border: {
      bottom: { style: "single", size: 6, color: "1F3864", space: 4 },
    },
  });
}

function subHeader(text) {
  return new Paragraph({
    children: [calibri(text, { bold: true, size: 22, color: "2E5496" })],
    spacing: { before: 200, after: 60 },
  });
}

function spacer() {
  return new Paragraph({
    children: [new TextRun("")],
    spacing: { before: 0, after: 80 },
  });
}

// ─── Document ───────────────────────────────────────────────────────────────

const doc = new Document({
  numbering: {
    config: [
      {
        reference: "bullets",
        levels: [
          {
            level: 0,
            format: LevelFormat.BULLET,
            text: "•",
            alignment: AlignmentType.LEFT,
            style: {
              paragraph: { indent: { left: 540, hanging: 260 } },
              run: { font: "Calibri", size: 22 },
            },
          },
        ],
      },
      {
        reference: "layers",
        levels: [
          {
            level: 0,
            format: LevelFormat.BULLET,
            text: "▪",
            alignment: AlignmentType.LEFT,
            style: {
              paragraph: { indent: { left: 720, hanging: 260 } },
              run: { font: "Calibri", size: 22 },
            },
          },
        ],
      },
      {
        reference: "numbered",
        levels: [
          {
            level: 0,
            format: LevelFormat.DECIMAL,
            text: "%1.",
            alignment: AlignmentType.LEFT,
            style: {
              paragraph: { indent: { left: 540, hanging: 260 } },
              run: { font: "Calibri", size: 22 },
            },
          },
        ],
      },
    ],
  },

  sections: [
    {
      properties: {
        page: {
          size: { width: 12240, height: 15840 },
          margin: { top: 1260, right: 1260, bottom: 1260, left: 1260 },
        },
      },

      // ── Footer ──────────────────────────────────────────────────────────
      footers: {
        default: new Footer({
          children: [
            new Paragraph({
              children: [
                calibri("Confidential — Greatland Realty Partners  |  May 2026", {
                  size: 18,
                  color: "777777",
                }),
                new TextRun({
                  children: ["\t", PageNumber.CURRENT],
                  font: "Calibri",
                  size: 18,
                  color: "777777",
                }),
              ],
              tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
              border: {
                top: { style: "single", size: 4, color: "CCCCCC", space: 4 },
              },
            }),
          ],
        }),
      },

      // ── Content ─────────────────────────────────────────────────────────
      children: [
        // ── TITLE BLOCK ─────────────────────────────────────────────────
        new Paragraph({
          children: [
            calibri("Revolution Labs — Monthly Close Automation", {
              bold: true,
              size: 40,
              color: "1F3864",
            }),
          ],
          alignment: AlignmentType.CENTER,
          spacing: { before: 240, after: 80 },
        }),
        new Paragraph({
          children: [
            calibri("How the Pipeline Works", {
              bold: true,
              size: 28,
              color: "2E5496",
            }),
          ],
          alignment: AlignmentType.CENTER,
          spacing: { before: 0, after: 80 },
        }),
        new Paragraph({
          children: [calibri("Prepared for the Accounting Manager", { size: 20, italics: true, color: "555555" })],
          alignment: AlignmentType.CENTER,
          spacing: { before: 0, after: 360 },
        }),

        // ── INTRO ────────────────────────────────────────────────────────
        bodySpace(
          "The GA Automation Pipeline replaces the manual journal entry preparation previously performed by JLL’s India team. Each month it reads data directly from Yardi and supporting systems, detects what needs to be accrued, and produces ready-to-import journal entries and review reports. The Property Accountant reviews all outputs before they reach the Accounting Manager."
        ),
        bodySpace(
          "The process runs in two stages: Pass 1 (before close) and Pass 2 (after close)."
        ),
        spacer(),

        // ══════════════════════════════════════════════════════════════════
        // SECTION 1: PASS 1
        // ══════════════════════════════════════════════════════════════════
        sectionHeader("Pass 1 — Journal Entry Generation (Pre-Close)"),
        body(
          "Before the monthly close, the pipeline reads the pre-close Yardi GL and detects every accrual entry needed to complete the period."
        ),

        // Step 1
        stepLabel("Step 1 — Files Uploaded"),
        body("The following files are loaded into the app at the start of each month:"),
        bullet("Yardi General Ledger (pre-close export) — the complete list of all transactions posted so far"),
        bullet("Nexus AP Accrual Detail — approved invoices not yet posted to Yardi"),
        bullet("Kardin Budget Comparison — budget vs. actual by account"),
        bullet("Receivable Detail & AR Aging — used to calculate the management fee basis"),
        bullet("KeyBank DACA Statement — backup source for management fee if AR reports unavailable"),
        bullet("Berkadia Loan Statements — debt service and escrow balances"),
        bullet("12-Month Income Statement (optional) — improves January accrual accuracy using prior December actuals"),

        // Step 2
        stepLabel("Step 2 — The GL Is Read and Parsed"),
        body(
          "The pipeline reads every account in the GL, identifies the reporting period, and notes which accounts already have journal entries posted. Accounts with existing entries are automatically excluded from accrual detection — the system will not create a duplicate."
        ),

        // Step 3
        stepLabel("Step 3 — Accrual Detection (Five Layers)"),
        body(
          "The system works through five layers in priority order. Once an account is claimed by an earlier layer, all later layers skip it — there is no double-counting."
        ),
        layerItem(
          "Layer 1 — Open Nexus Invoices:  ",
          "Invoices that have been approved in Nexus (the AP system) but not yet posted to Yardi are accrued in full. The system cross-checks against the GL to avoid duplicating invoices already posted."
        ),
        layerItem(
          "Layer 2 — Invoice Proration:  ",
          "For recurring vendors already partially recorded in the GL for this period (e.g., a monthly service invoice received mid-month), the system calculates the remaining days and prorates the unrecorded portion."
        ),
        layerItem(
          "Layer 3 — Historical Recurring Patterns:  ",
          "Accounts that consistently had expenses in prior months but are silent this period — utility estimates, contracted services, recurring retainers — receive an accrual based on the average of prior months’ actuals. The system uses the Budget Comparison YTD figure divided by months elapsed."
        ),
        layerItem(
          "Layer 4 — Budget Gap:  ",
          "As a last resort, accounts with a meaningful budget allocation and no GL activity receive an accrual at the budgeted amount. This catches contracted services that slipped through the earlier layers."
        ),
        layerItem(
          "Layer 5 — Payroll Bonus Accruals:  ",
          "Monthly allocations for engineering and administrative bonuses are accrued based on the Kardin budget, with the logic to skip months when the actual bonus payment is expected."
        ),

        // Step 4
        stepLabel("Step 4 — Management Fee"),
        body(
          "The monthly management fee is calculated at 3.00% of cash received (1.25% JLL + 1.75% GRP). The fee basis is pulled from the Receivable Detail report, which matches JLL’s own calculation method. Prepayments are excluded from the basis."
        ),

        // Step 5
        stepLabel("Step 5 — Real Estate Tax and Insurance"),
        body(
          "Monthly amortization entries for real estate taxes and property insurance are generated automatically. RE Tax is accrued as one-third of the quarterly bill each non-payment month, with a separate escrow entry in payment months (January, April, July, October). Insurance is drawn down monthly from the prepaid balance."
        ),

        // Step 6
        stepLabel("Step 6 — One-Off Items (User Input)"),
        body(
          "The Property Accountant enters any known items that the system cannot detect automatically — semi-annual billings, seasonal items, items received after the Nexus cutoff — directly in the app before running Pass 1."
        ),

        // Step 7
        stepLabel("Step 7 — Gut-Check: Existing Manual Journal Entries"),
        body(
          "Before generating output, the system scans the GL for any journal entries that were manually posted before the pipeline ran. These accounts are flagged in a review panel so the Property Accountant can confirm the posting was intentional. If correct, no accrual is generated (no double-counting). If incorrect, the entry can be removed from Yardi and Pass 1 re-run."
        ),

        // Step 8
        stepLabel("Step 8 — Output: Three Journal Entry Files"),
        body("The pipeline exports three separate files formatted for direct import into Yardi:"),
        bullet("GA_Accruals_JE.csv — all accruals (Nexus invoices, proration, historical, budget gap, management fee, RE tax, insurance, one-off items, tenant utility billings)"),
        bullet("GA_Prepaid_JE.csv — prepaid amortization schedule entries"),
        bullet("GA_Manual_JE.csv — any manually entered journal entries and reclasses"),
        body("The Property Accountant uploads each file to Yardi and runs the final close."),
        spacer(),

        // ══════════════════════════════════════════════════════════════════
        // SECTION 2: PASS 2
        // ══════════════════════════════════════════════════════════════════
        sectionHeader("Pass 2 — Report Generation (Post-Close)"),
        body(
          "After the close is run and all journal entries are posted, the pipeline reads the final GL and generates the review package."
        ),

        // Step 1
        stepLabel("Step 1 — Final Files Uploaded"),
        body("The same files are re-exported from Yardi after all journal entries have posted:"),
        bullet("Final Yardi GL (post-close)"),
        bullet("Final Trial Balance"),
        bullet("Final Budget Comparison (actuals updated)"),
        bullet("Yardi Bank Reconciliation"),
        bullet("Berkadia Loan Statements"),

        // Step 2
        stepLabel("Step 2 — GL vs. Trial Balance Tie-Out"),
        body(
          "Every balance sheet account is compared between the GL and the Trial Balance. Differences are flagged. In a clean close, all accounts tie to zero."
        ),

        // Step 3
        stepLabel("Step 3 — Seven-Point QC Checklist"),
        body(
          "The pipeline runs seven automated checks and produces a QC workbook with a pass/flag/fail for each:"
        ),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Trial Balance vs. Budget Comparison tie-out", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [
            calibri("Budget variances — accounts with significant over or under spend are flagged (Tier 1: ≥$5,000 or 5% of budget; Tier 2: $2,500–$5,000)", { size: 22 }),
          ],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Workpaper vs. Trial Balance — balance sheet account balances reconciled to workpapers", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Month-over-month swings — expense accounts with unusual changes from the prior month are flagged for review", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Balance sheet workpaper tie-out", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Accrual coverage — confirms material expense accounts were either posted or accrued", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),
        new Paragraph({
          numbering: { reference: "numbered", level: 0 },
          children: [calibri("Miscellaneous checks — insurance amortization, management fee calculation, RE tax escrow", { size: 22 })],
          spacing: { before: 40, after: 40 },
        }),

        // Step 4
        stepLabel("Step 4 — Variance Commentary"),
        body(
          "For every flagged budget variance, the pipeline generates a plain-English explanation of why the account is over or under budget. These comments are inserted directly into the annotated Budget Comparison report."
        ),

        // Step 5
        stepLabel("Step 5 — Output: Four Report Files"),
        bullet("GA_Workpapers.xlsx — monthly close workpaper with GL vs. TB tie-out for all balance sheet accounts. Grows month-over-month as prior periods are carried forward."),
        bullet("GA_QC_Workbook.xlsx — seven-point QC checklist with status and supporting detail for each check"),
        bullet("GA_Exceptions_Report.xlsx — all flagged items in one place for review"),
        bullet("GA_Budget_Comparison_Internal.xlsx — annotated budget comparison with variance commentary for GRP internal review"),
        spacer(),

        // ══════════════════════════════════════════════════════════════════
        // SECTION 3: WHAT IT DOES NOT DO
        // ══════════════════════════════════════════════════════════════════
        sectionHeader("What the Pipeline Does Not Do"),
        bullet(
          "Generate the Singerman monthly reporting package — that is downloaded directly from Yardi after the close"
        ),
        bullet(
          "Post journal entries to Yardi — the Property Accountant uploads the CSV files manually after reviewing them"
        ),
        bullet(
          "Make final accounting judgments — unusual items are flagged for the Property Accountant to review before the Accounting Manager sees the output"
        ),
        spacer(),

        // ══════════════════════════════════════════════════════════════════
        // SECTION 4: REVIEW AND SIGN-OFF
        // ══════════════════════════════════════════════════════════════════
        sectionHeader("Review and Sign-Off Flow"),
        body(
          "All outputs pass through a structured review before the Accounting Manager receives the final package:"
        ),
        numberedItem(
          "The Property Accountant runs Pass 1, reviews the proposed journal entries, and uploads the CSV files to Yardi."
        ),
        numberedItem("The final close is run in Yardi."),
        numberedItem(
          "The Property Accountant runs Pass 2 and reviews the QC workbook and exception report."
        ),
        numberedItem(
          "The Accounting Manager receives the final reports after the Property Accountant completes their review."
        ),
        spacer(),
      ],
    },
  ],
});

// ─── Write File ─────────────────────────────────────────────────────────────

const OUTPUT_PATH =
  "C:\\Users\\RyanCWalsh\\.claude\\ga-automation\\GA_Pipeline_Overview.docx";

Packer.toBuffer(doc).then((buffer) => {
  fs.writeFileSync(OUTPUT_PATH, buffer);
  console.log("SUCCESS: Written to", OUTPUT_PATH);
  console.log("File size:", buffer.length, "bytes");
});
