"""Plain-language help for BankLake: used by the dashboard's help boxes and Guide page, and written to docs/USER_GUIDE.md."""

# internal page name -> (friendly title, what is this, how to use it)
PAGE_HELP = {
    "Overview": ("Overview", "Pipeline health and the end-to-end flow, computed from the execution logs.",
                 "Read the metric strip, then the stage cards. Open Actions in the header to generate data or run pipelines."),
    "Ingestion": ("Source & Ingestion", "Where the source files come from. Every file that arrives is logged, and a file with the same content is never loaded twice.",
                  "Use the buttons to create fictional bank data or add a new day of activity. The table at the bottom is the receipt for every file."),
    "Bronze": ("Bronze Layer", "A faithful copy of what arrived, stored as text, with the file name and load time added. Nothing is fixed or removed here.",
               "Pick a table to see its first rows. The summary shows how many files loaded, were skipped as duplicates or were rejected."),
    "Silver Merge": ("Silver Merge", "Applies new, changed and deleted records to the current picture of each table. Deleted records are flagged, not erased.",
                     "The big numbers add up what happened to the records: new, changed, deleted, ignored (too old) and duplicates removed."),
    "Silver Transform": ("Silver Transform", "Fixes formats, checks the rules (for example, a balance cannot be text), hides personal details and sets aside bad records with the reason.",
                         "Pick a table to see why records were set aside and which columns are hidden."),
    "Gold": ("Gold Layer", "Summary tables built for the business: branches, customers, deposits, loans, transactions.",
             "Open a tab to see a chart and the table behind it. The Lineage tab shows exactly which source column each report column comes from."),
    "Metadata & orchestration": ("Metadata & Orchestration", "The rule book that decides which steps run, in what order, how often a failed step retries and what waits for what.",
                                 "Press Run to process data. Untick is_active on a step in the table to switch it off; that is all it takes to change the behaviour."),
    "SQL migration": ("SQL Migration", "Converts reports written in an old bank database's SQL dialect into the SQL used on AWS, and proves the answers are identical.",
                      "Press the button. Each query shows the old text, the new text, which rules were applied and whether the results match. Unsupported syntax is reported, never guessed."),
    "Data quality & reconciliation": ("Reconciliation", "Counts the records at the start and the end of every step and compares them, so nothing is lost or changed silently.",
                                      "Press Run the checks. Green means everything matches. Red names the first step where it stopped matching and why."),
    "Security & masking": ("Security & Masking", "Personal details (names, emails, phone numbers) are hidden everywhere in the reports. The real values sit in a separate encrypted vault that only some roles can open.",
                           "Pick a role in the left bar, look up a customer and press the unmask button. Allowed roles see real values; others are refused, and both outcomes are logged."),
    "Audit & monitoring": ("Audit & Monitoring", "A diary of everything the system did, plus alerts for anything that needs attention.",
                           "Look at the alerts first. Open a tab for the detail of runs, file loads, record changes and errors."),
    "BI dashboards": ("BI Reports", "The charts a bank manager would use to take decisions. They are built only from the finished Gold reports. All numbers are fictional.",
                      "Pick a tab. Hover over a chart for exact values."),
    "Table journey": ("Table journey", "Follows one table from the raw file through every stage to its report, showing the row count at each step.",
                      "Pick a table and read the numbered list from top to bottom. The tabs show the same rows at each stage."),
    "Data explorer": ("Data explorer", "A plain table browser for every dataset in the lake. Sensitive values are masked.",
                      "Pick a layer and a table, optionally filter by text."),
    "Customers": ("Customer lookup", "Browse fictional customers and see one customer's whole picture: accounts, deposits, loans and recent activity.",
                  "Search or choose a customer. Names and contacts show as stars unless the role in the left bar is allowed to see them."),
}

ANALOGY = ("Think of a water treatment plant. Raw water comes in from the bank's core system (Finacle, held in Teradata). It is collected and made "
           "safe straight away: personal details are masked at Bronze. The daily changes are applied (Silver Merge), the data is cleaned and checked "
           "(Silver Transform), and it is bottled as ready-to-use reporting tables (Gold), where the real values are put back for the people who build "
           "reports. BankLake also checks that the number of records that came out matches the number that went in.")

QUICK_START = [
    ("Open the console", "Run `streamlit run dashboard/app.py` in the project folder. It opens in your browser at http://localhost:8501."),
    ("Build the data", "On the first page press **Build the demo**. It creates fictional Finacle-style source files and runs every pipeline from Bronze to Gold (about two minutes)."),
    ("Read the pipeline", "The **ETL Pipeline** page shows Source, then 1 Ingestion (Bronze), 2 Transformation (Silver Merge, Silver Transform, Gold) and 3 Reporting (BI), with live row counts. Bronze and Silver are masked; Gold and BI are unmasked."),
    ("Open each part", "Use the buttons under the diagram: **1 Ingestion** shows the source tables and the PII masking, **2 Transformation** shows each step and the Teradata to Athena and EMR conversion, **3 Reporting and BI** shows the Gold tables and the dashboards."),
    ("Validate", "Open **Validation** and press **Run validation now** to compare record counts between the source and each layer."),
]

DEMO_SCRIPT = [
    ("0:00 The pipeline", "ETL Pipeline page: follow Source, Bronze, Silver Merge, Silver Transform, Gold, BI. Say which layers are masked and which are not."),
    ("1:00 Ingestion", "Open 1 Ingestion: source tables, the masking rules, and a Bronze preview showing masked values."),
    ("2:00 Transformation", "Open 2 Transformation: Silver Merge, Silver Transform (set-aside rows with reasons), then Gold and which Silver tables build each Gold table."),
    ("3:00 Teradata to Athena", "Same page, last tab: load the example procedure, show the Athena SQL and the generated EMR script."),
    ("4:00 Reporting", "Open 3 Reporting and BI: the Gold tables and the executive dashboards."),
    ("4:30 Validation", "Open Validation: source versus Bronze, Silver and Gold counts, then run the checks."),
]

PAGE_PURPOSE = [
    ("ETL Pipeline", "The whole flow on one diagram, with live row counts and which layers are masked.", "Everyone"),
    ("1 Ingestion", "Which source tables arrive from Finacle (Teradata) and how PII is masked at Bronze.", "Data engineers"),
    ("2 Transformation", "Silver Merge, Silver Transform and Gold, plus the Teradata to Athena conversion and EMR script.", "Data engineers, migration teams"),
    ("3 Reporting and BI", "The Gold tables and the executive dashboards built from them.", "Analysts, managers"),
    ("Validation", "Do record counts match between the source and each layer, and can every difference be explained?", "Data owners, auditors"),
]

ROLES = [
    ("executive, analyst", "See masked values only (stars)."),
    ("branch_manager", "Can unmask customer names, not phone numbers or emails."),
    ("relationship_manager", "Can unmask names, emails and phone numbers of customers."),
    ("compliance_officer, admin", "Can unmask everything; admin can also change masking rules and permissions."),
]
ROLE_NOTE = ("Masked layers (Bronze, Silver Merge, Silver Transform) hold no real personal details. Gold is unmasked by the pipeline itself (logged as UNMASK_AT_GOLD). "
             "The roles below apply when a person asks the vault for real values directly; the console has no login, and a real deployment would take the role from the bank's sign-in system.")

GLOSSARY = [
    ("Synthetic data", "Fictional data generated by the project. No real person, bank or client is involved."),
    ("Lakehouse", "One storage area (files) that is organised in layers so it can serve both reports and detailed analysis."),
    ("Raw data", "The source files exactly as they arrive."),
    ("Bronze", "The first layer: an untouched copy of the raw data, plus the file name and load time."),
    ("Silver Merge", "The second layer: applies inserts, updates and deletes so each table shows the current truth."),
    ("Silver Transform", "The third layer: cleaning, rule checks and masking. Bad records are set aside."),
    ("Silver", "The result of Silver Transform: clean, checked, masked tables."),
    ("Gold", "Business-ready summary tables built from Silver."),
    ("Quarantine", "A separate area where rejected records are kept together with the reason, so nothing disappears silently."),
    ("CDC (change data capture)", "Sending only what changed (new, updated, deleted records) instead of the whole table each time."),
    ("Soft delete", "Marking a record as deleted without erasing it, so history is kept."),
    ("Pipeline", "One automated step, such as 'load customers into Bronze'."),
    ("Orchestration", "Running pipelines in the right order, retrying failures and holding back steps whose inputs failed."),
    ("Metadata", "Data about the data: here, the rule book that tells the system which pipelines exist and how they run."),
    ("Watermark", "A bookmark recording the last batch a pipeline processed, so the next run only does new work."),
    ("Reconciliation", "Comparing counts and totals before and after a step to prove nothing was lost or changed."),
    ("Masking", "Replacing personal details with stars, for example a***@example.com."),
    ("Token", "A stable code standing in for a real value, so records can still be joined without exposing the value."),
    ("Vault", "A separate, encrypted store holding the real personal values. Only some roles can open it, and every opening is logged."),
    ("Role", "A job type (analyst, branch manager...) that decides what a person may see."),
    ("Audit log", "The permanent diary of what happened and who did it."),
    ("Lineage", "The trail showing where a number in a report came from."),
    ("SQL migration", "Converting reports written for an old database into the SQL used on AWS, then proving the results match."),
    ("Athena / Glue", "AWS services for querying files in S3 with SQL (Athena) and keeping the table catalogue (Glue)."),
    ("Parquet", "A compact column-based file format used to store the layers."),
]

FAQ = [
    ("It says 'run the pipelines first' or a page is empty.", "Nothing has been created yet. Press Build the demo on the first page, or use Actions (top right)."),
    ("Why does the first action take so long?", "The data engine (Spark) needs about ten seconds to start, and processing runs in a separate step. Later runs are quicker."),
    ("Will 'Create sample bank data' delete my work?", "It resets the data created by this project (the lake, source files and run history). It does not touch anything outside the project folder."),
    ("Why are names and phone numbers stars?", "Bronze, Silver Merge and Silver Transform are masked by design. Gold and the BI pages are the unmasked layer, so they show real (synthetic) names and phones."),
    ("Why do some records end up in quarantine?", "The generated data contains deliberate mistakes (missing keys, impossible values, duplicates). Quarantine shows each record with the reason it was set aside."),
    ("A check is red. Is something broken?", "Maybe by design: a check fails when counts between two layers differ without a recorded reason. The Validation page lists failures first with the stage and detail."),
    ("Is any of this real bank data?", "No. Everything is synthetic and the code is original."),
    ("Can I change what runs?", "Yes: set is_active to N for a pipeline in the metadata table (pipeline_metadata). Dependent steps will then be held back."),
]


def to_markdown() -> str:
    L = ["# How to use BankLake", "",
         "BankLake is a demonstration of a modern banking data platform built on fictional data. This guide is for anyone opening the website, including people with no technical background.", "",
         "## The idea in one paragraph", "", ANALOGY, "", "## Quick start", ""]
    L += [f"{i}. **{t}.** {d}" for i, (t, d) in enumerate(QUICK_START, 1)]
    L += ["", "## Layout of the console", "",
          "The left bar lists the pages. The bar across the top of every page shows the environment, the latest run ID and batch, the pipeline status and the last successful run. **Actions** (top right) runs the real commands: generate data, run pipelines, add a batch, reconcile. Each page has a **What am I looking at?** box where it helps.", "",
          "## Which page answers which question", "", "| Page | Question it answers | Who it is for |", "|---|---|---|"]
    L += [f"| {a} | {b} | {c} |" for a, b, c in PAGE_PURPOSE]
    L += ["", "## Roles", "", ROLE_NOTE, "", "| Role | What it can see |", "|---|---|"] + [f"| {a} | {b} |" for a, b in ROLES]
    L += ["", "## A five-minute demo", ""] + [f"- **{t}** — {d}" for t, d in DEMO_SCRIPT]
    L += ["", "## Glossary", "", "| Term | Meaning |", "|---|---|"] + [f"| {t} | {d} |" for t, d in GLOSSARY]
    L += ["", "## Questions and problems", ""]
    for q, a in FAQ:
        L += [f"**{q}**  ", a, ""]
    L += ["## What each page does (details)", "", "| Page | What it is | How to use it |", "|---|---|---|"]
    L += [f"| {v[0]} | {v[1]} | {v[2]} |" for v in PAGE_HELP.values()]
    return "\n".join(L) + "\n"
