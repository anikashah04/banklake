"""Metadata + audit database (SQLite). Pipeline config lives here, not in code."""
import sqlite3
from datetime import datetime

from .common import DATA, DB_PATH
from .schemas import ENTITIES, FLAG_COLUMNS, ORDER, SOURCE_MASKING, SOURCES

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_metadata(
  pipeline_id TEXT PRIMARY KEY, source_table TEXT, target_table TEXT,
  source_layer TEXT, target_layer TEXT, load_strategy TEXT, primary_key TEXT,
  is_active TEXT DEFAULT 'Y', handler TEXT, max_retries INTEGER DEFAULT 2,
  source_database TEXT, target_database TEXT, watermark_column TEXT, execution_order INTEGER);
CREATE TABLE IF NOT EXISTS pipeline_dependencies(
  pipeline_id TEXT, depends_on TEXT, PRIMARY KEY(pipeline_id, depends_on));
CREATE TABLE IF NOT EXISTS masking_metadata(
  entity TEXT, column_name TEXT, rule TEXT, is_active TEXT DEFAULT 'Y',
  PRIMARY KEY(entity, column_name));
CREATE TABLE IF NOT EXISTS role_permissions(
  role TEXT, entity TEXT, column_name TEXT, PRIMARY KEY(role, entity, column_name));
CREATE TABLE IF NOT EXISTS pipeline_watermark(
  pipeline_id TEXT PRIMARY KEY, last_batch_id INTEGER, last_run_id TEXT, updated_ts TEXT);
CREATE TABLE IF NOT EXISTS ingested_files(
  entity TEXT, file_name TEXT, batch_id INTEGER, rows_read INTEGER, rows_loaded INTEGER, ts TEXT,
  PRIMARY KEY(entity, file_name));
CREATE TABLE IF NOT EXISTS ingestion_audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, batch_id INTEGER, entity TEXT, source_file TEXT,
  source_system TEXT, load_type TEXT, file_hash TEXT, records_read INTEGER, records_loaded INTEGER,
  records_rejected INTEGER, status TEXT, message TEXT, ts TEXT);
CREATE TABLE IF NOT EXISTS transformation_audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, pipeline_id TEXT, layer TEXT, entity TEXT, batch_id INTEGER,
  load_type TEXT, rows_in INTEGER, inserts INTEGER, updates INTEGER, deletes INTEGER, ignored INTEGER,
  duplicates_removed INTEGER, rejected INTEGER, rows_out INTEGER, ts TEXT);
CREATE TABLE IF NOT EXISTS source_systems(
  source_id TEXT PRIMARY KEY, name TEXT, system_code TEXT, db_file TEXT);
CREATE TABLE IF NOT EXISTS source_tables(
  source_id TEXT, entity TEXT, load_frequency TEXT, date_column TEXT DEFAULT 'business_date', is_active TEXT DEFAULT 'Y',
  PRIMARY KEY(source_id, entity));
CREATE TABLE IF NOT EXISTS bronze_transform_rules(
  entity TEXT, column_name TEXT, rule TEXT, is_active TEXT DEFAULT 'Y', PRIMARY KEY(entity, column_name));
CREATE TABLE IF NOT EXISTS ingestion_runs(
  run_id TEXT PRIMARY KEY, business_date TEXT, status TEXT, start_ts TEXT, end_ts TEXT,
  tables_total INTEGER, tables_ok INTEGER, tables_failed INTEGER, tables_skipped INTEGER);
CREATE TABLE IF NOT EXISTS ingestion_batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, business_date TEXT, source_id TEXT, entity TEXT,
  load_frequency TEXT, period_key TEXT, period_end TEXT, batch_id INTEGER, status TEXT,
  source_count INTEGER, extracted INTEGER, transformed INTEGER, inserted INTEGER, bronze_count INTEGER,
  failed_stage TEXT, message TEXT, start_ts TEXT, end_ts TEXT);
CREATE TABLE IF NOT EXISTS pipeline_execution_log(
  execution_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, pipeline_id TEXT,
  source_table TEXT, target_table TEXT, start_ts TEXT, end_ts TEXT, duration_s REAL,
  rows_in INTEGER, rows_out INTEGER, rows_rejected INTEGER, status TEXT,
  error_message TEXT, retry_count INTEGER DEFAULT 0, run_type TEXT DEFAULT 'RUN');
CREATE TABLE IF NOT EXISTS pipeline_error_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, pipeline_id TEXT, ts TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS data_quality_results(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, entity TEXT, check_name TEXT,
  status TEXT, value TEXT, detail TEXT, ts TEXT);
CREATE TABLE IF NOT EXISTS reconciliation_results(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, entity TEXT, stage TEXT, check_name TEXT,
  source_value TEXT, target_value TEXT, status TEXT, detail TEXT, ts TEXT);
CREATE TABLE IF NOT EXISTS security_audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, actor TEXT, action TEXT, target TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS sql_migration_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, query_name TEXT, legacy_sql TEXT,
  converted_sql TEXT, applied TEXT, unsupported TEXT, status TEXT, match INTEGER, detail TEXT);
"""

# Which strategy each entity uses (full reload vs incremental merge)
STRATEGY = {"customer": "incremental", "account": "incremental", "transaction": "incremental",
            "branch": "full", "deposit": "full", "loan": "full"}

GOLD = {  # pipeline -> (target table, depends on silver entities, load strategy)
    "customer_360": (["customer", "account", "transaction", "deposit", "loan"], "full"),
    "branch_performance": (["branch", "account", "transaction"], "full"),
    "deposit_summary": (["deposit", "customer"], "monthly"),
    "loan_summary": (["loan", "customer"], "full"),
    "transaction_summary": (["transaction"], "full"),
}

SILVER_DEPS = {"account": ["customer"], "transaction": ["account"],
               "deposit": ["customer"], "loan": ["customer"]}

MASKING = [("customer", "customer_name", "NAME"), ("customer", "email", "EMAIL"),
           ("customer", "phone", "PHONE"), ("account", "account_number", "ACCOUNT")]


# role -> columns it may UNMASK ('*' = all). Roles not listed (analyst, executive) always see masked data.
ROLES = ["executive", "analyst", "branch_manager", "relationship_manager", "compliance_officer", "admin"]
UNMASK = [("branch_manager", "customer", "customer_name"),
          ("relationship_manager", "customer", "customer_name"), ("relationship_manager", "customer", "phone"),
          ("relationship_manager", "customer", "email"),
          ("compliance_officer", "*", "*"), ("admin", "*", "*")]


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def conn():
    DATA.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def query(sql: str, params=()) -> list[dict]:
    c = conn()
    try:
        return [dict(r) for r in c.execute(sql, params).fetchall()]
    finally:
        c.close()


def execute(sql: str, params=()) -> int:
    c = conn()
    try:
        cur = c.execute(sql, params)
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


def _add_column(c, table: str, column: str, decl: str) -> None:
    """Tiny migration helper: add a column to an existing table if an older DB lacks it."""
    if column not in [r[1] for r in c.execute(f"PRAGMA table_info({table})")]:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


# Logical database (Glue database name in the cloud) for each layer
LAYER_DB = {"raw": "raw_files", "bronze": "banklake_bronze", "silver_merge": "banklake_silver_merge",
            "silver": "banklake_silver", "gold": "banklake_gold"}


def _seed(c, pid, src_table, tgt_table, src_layer, tgt_layer, strategy, key, handler, wm_col, order):
    """Insert a pipeline definition once; fill the newer columns on rows created by older versions.
    Values a user already edited are never overwritten."""
    c.execute("INSERT OR IGNORE INTO pipeline_metadata(pipeline_id,source_table,target_table,source_layer,target_layer,"
              "load_strategy,primary_key,is_active,handler,max_retries) VALUES(?,?,?,?,?,?,?,'Y',?,2)",
              (pid, src_table, tgt_table, src_layer, tgt_layer, strategy, key, handler))
    c.execute("UPDATE pipeline_metadata SET source_database=COALESCE(source_database,?), target_database=COALESCE(target_database,?), "
              "watermark_column=COALESCE(watermark_column,?), execution_order=COALESCE(execution_order,?) WHERE pipeline_id=?",
              (LAYER_DB[src_layer], LAYER_DB[tgt_layer], wm_col, order, pid))


def init_db() -> None:
    """Create tables and seed pipeline metadata (never overwrites user edits)."""
    c = conn()
    c.executescript(DDL)
    _add_column(c, "ingested_files", "file_hash", "TEXT")
    for col, decl in [("source_database", "TEXT"), ("target_database", "TEXT"), ("watermark_column", "TEXT"), ("execution_order", "INTEGER")]:
        _add_column(c, "pipeline_metadata", col, decl)
    _add_column(c, "pipeline_watermark", "last_run_id", "TEXT")
    _add_column(c, "pipeline_watermark", "updated_ts", "TEXT")
    _add_column(c, "pipeline_execution_log", "run_type", "TEXT DEFAULT 'RUN'")
    _add_column(c, "transformation_audit_log", "rows_out", "INTEGER")
    for col, decl in [("layer", "TEXT"), ("entity", "TEXT"), ("batch_id", "INTEGER"), ("failure_reason", "TEXT")]:
        _add_column(c, "pipeline_execution_log", col, decl)
    # Silver is now produced by Silver Transform; point databases created by older versions at the new handlers.
    c.execute("UPDATE pipeline_metadata SET handler=REPLACE(handler,'banklake.silver.run_','banklake.silver_transform.loader.run_'), "
              "source_layer='silver_merge' WHERE handler LIKE 'banklake.silver.run_%'")
    for e, strat in STRATEGY.items():
        key, idx = ENTITIES[e]["key"], ORDER.index(e)
        _seed(c, f"bronze_{e}", f"raw/{e}", e, "raw", "bronze", strat, key, "banklake.bronze.run", None, 10 + idx)
        _seed(c, f"merge_{e}", e, e, "bronze", "silver_merge", strat, key, "banklake.silver_merge.loader.run", "_batch_id", 20 + idx)
        _seed(c, f"silver_{e}", e, e, "silver_merge", "silver", strat, key,
              f"banklake.silver_transform.loader.run_{e}", "_batch_id", 30 + idx)
        c.execute("INSERT OR IGNORE INTO pipeline_dependencies VALUES(?,?)", (f"merge_{e}", f"bronze_{e}"))
        c.execute("INSERT OR IGNORE INTO pipeline_dependencies VALUES(?,?)", (f"silver_{e}", f"merge_{e}"))
        c.execute("INSERT OR IGNORE INTO pipeline_dependencies VALUES(?,?)", (f"silver_{e}", f"bronze_{e}"))
        for d in SILVER_DEPS.get(e, []):
            c.execute("INSERT OR IGNORE INTO pipeline_dependencies VALUES(?,?)", (f"silver_{e}", f"silver_{d}"))
    for n, (g, (deps, strat)) in enumerate(GOLD.items()):
        _seed(c, f"gold_{g}", ",".join(deps), g, "silver", "gold", strat, None, f"banklake.gold.run_{g}", None, 40 + n)
        for d in deps:
            c.execute("INSERT OR IGNORE INTO pipeline_dependencies VALUES(?,?)", (f"gold_{g}", f"silver_{d}"))
    for sid, sd in SOURCES.items():
        c.execute("INSERT OR IGNORE INTO source_systems VALUES(?,?,?,?)", (sid, sd["name"], sd["system"], sd["db"]))
        for ent, freq in sd["tables"].items():
            c.execute("INSERT OR IGNORE INTO source_tables(source_id,entity,load_frequency) VALUES(?,?,?)", (sid, ent, freq))
    for ent, cols in FLAG_COLUMNS.items():
        for col in cols:
            c.execute("INSERT OR IGNORE INTO bronze_transform_rules VALUES(?,?,?,?)", (ent, col, "TRIM_UPPER", "Y"))
    for ent, col, rule in MASKING + SOURCE_MASKING:
        c.execute("INSERT OR IGNORE INTO masking_metadata VALUES(?,?,?,?)", (ent, col, rule, "Y"))
    for role, ent, col in UNMASK:
        c.execute("INSERT OR IGNORE INTO role_permissions VALUES(?,?,?)", (role, ent, col))
    c.commit()
    c.close()


# ---- watermark helpers ----
def get_watermark(pipeline_id: str) -> int:
    r = query("SELECT last_batch_id FROM pipeline_watermark WHERE pipeline_id=?", (pipeline_id,))
    return int(r[0]["last_batch_id"]) if r else -1


def set_watermark(pipeline_id: str, batch_id: int, run_id: str | None = None) -> None:
    execute("INSERT OR REPLACE INTO pipeline_watermark(pipeline_id,last_batch_id,last_run_id,updated_ts) VALUES(?,?,?,?)",
            (pipeline_id, int(batch_id), run_id, now()))


# ---- audit helpers ----
def log_security(actor: str, action: str, target: str, detail: str = "") -> None:
    execute("INSERT INTO security_audit_log(ts,actor,action,target,detail) VALUES(?,?,?,?,?)",
            (now(), actor, action, target, detail))
