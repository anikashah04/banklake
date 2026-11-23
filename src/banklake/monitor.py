"""Monitoring: failure classification, pipeline health, data freshness and alerts, all computed from the audit tables."""
from . import meta

FAILURE_REASONS = ("INACTIVE", "UPSTREAM_FAILED", "SCHEMA_MISMATCH", "MISSING_INPUT", "PERMISSION_DENIED", "UNEXPECTED_ERROR")


def classify_failure(status: str, message: str | None) -> str | None:
    """Short, stable category for a non-successful step (the full text stays in error_message)."""
    if status == "SUCCESS" or status == "RUNNING":
        return None
    if status == "SKIPPED":
        return "INACTIVE"
    if status == "BLOCKED":
        return "UPSTREAM_FAILED"
    m = (message or "").lower()
    if "schema mismatch" in m:
        return "SCHEMA_MISMATCH"
    if any(x in m for x in ("filenotfounderror", "no such file", "path does not exist", "path_not_found", "does not exist")):
        return "MISSING_INPUT"
    if "permission" in m or "access denied" in m:
        return "PERMISSION_DENIED"
    return "UNEXPECTED_ERROR"


def latest_per_pipeline() -> list[dict]:
    return meta.query(
        "SELECT l.*, m.target_layer AS m_layer FROM pipeline_execution_log l "
        "LEFT JOIN pipeline_metadata m ON m.pipeline_id = l.pipeline_id "
        "WHERE l.execution_id IN (SELECT max(execution_id) FROM pipeline_execution_log GROUP BY pipeline_id) ORDER BY l.execution_id")


def health_summary() -> dict:
    latest = latest_per_pipeline()
    counts: dict[str, int] = {}
    for r in latest:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    fresh = meta.query(
        "SELECT COALESCE(l.layer, m.target_layer) AS layer, max(l.end_ts) AS last_success, count(DISTINCT l.pipeline_id) AS pipelines "
        "FROM pipeline_execution_log l LEFT JOIN pipeline_metadata m ON m.pipeline_id = l.pipeline_id "
        "WHERE l.status='SUCCESS' GROUP BY 1 ORDER BY 2")
    rejected: dict[str, int] = {}
    for r in latest:
        if r["status"] == "SUCCESS":
            layer = r.get("layer") or r["m_layer"]
            rejected[layer] = rejected.get(layer, 0) + int(r["rows_rejected"] or 0)
    slow = sorted((r for r in latest if r["status"] == "SUCCESS"), key=lambda r: -(r["duration_s"] or 0))[:5]
    return dict(pipelines=len(latest), status_counts=counts, freshness=fresh, rejected_by_layer=rejected,
                slowest=[dict(pipeline_id=r["pipeline_id"], duration_s=r["duration_s"]) for r in slow],
                last_run_id=latest[-1]["run_id"] if latest else None)


def alerts() -> list[dict]:
    """Things a person should look at, most severe first. Every alert names its source table."""
    out = []
    for r in latest_per_pipeline():
        if r["status"] == "FAILED":
            out.append(dict(severity="HIGH", source="pipeline_execution_log", subject=r["pipeline_id"],
                            message=f"{r.get('failure_reason') or 'FAILED'}: {(r['error_message'] or '')[:160]}"))
        elif r["status"] == "BLOCKED":
            out.append(dict(severity="MEDIUM", source="pipeline_execution_log", subject=r["pipeline_id"], message=r["error_message"] or "blocked"))
    rec = meta.query("SELECT run_id FROM reconciliation_results ORDER BY id DESC LIMIT 1")
    if rec:
        bad = meta.query("SELECT entity, stage, check_name FROM reconciliation_results WHERE run_id=? AND status='FAIL'", (rec[0]["run_id"],))
        for b in bad[:20]:
            out.append(dict(severity="HIGH", source="reconciliation_results", subject=f"{b['entity']}/{b['stage']}", message=f"failed check {b['check_name']}"))
    for f in meta.query("SELECT entity, source_file, message FROM ingestion_audit_log WHERE status='FAILED' ORDER BY id DESC LIMIT 20"):
        out.append(dict(severity="HIGH", source="ingestion_audit_log", subject=f"{f['entity']}/{f['source_file']}", message=(f["message"] or "")[:160]))
    for s in meta.query("SELECT action, count(*) n FROM security_audit_log WHERE action IN ('VAULT_AUTH_FAILED','UNMASK_DENIED') GROUP BY action"):
        out.append(dict(severity="HIGH" if s["action"] == "VAULT_AUTH_FAILED" else "LOW", source="security_audit_log",
                        subject=s["action"], message=f"{s['n']} event(s)"))
    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    return sorted(out, key=lambda a: order[a["severity"]])
