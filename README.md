"""
BigQuery SQL Optimizer using LangGraph
Requires:
- src.utils.sa_helper
- src.utils.sql_parser
- src.services.bq_metadata_extractor
- src.llm.gemini
- GoogleCloudSqlUtility
"""

# imports
import json
import logging
from typing import Any, Dict, List
from langgraph.graph import StateGraph, END
from google.cloud import bigquery

from src.utils.sa_helper import get_impersonate_sa_connection, get_bigquery_client
from src.utils.sql_parser import extract_table_columns_from_query
from src.database.db_config import GoogleCloudSqlUtility
from src.llm.gemini import GeminiCode

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# cost util

def bytes_to_usd(bytes_count: int) -> float:
    return float(bytes_count) / 1_000_000_000_000.0 * 5.0

# nodes

def node_query_intake(state):
    if "candidates" not in state:
        state["candidates"] = []
    state["candidates"].append({
        "project": state.get("config", {}).get("project", "unknown"),
        "sql": state.get("original_query", "").strip(),
        "source": "original",
        "cost_usd": None,
        "bytes_processed": None,
        "is_valid": True,
        "semantic_score": 1.0,
        "confidence_score": 1.0,
        "reasoning": "original"
    })
    return state


def node_context_enrichment(state):
    sql = state.get("original_query", "")
    try:
        tables_cols = extract_table_columns_from_query(sql)
        state["tables_involved"] = list(tables_cols.keys())
        cols = []
        for t, cl in tables_cols.items():
            for c in cl:
                cols.append(f"{t}.{c}")
        state["column_involved"] = cols

        results = []
        if state["tables_involved"]:
            db = GoogleCloudSqlUtility(state.get("config", {}).get("project"))
            conn = db.get_db_connection()
            cur = conn.cursor()
            for t in state["tables_involved"]:
                q = f"SELECT metadata FROM bq_metadata WHERE table_name = '{t}'"
                try:
                    cur.execute(q)
                    rows = cur.fetchall()
                    for r in rows:
                        v = r[0]
                        try:
                            if isinstance(v, str):
                                v = json.loads(v)
                        except Exception:
                            pass
                        results.append({"table": t, "metadata": v})
                except Exception:
                    pass
            cur.close()
            conn.close()
        state["table_metadata"] = {r["table"]: r["metadata"] for r in results}
    except Exception:
        state["tables_involved"] = []
        state["column_involved"] = []
        state["table_metadata"] = {}
    return state


def node_static_analysis(state):
    q = state.get("original_query", "").upper()
    flags = []
    if "SELECT *" in q:
        flags.append("SELECT_STAR")
    if "CROSS JOIN" in q:
        flags.append("CROSS_JOIN")
    state["analysis_results"] = {"flags": flags}
    return state


def node_rule_based_rewrite(state):
    orig = state.get("original_query", "").strip()
    rewritten = orig
    if "SELECT *" in orig.upper() and state.get("table_metadata") and len(state["table_metadata"]) == 1:
        table = list(state["table_metadata"].keys())[0]
        schema = state["table_metadata"].get(table, {}).get("schema", [])
        cols = [c["name"] for c in schema] if isinstance(schema, list) else []
        if cols:
            rewritten = orig.replace("SELECT *", f"SELECT {', '.join(cols)}")
    if rewritten != orig:
        state["candidates"].append({
            "project": state.get("config", {}).get("project", "unknown"),
            "sql": rewritten,
            "source": "rule_based",
            "cost_usd": None,
            "bytes_processed": None,
            "is_valid": False,
            "semantic_score": 0.0,
            "confidence_score": 0.0,
            "reasoning": "rule_based"
        })
    return state


def node_dry_run_baseline(state):
    original = state.get("original_query", "")
    project = state.get("config", {}).get("project")
    market = state.get("config", {}).get("market")
    try:
        imp = get_impersonate_sa_connection(project, market)
        client = get_bigquery_client(project, imp)
        jc = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(original, job_config=jc, project=project)
        b = int(job.total_bytes_processed or 0)
        state["original_cost_bytes"] = b
        state["original_cost_usd"] = bytes_to_usd(b)
    except Exception:
        state["original_cost_bytes"] = None
        state["original_cost_usd"] = None
    return state


def node_ai_rewrite(state):
    orig = state.get("original_query", "").strip()
    if not orig:
        return state
    ctx = json.dumps(state.get("table_metadata", {}), indent=2)
    an = json.dumps(state.get("analysis_results", {}), indent=2)
    prompt = f"""
Original Query:
{orig}
Schema:
{ctx}
Flags:
{an}
Rewrite to reduce bytes, preserve semantics.
Return SQL only.
"""
    try:
        gem = GeminiCode(prompt)
        out = gem.optimize(prompt)
        if isinstance(out, dict):
            sql = out.get("sql") or out.get("optimized_sql") or json.dumps(out)
        else:
            raw = str(out).strip().replace("```sql", "").replace("```", "").strip()
            try:
                j = json.loads(raw)
                sql = j.get("sql") or j.get("optimized_sql") or raw
            except Exception:
                sql = raw
        if sql and sql != orig:
            state["candidates"].append({
                "project": state.get("config", {}).get("project", "unknown"),
                "sql": sql,
                "source": "ai_rewrite",
                "cost_usd": None,
                "bytes_processed": None,
                "is_valid": False,
                "semantic_score": 0.0,
                "confidence_score": 0.0,
                "reasoning": "ai"
            })
    except Exception:
        pass
    return state


def node_dry_run_candidates(state):
    project = state.get("config", {}).get("project")
    market = state.get("config", {}).get("market")
    try:
        imp = get_impersonate_sa_connection(project, market)
        client = get_bigquery_client(project, imp)
    except Exception:
        client = None
    for c in state.get("candidates", []):
        sql = c.get("sql")
        if not sql:
            continue
        try:
            if client:
                jc = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
                job = client.query(sql, job_config=jc, project=project)
                b = int(job.total_bytes_processed or 0)
            else:
                b = 0
            c["bytes_processed"] = b
            c["cost_usd"] = bytes_to_usd(b)
        except Exception:
            c["bytes_processed"] = None
            c["cost_usd"] = None
    return state


def node_semantic_validation_ai(state):
    orig = state.get("original_query", "").strip()
    for c in state.get("candidates", []):
        sql = c.get("sql", "").strip()
        if sql == orig:
            c["is_valid"] = True
            c["semantic_score"] = 1.0
            c["confidence_score"] = 1.0
            continue
        prompt = f"""
Original:
{orig}
Candidate:
{sql}
Return JSON: {{"is_valid":true/false, "semantic_score":0-1, "reason":""}}
"""
        try:
            gem = GeminiCode(prompt)
            out = gem.optimize(prompt)
            if isinstance(out, dict):
                r = out
            else:
                raw = str(out).strip().replace("```json", "").replace("```", "").strip()
                r = json.loads(raw)
            c["is_valid"] = bool(r.get("is_valid", False))
            c["semantic_score"] = float(r.get("semantic_score", 0.0))
            c["confidence_score"] = float(r.get("semantic_score", 0.0))
            c["reasoning"] = r.get("reason", "")
        except Exception:
            c["is_valid"] = False
            c["semantic_score"] = 0.0
            c["confidence_score"] = 0.0
    return state


def node_scoring(state):
    ob = state.get("original_cost_bytes")
    best = None
    best_score = -1
    for c in state.get("candidates", []):
        sem = c.get("semantic_score", 0.0)
        b = c.get("bytes_processed")
        if b is None or ob in (None, 0):
            sf = 0.0
        else:
            sf = max(0.0, (ob - b) / max(1, ob))
        score = sem * sf
        c["confidence_score"] = score
        if c.get("is_valid") and score > best_score:
            best_score = score
            best = c
    state["best_candidate"] = best
    return state


def node_finalize(state):
    best = state.get("best_candidate")
    if best:
        state["final_output"] = {
            "optimized_query": best.get("sql"),
            "estimated_bytes": best.get("bytes_processed"),
            "estimated_cost_usd": best.get("cost_usd"),
            "confidence": best.get("confidence_score"),
            "reasoning": best.get("reasoning", "") or "Selected best candidate"
        }
    else:
        state["final_output"] = {
            "optimized_query": state.get("original_query"),
            "estimated_bytes": state.get("original_cost_bytes"),
            "estimated_cost_usd": state.get("original_cost_usd"),
            "confidence": 0.0,
            "reasoning": "No valid optimization candidates found; returning original SQL"
        }
    return state


# graph and runner

def build_optimizer_graph():
    g = StateGraph()
    g.add_node(node_query_intake)
    g.add_node(node_context_enrichment)
    g.add_node(node_static_analysis)
    g.add_node(node_rule_based_rewrite)
    g.add_node(node_dry_run_baseline)
    g.add_node(node_ai_rewrite)
    g.add_node(node_dry_run_candidates)
    g.add_node(node_semantic_validation_ai)
    g.add_node(node_scoring)
    g.add_node(node_finalize)

    g.add_edge(node_query_intake, node_context_enrichment)
    g.add_edge(node_context_enrichment, node_static_analysis)
    g.add_edge(node_static_analysis, node_rule_based_rewrite)
    g.add_edge(node_rule_based_rewrite, node_dry_run_baseline)
    g.add_edge(node_dry_run_baseline, node_ai_rewrite)
    g.add_edge(node_ai_rewrite, node_dry_run_candidates)
    g.add_edge(node_dry_run_candidates, node_semantic_validation_ai)
    g.add_edge(node_semantic_validation_ai, node_scoring)
    g.add_edge(node_scoring, node_finalize)
    g.add_edge(node_finalize, END)
    return g


def run_optimizer(sql: str, project: str, market: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "original_query": sql.strip(),
        "config": {"project": project, "market": market},
        "tables_involved": [],
        "column_involved": [],
        "table_metadata": {},
        "analysis_results": {},
        "candidates": [],
        "best_candidate": None,
    }
    graph = build_optimizer_graph()
    result = graph.run(state)
    return result.get("final_output", {})


# public API

def get_optimized_query(sql: str, project: str, market: str) -> Dict[str, Any]:
    result = run_optimizer(sql, project, market)
    q = result.get("optimized_query")
    r = result.get("reasoning", "")
    if q.strip() == sql.strip():
        result["note"] = "No improvement possible; original query returned"
    else:
        result["note"] = "Optimization applied successfully"
    return result
    best = state.get("best_candidate")
    if best:

