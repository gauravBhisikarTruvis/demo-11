"""
LangGraph BigQuery Optimizer
- Uses existing helpers: get_impersonate_sa_connection, get_bigquery_client,
  GoogleCloudSqlUtility, extract_table_columns_from_query, GeminiCode, Estimate
- Single public API: get_optimized_query(sql, project, market)
"""

import json
import logging
import traceback
from typing import Any, Dict, List, Optional

from langgraph.graph import StateGraph, END

from google.cloud import bigquery

from src.utils.sa_helper import get_impersonate_sa_connection, get_bigquery_client
from src.utils.sql_parser import extract_table_columns_from_query, extract_table_columns_from_query as get_bq_context
from src.database.db_config import GoogleCloudSqlUtility
from src.llm.gemini import GeminiCode
from src.services.estimate import Estimate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def bytes_to_usd(bytes_count: Optional[int]) -> Optional[float]:
    if bytes_count is None:
        return None
    return float(bytes_count) / 1_000_000_000_000.0 * 5.0


# Node: intake
def node_query_intake(state: Dict[str, Any]) -> Dict[str, Any]:
    if "candidates" not in state:
        state["candidates"] = []
    state["candidates"].append({
        "project": state.get("config", {}).get("project", "unknown"),
        "sql": state.get("original_query", ""),
        "source": "original",
        "cost_usd": None,
        "bytes_processed": None,
        "is_valid": True,
        "semantic_score": 1.0,
        "confidence_score": 1.0,
        "reasoning": "original"
    })
    return state


# Node: context enrichment (uses existing extractor / DB util)
def node_context_enrichment(state: Dict[str, Any]) -> Dict[str, Any]:
    sql = state.get("original_query", "")
    try:
        tables_cols = get_bq_context(sql) or {}
        state["tables_involved"] = list(tables_cols.keys())
        cols = []
        for t, c in tables_cols.items():
            for col in c:
                cols.append(f"{t}.{col}")
        state["column_involved"] = cols

        results = []
        if state["tables_involved"]:
            db = GoogleCloudSqlUtility(state.get("config", {}).get("host_project") or state.get("config", {}).get("project"))
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
                    continue
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
        state["table_metadata"] = {r["table"]: r["metadata"] for r in results}
    except Exception as e:
        logger.warning(f"Context enrichment failed: {e}")
        state["tables_involved"] = []
        state["column_involved"] = []
        state["table_metadata"] = {}
    return state


# Node: static analysis
def node_static_analysis(state: Dict[str, Any]) -> Dict[str, Any]:
    q = state.get("original_query", "") or ""
    uq = q.upper()
    flags = []
    if "SELECT *" in uq:
        flags.append("SELECT_STAR")
    if "CROSS JOIN" in uq:
        flags.append("CROSS_JOIN")
    state["analysis_results"] = {"flags": flags}
    return state


# Node: rule-based rewrite
def node_rule_based_rewrite(state: Dict[str, Any]) -> Dict[str, Any]:
    orig = state.get("original_query", "")
    rewritten = orig
    try:
        if "SELECT *" in orig.upper() and state.get("table_metadata") and len(state.get("table_metadata", {})) == 1:
            table = list(state["table_metadata"].keys())[0]
            schema = state["table_metadata"].get(table, {}).get("schema") or []
            cols = [c.get("column_name") if isinstance(c, dict) else c.get("name") for c in schema] if isinstance(schema, list) else []
            cols = [c for c in cols if c]
            if cols:
                rewritten = orig.replace("SELECT *", f"SELECT {', '.join(cols)}")
    except Exception:
        rewritten = orig
    if rewritten and rewritten.strip() != orig.strip():
        state.setdefault("candidates", []).append({
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


# Node: baseline dry run using Estimate
def node_dry_run_baseline(state: Dict[str, Any]) -> Dict[str, Any]:
    orig = state.get("original_query", "")
    project = state.get("config", {}).get("project")
    market = state.get("config", {}).get("market")
    try:
        est = Estimate(orig, project, market)
        cost_info = est.estimate_query_cost()
        state["original_cost_bytes"] = int(cost_info.get("bytes_processed") or 0)
        state["original_cost_usd"] = float(cost_info.get("estimated_cost_usd") or 0.0)
    except Exception as e:
        logger.warning(f"Baseline dry-run failed: {e}")
        state["original_cost_bytes"] = None
        state["original_cost_usd"] = None
    return state


# enterprise prompt template
ENTERPRISE_PROMPT = """
You are an Enterprise-Grade BigQuery SQL Optimization System.
Task: Rewrite the given BigQuery SQL to reduce scanned bytes and runtime while preserving EXACT semantics.
Rules:
- Do not change output columns, GROUP BY semantics, or join correctness.
- Prefer predicate pushdown, projection pruning, partition filters, and removal of redundant scans.
- Output valid Standard SQL compatible with BigQuery.
Return JSON only: {"sql":"...","explanation":"...","confidence":0-100}

Input SQL:
{SQL}

Schema Context:
{SCHEMA}

Flags:
{FLAGS}
"""


# Node: AI rewrite
def node_ai_rewrite(state: Dict[str, Any]) -> Dict[str, Any]:
    orig = state.get("original_query", "")
    if not orig:
        return state
    context_str = json.dumps(state.get("table_metadata", {}), indent=2)
    flags_str = json.dumps(state.get("analysis_results", {}), indent=2)
    prompt = ENTERPRISE_PROMPT.format(SQL=orig, SCHEMA=context_str, FLAGS=flags_str)
    try:
        gem = GeminiCode(prompt)
        out = gem.optimize(prompt)
        ai_sql = None
        explanation = None
        confidence = None
        if isinstance(out, dict):
            ai_sql = out.get("sql") or out.get("optimized_sql")
            explanation = out.get("explanation") or out.get("reason")
            confidence = out.get("confidence")
        else:
            raw = str(out).strip()
            cleaned = raw.replace("```json", "").replace("```", "").strip()
            try:
                j = json.loads(cleaned)
                ai_sql = j.get("sql") or j.get("optimized_sql")
                explanation = j.get("explanation") or j.get("reason")
                confidence = j.get("confidence")
            except Exception:
                ai_sql = cleaned
        if ai_sql and ai_sql.strip() and ai_sql.strip() != orig.strip():
            state.setdefault("candidates", []).append({
                "project": state.get("config", {}).get("project", "unknown"),
                "sql": ai_sql,
                "source": "ai_rewrite",
                "cost_usd": None,
                "bytes_processed": None,
                "is_valid": False,
                "semantic_score": 0.0,
                "confidence_score": 0.0,
                "reasoning": explanation or "ai_rewrite",
            })
    except Exception as e:
        logger.error(f"AI rewrite failed: {e}
{traceback.format_exc()}")
    return state


# Node: dry-run candidates using Estimate
def node_dry_run_candidates(state: Dict[str, Any]) -> Dict[str, Any]:
    project = state.get("config", {}).get("project")
    market = state.get("config", {}).get("market")
    for cand in state.get("candidates", []):
        sql = cand.get("sql")
        if not sql:
            continue
        try:
            est = Estimate(sql, project, market)
            cost_info = est.estimate_query_cost()
            bytes_processed = int(cost_info.get("bytes_processed") or 0)
            cand["bytes_processed"] = bytes_processed
            cand["cost_usd"] = float(cost_info.get("estimated_cost_usd") or 0.0)
        except Exception as e:
            logger.warning(f"Candidate dry-run failed: {e}")
            cand["bytes_processed"] = None
            cand["cost_usd"] = None
    return state


# Node: AI semantic validation
SEMANTIC_PROMPT = """
You are a BigQuery SQL Semantic Validator.
Compare ORIGINAL and CANDIDATE queries and answer whether they produce the same result semantics.
Return JSON only: {"is_valid": true/false, "semantic_score": 0.0-1.0, "reason":"..."}

Original:
{ORIG}

Candidate:
{CAND}
"""


def node_semantic_validation(state: Dict[str, Any]) -> Dict[str, Any]:
    orig = state.get("original_query", "")
    for cand in state.get("candidates", []):
        sql = cand.get("sql", "")
        if not sql:
            cand.update({"is_valid": False, "semantic_score": 0.0, "confidence_score": 0.0, "reasoning": "empty"})
            continue
        if sql.strip() == orig.strip():
            cand.update({"is_valid": True, "semantic_score": 1.0, "confidence_score": 1.0, "reasoning": "identical"})
            continue
        prompt = SEMANTIC_PROMPT.format(ORIG=orig, CAND=sql)
        try:
            gem = GeminiCode(prompt)
            out = gem.optimize(prompt)
            if isinstance(out, dict):
                res = out
            else:
                raw = str(out).strip().replace("```json", "").replace("```", "").strip()
                res = json.loads(raw)
            cand["is_valid"] = bool(res.get("is_valid", False))
            cand["semantic_score"] = float(res.get("semantic_score", 0.0))
            cand["confidence_score"] = float(res.get("semantic_score", 0.0))
            cand["reasoning"] = res.get("reason", "")
        except Exception as e:
            logger.warning(f"Semantic validation error: {e}")
            cand.update({"is_valid": False, "semantic_score": 0.0, "confidence_score": 0.0, "reasoning": f"validation error: {e}"})
    return state


# Node: scoring
def node_scoring(state: Dict[str, Any]) -> Dict[str, Any]:
    ob = state.get("original_cost_bytes") or 0
    best = None
    best_score = -1.0
    for c in state.get("candidates", []):
        sem = c.get("semantic_score", 0.0)
        b = c.get("bytes_processed") if c.get("bytes_processed") is not None else None
        if b is None or ob in (None, 0):
            savings = 0.0
        else:
            savings = max(0.0, (ob - b) / max(1, ob))
        score = sem * savings
        c["confidence_score"] = score
        if c.get("is_valid") and score > best_score:
            best_score = score
            best = c
    state["best_candidate"] = best
    return state


# Node: finalize
def node_finalize(state: Dict[str, Any]) -> Dict[str, Any]:
    best = state.get("best_candidate")
    orig = state.get("original_query", "")
    out: Dict[str, Any] = {}
    out["original_query"] = orig
    out["original_cost_bytes"] = state.get("original_cost_bytes")
    out["original_cost_usd"] = state.get("original_cost_usd")
    out["candidates"] = state.get("candidates", [])
    if best:
        out["optimized_query"] = best.get("sql")
        out["optimized_cost_bytes"] = best.get("bytes_processed")
        out["optimized_cost_usd"] = best.get("cost_usd")
        out["bytes_saved"] = (out.get("original_cost_bytes") or 0) - (out.get("optimized_cost_bytes") or 0)
        out["cost_saved_usd"] = (out.get("original_cost_usd") or 0.0) - (out.get("optimized_cost_usd") or 0.0)
        out["saving_pct"] = (out["bytes_saved"] / max(1, out.get("original_cost_bytes") or 1)) * 100.0
        out["reasoning"] = best.get("reasoning")
        out["confidence"] = best.get("confidence_score")
    else:
        out["optimized_query"] = orig
        out["optimized_cost_bytes"] = out.get("original_cost_bytes")
        out["optimized_cost_usd"] = out.get("original_cost_usd")
        out["bytes_saved"] = 0
        out["cost_saved_usd"] = 0.0
        out["saving_pct"] = 0.0
        out["reasoning"] = "No valid optimization candidates found"
        out["confidence"] = 0.0
    state["final_output"] = out
    return state


# Build graph
def build_optimizer_graph() -> StateGraph:
    g = StateGraph()
    g.add_node(node_query_intake)
    g.add_node(node_context_enrichment)
    g.add_node(node_static_analysis)
    g.add_node(node_rule_based_rewrite)
    g.add_node(node_dry_run_baseline)
    g.add_node(node_ai_rewrite)
    g.add_node(node_dry_run_candidates)
    g.add_node(node_semantic_validation)
    g.add_node(node_scoring)
    g.add_node(node_finalize)
    g.add_edge(node_query_intake, node_context_enrichment)
    g.add_edge(node_context_enrichment, node_static_analysis)
    g.add_edge(node_static_analysis, node_rule_based_rewrite)
    g.add_edge(node_rule_based_rewrite, node_dry_run_baseline)
    g.add_edge(node_dry_run_baseline, node_ai_rewrite)
    g.add_edge(node_ai_rewrite, node_dry_run_candidates)
    g.add_edge(node_dry_run_candidates, node_semantic_validation)
    g.add_edge(node_semantic_validation, node_scoring)
    g.add_edge(node_scoring, node_finalize)
    g.add_edge(node_finalize, END)
    return g


# Public API

def get_optimized_query(sql: str, project: str, market: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "original_query": sql.strip(),
        "config": {"project": project, "market": market, "host_project": project},
        "tables_involved": [],
        "column_involved": [],
        "table_metadata": {},
        "analysis_results": {},
        "candidates": [],
        "best_candidate": None,
    }
    graph = build_optimizer_graph()
    try:
        result = graph.run(state)
        out = result.get("final_output", {})
        # enrich output with meta
        out["static_flags"] = state.get("analysis_results", {}).get("flags", [])
        out["table_metadata_summary"] = {k: (v if isinstance(v, dict) else {}) for k, v in state.get("table_metadata", {}).items()}
        return {"success": True, "result": out}
    except Exception as e:
        logger.error(f"Optimizer run failed: {e}
{traceback.format_exc()}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    sample_sql = "SELECT * FROM `project.dataset.table` WHERE event_date >= '2024-01-01'"
    res = get_optimized_query(sample_sql, project="hsbc-123", market="default")
    print(json.dumps(res, indent=2))
