def build_optimizer_graph() -> StateGraph:

    g = StateGraph(OptimizerState)

    # register nodes
    g.add_node("query_intake", node_query_intake)
    g.add_node("context_enrichment", node_context_enrichment)
    g.add_node("rule_based", node_rule_based_rewrite)
    g.add_node("dry_run_baseline", node_dry_run_baseline)
    g.add_node("ai_rewrite", node_ai_rewrite)
    g.add_node("dry_run_candidates", node_dry_run_candidates)
    g.add_node("semantic_validation", node_semantic_validation)
    g.add_node("scoring", node_scoring)
    g.add_node("finalize", node_finalize)

    # wire edges
    g.add_edge("query_intake", "context_enrichment")
    g.add_edge("context_enrichment", "rule_based")
    g.add_edge("rule_based", "dry_run_baseline")
    g.add_edge("dry_run_baseline", "ai_rewrite")
    g.add_edge("ai_rewrite", "dry_run_candidates")
    g.add_edge("dry_run_candidates", "semantic_validation")
    g.add_edge("semantic_validation", "scoring")
    g.add_edge("scoring", "finalize")

    # entry + exit
    g.set_entry_point("query_intake")
    g.set_finish_point("finalize")

    return g.compile()


def get_optimized_query(sql: str, project: str, market: str) -> Dict[str, Any]:

    state: Dict[str, Any] = {
        "original_query": sql.strip(),
        "config": {"project": project, "market": market},
        "tables_involved": [],
        "column_involved": [],
        "table_metadata": {},
        "analysis_results": {},
        "candidates": [],
        "best_candidate": None,
        # important missing fields:
        "recursion_count": 0,
        "final_output": {}
    }

    graph = build_optimizer_graph()

    try:
        result = graph.invoke(state)
        out = result["final_output"]
        return {"success": True, "result": out}

    except Exception as e:
        logger.error(f"Optimizer run failed: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}
