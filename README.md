def node_semantic_validation_ai(state: OptimizerState) -> OptimizerState:
    """
    Node: AI Semantic Validation

    Uses YOUR GeminiCode(prompt).optimize(prompt) method to determine
    whether SQL rewrite candidates preserve semantic meaning.

    Produces & updates:
        project, is_valid, semantic_score, confidence_score, reasoning
    """

    logger.info("--- Step 9: Semantic Validation (AI) ---")

    original_sql = state.get("original_query", "").strip()
    candidates   = state.get("candidates", [])
    project      = state.get("config", {}).get("project", "unknown")

    if not original_sql:
        logger.warning("Original SQL missing → cannot validate semantics.")
        return state

    if not candidates:
        logger.info("No rewrite candidates → skipping semantic validation.")
        return state

    for cand in candidates:

        rewritten = cand.get("sql", "").strip()

        # Always update project:
        cand["project"] = project

        # Reject empty SQL
        if not rewritten:
            cand.update({
                "is_valid": False,
                "semantic_score": 0.0,
                "confidence_score": 0.0,
                "reasoning": "Candidate SQL empty."
            })
            continue

        # Auto pass identical SQL
        if rewritten == original_sql:
            cand.update({
                "is_valid": True,
                "semantic_score": 1.0,
                "confidence_score": 1.0,
                "reasoning": "SQL identical to original → automatically correct."
            })
            continue

        ###############################################################
        # Build the validation prompt for GeminiCode
        ###############################################################
        prompt = f"""
You are a BigQuery SQL Semantic Validator.

Determine if rewritten SQL produces the same final meaning
as the original SQL.

Original SQL:
```sql
{original_sql}



Generated SQL:
{rewritten}

Return STRICT JSON ONLY:

{{
"is_valid": true/false,
"semantic_score": number between 0 and 1,
"reason": string
}}
"""


try:
        ###########################################################
        # Run YOUR Gemini implementation
        ###########################################################
        gemini = GeminiCode(prompt)
        out    = gemini.optimize(prompt)

        ###########################################################
        # Normalize returned output from Gemini
        ###########################################################
        if isinstance(out, dict):
            structured = out
        else:
            raw = str(out).strip()
            cleaned = (
                raw.replace("```json", "")
                   .replace("```", "")
                   .strip()
            )
            structured = json.loads(cleaned)

        ###########################################################
        # Update candidate object to your typed structure
        ###########################################################
        cand["is_valid"]         = bool(structured.get("is_valid", False))
        cand["semantic_score"]   = float(structured.get("semantic_score", 0.0))
        cand["confidence_score"] = float(structured.get("semantic_score", 0.0))
        cand["reasoning"]        = structured.get("reason", "")

    except Exception as e:
        ###########################################################
        # Safe fallback
        ###########################################################
        logger.error(f"Semantic validation failed: {e}")
        cand.update({
            "is_valid": False,
            "semantic_score": 0.0,
            "confidence_score": 0.0,
            "reasoning": f"Validation error: {e}"
        })

return state
