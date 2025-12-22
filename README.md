def _parse_structured_context(self, raw_text: str):
    """
    Parse RAG raw structured output into clean key/value dictionaries.
    Raw text format example:
        "table=event_store\\market=AMH_OB\\data_namespace=AMH_FZ_FDR_DEV_SIT\\column=customer_id"
    """

    if not raw_text or not isinstance(raw_text, str):
        return {}

    # Normal container
    parsed = {}

    # Split by backslash
    parts = raw_text.split("\\")
    # Example parts:
    # ['table=event_store', 'market=AMH_OB', 'data_namespace=AMH_FZ_FDR_DEV_SIT', 'column=customer_id']

    for p in parts:
        if "=" not in p:
            continue

        k, v = p.split("=", 1)
        k = k.strip().lower()
        v = v.strip()

        # only store meaningful values
        if v and v.lower() not in ("none", "null", ""):
            parsed[k] = v

    ##################################################################
    # Build table context
    ##################################################################
    result = {
        "context_type": None,
        "table": None,
        "column": None,
        "market": None,
        "data_namespace": None,
        "score": None,
        "description": None,
    }

    # TABLE
    if "table" in parsed:
        result["context_type"] = "table"
        result["table"] = parsed.get("table")

    # COLUMN
    if "column" in parsed:
        result["context_type"] = "column"
        result["column"] = parsed.get("column")

    # OPTIONAL FIELDS
    if "market" in parsed:
        result["market"] = parsed.get("market")

    if "data_namespace" in parsed:
        result["data_namespace"] = parsed.get("data_namespace")

    # SCORE SUPPORT
    if "score" in parsed:
        try:
            result["score"] = float(parsed.get("score"))
        except:
            pass

    return result
