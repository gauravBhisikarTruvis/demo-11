def query(self, question: str):

    column_contexts, table_contexts = self.retrieve_similar(question)

    if not column_contexts and not table_contexts:
        return {
            "text_context": [],
            "structured_context": []
        }

    structured_results = []
    text_blocks = []

    # -----------------------------
    # PROCESS COLUMN CONTEXT DATA
    # -----------------------------
    for raw_text, score in column_contexts:

        # raw_text is a dict → contains full table + column metadata
        # DO NOT FLATTEN!
        structured_dict = self.parse_structured_context(raw_text)

        entry = {
            "context_type": "column",
            "score": score,
            "table_name": structured_dict.get("table_name"),
            "column_name": structured_dict.get("column_name"),
            "metadata": {
                "description": structured_dict.get("description"),
                "data_namespace": structured_dict.get("data_namespace"),
                "data_type": structured_dict.get("data_type"),
                "is_filterable": structured_dict.get("is_filterable"),
                "is_aggregatable": structured_dict.get("is_aggregatable"),
                "sample_usage": structured_dict.get("sample_usage"),
                "tags": structured_dict.get("tags"),
                "related_business_terms": structured_dict.get("related_business_terms"),
                "join_tables": structured_dict.get("join_tables"),
                "key_columns": structured_dict.get("key_columns"),
                "sort_columns": structured_dict.get("sort_columns"),
                "aggregate_columns": structured_dict.get("aggregate_columns")
            }
        }

        structured_results.append(entry)

    # -----------------------------
    # PROCESS TABLE CONTEXT DATA
    # -----------------------------
    for raw_text, score in table_contexts:

        structured_dict = self.parse_structured_context(raw_text)

        entry = {
            "context_type": "table",
            "score": score,
            "table_name": structured_dict.get("table_name"),
            "metadata": {
                "display_name": structured_dict.get("display_name"),
                "category": structured_dict.get("category"),
                "tags": structured_dict.get("tags"),
                "columns": structured_dict.get("columns"),
                "filter_columns": structured_dict.get("filter_columns"),
                "aggregate_columns": structured_dict.get("aggregate_columns"),
                "sort_columns": structured_dict.get("sort_columns"),
                "key_columns": structured_dict.get("key_columns"),
                "join_tables": structured_dict.get("join_tables"),
                "related_business_terms": structured_dict.get("related_business_terms"),
                "sample_usage": structured_dict.get("sample_usage")
            }
        }

        structured_results.append(entry)

    # -----------------------------
    # FINAL SORTING
    # -----------------------------
    structured_results = sorted(
        structured_results,
        key=lambda x: x["score"],
        reverse=True
    )

    return {
        "structured_context": structured_results[:self.RESPONSE_LIMIT]
    }
