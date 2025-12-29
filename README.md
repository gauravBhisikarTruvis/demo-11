    for raw_text, score in table_contexts:
        parsed = self.parse_context_text(raw_text)

        for item in parsed:
            tables.append({
                "project": item.get("project"),
                "dataset": item.get("dataset"),
                "table": item.get("table"),
                "display_name": item.get("display_name"),
                "description": item.get("description"),
                "tags": item.get("tags"),
                "similarity_score": score
            })
