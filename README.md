import ast

def normalize_parsed_items(parsed_items: list[str]) -> list[dict]:
    normalized = []

    for item in parsed_items:
        if isinstance(item, dict):
            normalized.append(item)
        elif isinstance(item, str):
            try:
                normalized.append(ast.literal_eval(item))
            except Exception as e:
                logger.error(f"Failed to parse context item: {item}")
        else:
            logger.warning(f"Unknown context item type: {type(item)}")

    return normalized
