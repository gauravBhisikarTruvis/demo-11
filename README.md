def parse_context_text(self, raw_text: str) -> list[dict]:
    records = []

    blocks = [b.strip() for b in raw_text.split("\n") if b.strip()]

    for block in blocks:
        record = {}

        for line in block.split("|"):
            if "=" in line:
                key, value = line.split("=", 1)
                record[key.strip()] = value.strip()
            elif ":" in line:
                key, value = line.split(":", 1)
                record[key.strip()] = value.strip()

        if record:
            records.append(record)

    return records
