    for p in params:
        if isinstance(p, str):
            p = "'" + p.replace("'", "''") + "'"
        sql = sql.replace("%s", str(p), 1)
    return sql
