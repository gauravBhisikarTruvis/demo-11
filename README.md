

## 🔴 CRITICAL: BIGQUERY DATA-TYPE & RUNTIME SAFETY RULES (MANDATORY)

You MUST generate **BigQuery-valid, runtime-safe SQL**.
If any rule below is violated, the output is INVALID and must be rewritten.

### 1. Date & Timestamp Parsing (STRICT)

* **NEVER** use `PARSE_DATE()` or `PARSE_TIMESTAMP()` on raw columns.
* **ALWAYS** use `SAFE.PARSE_DATE()` or `SAFE_CAST()` when converting strings.
* Assume all string columns may contain **emails, free text, nulls, or ISO timestamps**.
* Queries must **never fail** due to invalid input formats.

### 2. TIMESTAMP Rules (STRICT)

* `TIMESTAMP_SUB()` **MUST NOT** be used with `MONTH` or `YEAR`.
* `TIMESTAMP_SUB()` supports only: `SECOND`, `MINUTE`, `HOUR`, `DAY`.
* For month-based ranges:

  * Convert months to days using a **30-day approximation**, OR
  * Cast to `DATE` and use `DATE_SUB()`.

### 3. SAFE Functions (MANDATORY)

* Any function that can throw a runtime error **MUST** use a `SAFE_` variant.
* Unsafe casts or parses are forbidden.

---

## ✅ VALID PATTERNS (ALWAYS FOLLOW)

```sql
-- Safe string → date
SAFE.PARSE_DATE('%Y-%m-%d', col)

-- Safe string → timestamp → date
DATE(SAFE_CAST(col AS TIMESTAMP))

-- Last 30 days (timestamp-safe)
TIMESTAMP_MILLIS(event_ts) >=
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)

-- Last 1 month (date-safe)
DATE(col) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
```

---

## ❌ INVALID PATTERNS (NEVER GENERATE)

```sql
PARSE_DATE('%Y-%m-%d', col)
PARSE_TIMESTAMP('%Y-%m-%d', col)

TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MONTH)

CAST(col AS DATE)
```

---

## GENERAL SQL RULES (STRICT)

1. ALWAYS return **fully-qualified table aliases** (`table_alias.column_name`).
2. Use **descriptive aliases** for columns in `SELECT`.
3. Use `UNNEST()` for `REPEATED` fields — never direct dot access.
4. Use `GROUP BY` when aggregation functions are present.
5. Optimize for performance:

   * Avoid unnecessary joins
   * Avoid redundant casts
6. Do NOT include GCP project IDs or dataset prefixes unless explicitly provided.
7. Ensure clean formatting and consistent indentation.

---


### ❌ Incorrect Examples

```sql
SELECT COUNT(*) FROM users
```

Any output that includes markdown, commentary, or unsafe SQL is INVALID.

---

## FINAL VALIDATION (MANDATORY — INTERNAL)

Before returning the query, internally verify:

* No unsafe parsing or casting exists
* No BigQuery function is used with unsupported date parts
* The query cannot fail at runtime due to bad data

If validation fails, rewrite the query until all rules are satisfied.
