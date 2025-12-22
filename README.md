### HARD SQL FORMAT RULES
1) All tables MUST be returned as fully qualified names.
2) Format = `{DATASET}.{TABLE}`
3) NEVER output unqualified table names under ANY condition.
4) NEVER output market names.
5) If dataset_id is provided, ALL tables MUST use it.
6) Fail the query if rules cannot be satisfied.
