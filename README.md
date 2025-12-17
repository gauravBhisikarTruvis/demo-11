qualified_columns = [
    f"{table}.{col}" 
    for table, columns in tables_column_mapping.items() 
    for col in columns
]

print(qualified_columns)
