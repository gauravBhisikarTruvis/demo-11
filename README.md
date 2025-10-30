class ColumnConfigUpdate(BaseModel):
    # Required keys to identify the specific column record to update (used in the WHERE clause)
    data_source_id: str = Field(..., description="The ID of the data source.")
    table_name: str = Field(..., description="The name of the table to which the column belongs.")
    column_name: str = Field(..., description="The specific name of the column to update.")

    # Optional fields for actual update (used in the SET clause)
    data_namespace: Optional[str] = None
    description: Optional[str] = None
    data_type: Optional[str] = None
    is_filterable: Optional[bool] = None
    is_aggregatable: Optional[bool] = None
    sample_values: Optional[List[Any]] = None
    # Assuming sample_values is a list that should be stored as JSONB/TEXT

# Schema for updating a single table's configuration
class TableConfigUpdate(BaseModel):
    # Required keys to identify the specific table record to update (used in the WHERE clause)
    data_source_id: str = Field(..., description="The ID of the data source.")
    table_name: str = Field(..., description="The specific name of the table to update.")

    # Optional fields for actual update (used in the SET clause)
    display_name: Optional[str] = None
    data_namespace: Optional[str] = None
    description: Optional[str] = None
    filter_columns: Optional[List[str]] = None
    aggregate_columns: Optional[List[str]] = None
    sort_columns: Optional[List[str]] = None
    key_columns: Optional[List[str]] = None
    join_tables: Optional[List[str]] = None
    related_business_terms: Optional[List[str]] = None
    sample_usage: Optional[str] = None
    tags: Optional[List[str]] = None
    # created_at/by and updated_at/by should typically be handled by the database or utility,
    # but 'updated_by' can be explicitly set if passed.
    updated_by: Optional[str] = None
