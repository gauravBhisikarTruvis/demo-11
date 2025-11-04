import datetime
import json
# Assuming necessary imports like GoogleCloudSqlUtility, sql, logger,
# PostgresLoader, PostgresVectorLoader, TableUpdateRequest, generate_id_key, etc., exist.

# ... (Previous code and imports) ...

@pg_router.post("/update-table-metadata")
def update_table(payload: TableUpdateRequest):
    """
    Handles updating table metadata in the main Postgres table and 
    generates/updates vector embeddings in the vector table for search.
    """
    conn = None
    cur = None
    
    # Assuming user_name_user and user_id_user are retrieved from a secure context
    username = 'unknown' # Replace with actual user context retrieval
    user_id = 'unknown'
    
    # 1. Normalize payload to a list of rows (as seen in image_801f2a.png)
    rows_to_update = payload.obj if isinstance(payload.obj, list) else [payload.obj]
    
    try:
        # Get DB connection and loaders
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()
        cur = conn.cursor()
        
        # Instantiate Loaders (as seen in image_802768.png)
        pg = PostgresLoader(payload.market)
        postgres_vector_loader = PostgresVectorLoader(payload.market)
        
        current_time = datetime.datetime.utcnow()

        for update_data in rows_to_update:
            # Generate the unique ID for the metadata record
            id_key = generate_id_key(
                project_id=project_id, # Assuming project_id is available
                dataset_id=dataset_id, # Assuming dataset_id is available
                payload_table_name=payload.table_name
            )

            # 1a. Prepare Data for Primary Metadata Update
            # (Based on image_802768.png and image_802789.png data prep)
            update_data["id_key"] = id_key
            update_data.setdefault("data_source_id", project_id)
            update_data.setdefault("table_name", payload.table_name)
            update_data.setdefault("display_name", payload.table_name)
            update_data.setdefault("data_namespace", dataset_id)
            update_data['updated_at'] = current_time
            update_data['updated_by'] = username

            # 2. GENERATE AND STORE EMBEDDING (NEW/REQUIRED STEP)
            # This must happen inside the loop for every record being updated.
            # The 'insert_table_context' method (from image_802a6f.png) 
            # is assumed to handle text extraction, embedding generation, 
            # and UPSERT into the vector store.
            try:
                postgres_vector_loader.insert_table_context(update_data, "table_context")
                # Logs/prints will indicate success inside the loader class
            except Exception as e:
                # Log the embedding failure but allow the main metadata update to continue
                logger.error(f"VECTOR EMBEDDING FAILED for ID {id_key}: {e}")

            # 3. PROCESS DATA FOR SQL UPDATE (Based on image_802789.png)
            processed_data = {}
            for key, value in update_data.items():
                if key in ["filter_columns", "aggregate_columns", "sort_columns", "key_columns", "tags", "related_business_terms"]:
                    processed_data[key] = value if value is not None else []
                elif key in ["sample_usage", "join_tables"]:
                    # Dump lists/dicts to JSON strings for Postgres storage
                    processed_data[key] = json.dumps(value) if value is not None else '[]' 
                else:
                    processed_data[key] = value
            
            # 4. EXECUTE SQL UPDATE (SQL logic is complex, simplified structure shown)
            keys = list(processed_data.keys())
            values = [processed_data[k] for k in keys]
            update_fields = [k for k in keys if k != "id_key"]

            # Assuming complex SQL construction logic from image_802789.png runs here:
            query = sql.SQL("""
                INSERT INTO table_config ({fields})
                VALUES ({placeholders})
                ON CONFLICT (id_key) DO UPDATE SET
                ({updates}) = (EXCLUDED.updates)
            """).format(
                fields=sql.Identifier(keys),
                placeholders=sql.Placeholder(len(keys)),
                updates=sql.SQL(",").join(map(sql.Identifier, update_fields))
            )
            
            # Execute the SQL Update on the main metadata table
            cur.execute(query, tuple(values))

            # 5. Handle generic audit (outside the embedding process)
            # old_data = get_table_audit_data(cur, id_key) 
            # handle_generic_audit(...)

        # Commit all database changes at once after the loop
        conn.commit()
        
        logger.info("UPDATE_TABLE completed successfully - User: %s, Market: %s, Table: %s, Rows updated: %d",
                    username, payload.market, payload.table_name, len(rows_to_update))
        return JSONResponse(status_code=200, content={"message": "Row(s) update successful."})

    except Exception as e:
        logger.exception("UPDATE_TABLE ERROR - User: %s, Market: %s, Table: %s, Error: %s",
                         username, payload.market, payload.table_name, str(e))
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
        
    finally:
        if cur: cur.close()
        if conn: conn.close()
