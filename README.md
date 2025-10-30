from fastapi import APIRouter
import psycopg2

@pg_router.post("/get-table-metadata")
def get_table_metadata(payload: Metadata):
    market = payload.market
    try:
        # ... open conn, run query, fetch rows ...
        columns = [desc[0] for desc in cur.description]
        table_data = [dict(zip(columns, row)) for row in rows]
        return {"status_code": 200, "content": {"tables": table_data}}
    except Exception as e:
        logger.error("DATABASE_ERROR - Market: %s, PostgreSQL Error: %s", market, str(e))
        raise HTTPException(status_code=500, detail="Database connection error")




        from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# ...
table_data = [dict(zip(columns, row)) for row in rows]
payload = {"tables": table_data}
return JSONResponse(status_code=200, content=jsonable_encoder(payload))
