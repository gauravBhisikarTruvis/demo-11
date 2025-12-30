UPDATE public.table_context
SET
    project_id = COALESCE(%s, project_id),
    updated_at = NOW()
WHERE id_key = %s
RETURNING *;
