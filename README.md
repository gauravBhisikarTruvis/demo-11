-- 1. Grant usage on the schema (Fixes the DDL failed error)
GRANT USAGE ON SCHEMA public TO "query-genie@hsbc-12432649-c48nlpuk-dev.iam";

-- 2. Grant read access to all current tables (Fixes the project_ids/markets error)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "query-genie@hsbc-12432649-c48nlpuk-dev.iam";

-- 3. Ensure future tables also have permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT ON TABLES TO "query-genie@hsbc-12432649-c48nlpuk-dev.iam";
