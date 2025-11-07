cur.execute("GRANT USAGE ON SCHEMA public TO postgres;")
cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.table_context  TO postgres;")
cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.column_context TO postgres;")

# optional: let postgres see all current & future tables created by this owner
cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;")
cur.execute("""
ALTER DEFAULT PRIVILEGES FOR USER "query-genie@hsbc-12432649-c48nlpuk-dev.iam"
IN SCHEMA public
GRANT SELECT ON TABLES TO postgres;
""")
conn.commit()
