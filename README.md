
./cloud-sql-proxy hsbc-12432649-c48nlpuk-dev:europe-west2:query-genie-dev &


# Connect to the default 'postgres' database
psql "host=127.0.0.1 sslmode=disable user=postgres dbname=postgres"

\dt


\c query-genie-dev
\dt

./cloud-sql-proxy -instances=hsbc-12432649-c48nlpuk-dev:europe-west2:query-genie-dev=tcp:5432 -ip_address_types=PRIVATE &

