import psycopg2
 
conn = psycopg2.connect(
    host="instance-0139fb06-cf01-4b02-88cb-f61e19e6fef1.database.cloud.databricks.com",
    dbname="irs_group_lakebase",
    user="irs_lakebase_user",
    password="irslakebase123",
    port=5432,
    sslmode="require"
)
query = f"""
SELECT * from playing_with_lakebase
--SELECT current_user
"""
 
with conn.cursor() as cur:
    cur.execute(query)
    version = cur.fetchmany()
    print(version)
conn.close()
 