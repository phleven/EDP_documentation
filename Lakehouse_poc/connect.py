import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host = "host=instance-0139fb06-cf01-4b02-88cb-f61e19e6fef1.database.cloud.databricks.com",
    port=5432,
    database="databricks_postgres",
    user="phleven@deloitte.com",
    password="",
    sslmode="require"
    )

df = pd.read_sql("SELECT * FROM ", conn)
conn.close()
display.df