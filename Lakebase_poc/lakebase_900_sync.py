import time
import psycopg2
from pyspark.sql import Row
from pyspark.sql import functions as F

host="instance-0139fb06-cf01-4b02-88cb-f61e19e6fef1.database.cloud.databricks.com"

lakebase_capunits = 4 # fixed value from lakebase 
batch_size = 50000
dbname ="irs_group_lakebase"

#create dataframe from a Delta 
input_df = spark.sql("select * from irs_group_catalog.lakebase.xml_data")

start_time = time.time()
row_count = input_df.count()

# Query target table columns to filter DataFrame accordingly
pg_conn = psycopg2.connect(
    host=host, dbname=dbname, user="irs_lakebase_user",
    password="irslakebase123", port=5432, sslmode="require"
)
with pg_conn.cursor() as cur:
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'data_990' ORDER BY ordinal_position")
    target_cols = set(row[0] for row in cur.fetchall())
pg_conn.close()

# Transform DataFrame column names to match PostgreSQL schema
# PostgreSQL table has lowercase columns with single underscores and no special characters
seen = set()
select_exprs = []
for col_name in input_df.columns:
    new_col_name = col_name.lower().replace('__', '_').replace('@', '')
    # Handle specific abbreviated names BEFORE truncation - only for preparerfirmname columns
    if 'preparerfirmgrp_preparerfirmname' in new_col_name:
        new_col_name = new_col_name.replace('businessnameline1txt', 'biznameln1')
        new_col_name = new_col_name.replace('businessnameline2txt', 'biznameln2')
    # Handle truncated column names in PostgreSQL (63 char limit)
    if len(new_col_name) > 63:
        new_col_name = new_col_name[:63]
    # Only include columns that exist in the target table, skip duplicates
    if new_col_name in target_cols and new_col_name not in seen:
        select_exprs.append(F.col(f"`{col_name}`").alias(new_col_name))
        seen.add(new_col_name)

transformed_df = input_df.select(select_exprs)

transformed_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{host}:5432/{dbname}") \
    .option("dbtable", "data_990") \
    .option("user", "irs_lakebase_user") \
    .option("password", "irslakebase123") \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", batch_size) \
    .mode("append") \
    .save()

end_time = time.time()
elapsed = end_time - start_time
rows_per_sec = row_count / elapsed if elapsed > 0 else None

perf_data = [
    Row(
        cluster_workers = worker_node_info_count,
        cpus_per_worker = cpus_per_worker,
        total_worker_cpus = total_worker_cpus,
        lakebase_capunits=lakebase_capunits,
        batch_size=batch_size,
        rows_written=row_count,
        elapsed_time_sec=elapsed,
        rows_per_sec=rows_per_sec
    )
]
perf_df = spark.createDataFrame(perf_data)
perf_df.write.mode("append").saveAsTable("irs_group_catalog.lakebase.data_990")

display(perf_df)