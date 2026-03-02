import time
from pyspark.sql import Row

start_time = time.time()
row_count = input_df.count()

input_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{host}:5432/{dbname}") \
    .option("dbtable", "pg_yellow_taxi_trip_data") \
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
perf_df.write.mode("append").saveAsTable("irs_group_catalog.nyctaxi.yellow_taxi_export_perf")

display(perf_df)