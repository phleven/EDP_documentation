from pyspark.sql.functions import current_timestamp, input_file_name, col

input_dir = "/Volumes/irs_group_catalog/lakebase/files/downloads/2025_TEOS_XML_01A/"
row_tag = "ReturnHeader"

target_table ="irs_group_catalog.lakebase.xml_delta"
# Optional: if you prefer a managed table, omit .option("path", ...) irs_group_catalog.lakebase
# If you prefer an external Delta table, set a location and use .save(...) or .option("path", ...)
# target_path = "/Volumes/<catalog>/<schema>/<volume>/delta/xml_delta"

#-----working code but not flattened
df = (
    spark.read.format("xml")
    .option("rowTag", row_tag)
    .option("inferSchema", "true")   # consider providing an explicit schema for stability
    .load(f"{input_dir}/*.xml")
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingest_time", current_timestamp())
)

# Write to a Delta table in Unity Catalog
(df.write
   .format("delta")
   .mode("append")   # use "overwrite" for full reloads
   .saveAsTable(target_table)
)
