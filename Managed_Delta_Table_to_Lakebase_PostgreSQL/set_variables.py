lakebase_capunits = 4 # fixed value from lakebase 
batch_size = 50000
dbname ="irs_group_lakebase"

host="instance-0139fb06-cf01-4b02-88cb-f61e19e6fef1.database.cloud.databricks.com"

#create dataframe from a Delta 
input_df = spark.sql("select * from irs_group_catalog.nyctaxi.yellow_taxi_export")

input_df.count()