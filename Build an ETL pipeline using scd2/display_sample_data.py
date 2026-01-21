catalog = "irs_group_catalog"
schema = dbName = db = "lakeflow-demo-schema"
display(spark.read.json(f"/Volumes/{catalog}/{schema}/raw_data/customers"))