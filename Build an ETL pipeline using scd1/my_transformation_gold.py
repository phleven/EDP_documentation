from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table(name="customers_1", comment="Clean, materialized customers")

dp.create_auto_cdc_flow(
  target="customers_1",  # The customer table being materialized
  source="customers_clean",  # the incoming CDC
  keys=["id"],  # what we'll be using to match the rows to upsert
  sequence_by=col("operation_date"),  # de-duplicate by operation date, getting the most recent value
  ignore_null_updates=False,
  apply_as_deletes=expr("operation = 'DELETE'"),  # DELETE condition
  except_column_list=["operation", "operation_date", "_rescued_data"],
)