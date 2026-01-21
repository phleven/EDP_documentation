from pyspark import pipelines as dp
from pyspark.sql.functions import *

# create the table
dp.create_streaming_table(
    name="customers_history_1", comment="Slowly Changing Dimension Type 1 for customers"
)

# store all changes as SCD2
dp.create_auto_cdc_flow(
    target="customers_history_1",
    source="customers_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="1",
)  # Enable SCD1 and store individual updates