import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="instance-0139fb06-cf01-4b02-88cb-f61e19e6fef1.database.cloud.databricks.com",
    dbname="irs_group_lakebase",
    user="irs_lakebase_user",
    password="irslakebase123",
    port=5432,
    sslmode="require"
)
query = f"""
select 
    d.returnheader_filer_ein as ein, 
    d.returnheader_filer_businessname_businessnameline1txt as business, 
    d.returnheader_businessofficergrp_personnm as name,
    d. returnheader_returntypecd as return_type,
    d.returnheader_returnts as timestamp,
    z.code as zip,
    z.lat as latatude, 
    z.lon as longituide
from 
    data_zipcodes z, data_990 d   
where 
    z.code = '92501'
and
   d.returnheader_preparerfirmgrp_preparerusaddress_zipcd = z.code
order by
    d.returnheader_filer_ein"""

with conn.cursor() as cur:
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
conn.close()


df = pd.DataFrame(rows, columns=colnames) if rows else pd.DataFrame()
if not df.empty:
    df_spark = spark.createDataFrame(df)
    display(df_spark)
else:
    print("No results returned.")
