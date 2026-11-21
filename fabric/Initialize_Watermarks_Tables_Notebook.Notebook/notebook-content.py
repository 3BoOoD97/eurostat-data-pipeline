# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a904ab17-bc40-4b65-8d6f-bccceb88dcd3",
# META       "default_lakehouse_name": "Eurostat_Asylum_Lakehouse",
# META       "default_lakehouse_workspace_id": "4b2e584f-8b24-4a80-9eaa-92484d08b3a7",
# META       "known_lakehouses": [
# META         {
# META           "id": "a904ab17-bc40-4b65-8d6f-bccceb88dcd3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark.sql("CREATE SCHEMA IF NOT EXISTS control")

schema = StructType([
    StructField("dataset_name", StringType(), False),
    StructField("last_imported_source_date", TimestampType(), True)
])

initial_data = [
    ("migr_asyappctzm", None),
    ("migr_asypenctzm", None),
    ("migr_asydcfstq", None)
]

df_watermarks = spark.createDataFrame(initial_data, schema)
df_watermarks.write.format("delta").saveAsTable("control.fabric_watermarks")



display(spark.table("control.fabric_watermarks"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
