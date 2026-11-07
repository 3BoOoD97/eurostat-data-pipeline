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

from pyspark.sql.functions import col, concat, lit, to_date
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

df_bronze_asyappctzm = spark.table("bronze.asyappctzm")
df_bronze_asydcfstq = spark.table("bronze.asydcfstq")
df_bronze_asypenctzm = spark.table("bronze.asypenctzm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 ### asyappctzm UPSERT


# If condtion to check if the silver table exists or not 
if spark._jsparkSession.catalog().tableExists("silver.asyappctzm"):
        
    # Prepare target & source tables
    source_asyappctzm = df_bronze_asyappctzm
    target_asyappctzm = DeltaTable.forName(spark, "silver.asyappctzm")

    # nationality_code, country_code, time_period, and applicant_type are the PK combination for this table
    target_asyappctzm.alias("target_table").merge(source_asyappctzm.alias("source_table"), "target_table.nationality_code = source_table.nationality_code \
    AND target_table.country_code = source_table.country_code AND target_table.time_period = source_table.time_period \
    AND target_table.applicant_type = source_table.applicant_type ") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Incremental load for asyappctzm completed successfully")
else:
    # If there is not silver table. Then, create a new one 
    df_bronze_asyappctzm.write.format("delta").mode("overwrite").partitionBy("time_period") \
    .saveAsTable("silver.asyappctzm")
    print("Initial load for asyappctzm completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 ### asydcfstq UPSERT


# If condtion to check if the silver table exists or not 
if spark._jsparkSession.catalog().tableExists("silver.asydcfstq"):
        
    # Prepare target & source tables
    source_asydcfstq = df_bronze_asydcfstq
    target_asydcfstq = DeltaTable.forName(spark, "silver.asydcfstq")

    # nationality_code, decision, country_code, and time_period are the PK combination for this table
    target_asydcfstq.alias("target_table").merge(source_asydcfstq.alias("source_table"), "target_table.nationality_code = source_table.nationality_code \
    AND target_table.country_code = source_table.country_code AND target_table.time_period = source_table.time_period \
    AND target_table.decision = source_table.decision ")\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Incremental load for asydcfstq completed successfully")
else:
    # If there is not silver table. Then, create a new one 
    df_bronze_asydcfstq.write.format("delta").mode("overwrite").partitionBy("time_period") \
    .saveAsTable("silver.asydcfstq")
    print("Initial load for asydcfstq completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 ### asypenctzm UPSERT


# If condtion to check if the silver table exists or not 
if spark._jsparkSession.catalog().tableExists("silver.asypenctzm"):
        
    # Prepare target & source tables
    source_asypenctzm = df_bronze_asypenctzm
    target_asypenctzm = DeltaTable.forName(spark, "silver.asypenctzm")

    # nationality_code, country_code, and time_period are the PK combination for this table
    target_asypenctzm.alias("target_table").merge(source_asypenctzm.alias("source_table"), "target_table.nationality_code = source_table.nationality_code \
    AND target_table.country_code = source_table.country_code AND target_table.time_period = source_table.time_period")\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Incremental load for asypenctzm completed successfully")
else:
    # If there is not silver table. Then, create a new one 
    df_bronze_asypenctzm.write.format("delta").mode("overwrite").partitionBy("time_period") \
    .saveAsTable("silver.asypenctzm")
    print("Initial load for asypenctzm completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
