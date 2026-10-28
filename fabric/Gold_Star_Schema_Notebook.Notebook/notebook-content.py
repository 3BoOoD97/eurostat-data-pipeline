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

from pyspark.sql.functions import col, trim, substring, ceil, when, concat, lit, to_date
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_asyappctzm= spark.table("silver.asyappctzm")
df_silver_asydcfstq = spark.table("silver.asydcfstq")
df_silver_asypenctzm = spark.table("silver.asypenctzm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_date table with these cols time_period PK, year, quarter, month, period_type

df_asyappctzm_period = df_silver_asyappctzm.select(trim(col("time_period")).alias("time_period"))
df_asypenctzm_period = df_silver_asypenctzm.select(trim(col("time_period")).alias("time_period"))
df_asydcfstq_period = df_silver_asydcfstq.select(trim(col("time_period")).alias("time_period"))

df_all_periods = df_asyappctzm_period.union(df_asypenctzm_period).union(df_asydcfstq_period).distinct()

df_dim_date = (df_all_periods
    .withColumn("year", substring(col("time_period"), 1, 4))
    .withColumn("period_type", when(col("time_period").contains("Q"), lit("Quarterly"))
        .otherwise(lit("Monthly"))
    )
    .withColumn("month", when(col("period_type") == "Monthly", substring(col("time_period"), 6, 2))
        .otherwise(lit(None))
    )
    .withColumn("quarter", when(col("period_type") == "Quarterly", substring(col("time_period"), 6, 2))
        .otherwise(concat(lit("Q"), ceil(col("month").cast("int") / 3).cast("string")))
    ))

display(df_dim_date)
    

display(df_dim_date.select(col("month")).distinct())
display(df_dim_date.select(col("quarter")).distinct())
display(df_dim_date.select(col("period_type")).distinct())

df_dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create gold_fact_asyappctzm table


df_gold_asyappctzm = df_silver_asyappctzm.select(
    "time_period", "country_code", "country_name", "nationality_code", "applicant_type",
    "sex", "age_group", "flag", "applicant_count"
    )

df_gold_asyappctzm.write.format("delta").mode("overwrite").saveAsTable("gold.fact_asyappctzm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create gold_fact_asydcfstq table

df_gold_asydcfstq = df_silver_asydcfstq.select(
    "time_period", "country_code", "country_name", "nationality_code", 
    "decision", "sex", "age_group", "flag", "decision_count"
)

df_gold_asydcfstq.write.format("delta").mode("overwrite").saveAsTable("gold.fact_asydcfstq")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create gold_fact_asypenctzm table

df_gold_asypenctzm = df_silver_asypenctzm.select(
    "time_period", "country_code", "country_name", "nationality_code", 
    "sex", "age_group", "flag", "pending_count"
)

df_gold_asypenctzm.write.format("delta").mode("overwrite").saveAsTable("gold.fact_asypenctzm")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
