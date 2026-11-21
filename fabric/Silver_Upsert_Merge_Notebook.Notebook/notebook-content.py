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

# PARAMETERS CELL ********************

dataset_name = "asyappctzm"
source_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_timestamp


spark.sql("CREATE SCHEMA IF NOT EXISTS silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merge_keys = {
    "asyappctzm": [
        "nationality_code",
        "country_code",
        "time_period",
        "applicant_type"
    ],
    "asydcfstq": [
        "nationality_code",
        "country_code",
        "time_period",
        "decision"
    ],
    "asypenctzm": [
        "nationality_code",
        "country_code",
        "time_period"
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate dataset name
if dataset_name not in merge_keys:
    raise ValueError(f"Unsupported dataset: {dataset_name}")

# Define target Silver table
target_table_name = f"silver.{dataset_name}"

# Get primary keys
keys = merge_keys[dataset_name]

# Build dynamic merge condition
merge_condition = " AND ".join(
    [
        f"target_table.`{key}` = source_table.`{key}`"
        for key in keys
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the bronze source table
df_bronze_dataset = spark.table(f"bronze.{dataset_name}")

if df_bronze_dataset.limit(1).count() == 0:
    raise ValueError(
        f"Bronze table {dataset_name} is empty!"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the Silver table exists
if spark.catalog.tableExists(target_table_name):

    target_table = DeltaTable.forName(
        spark,
        target_table_name
    )

    (
        target_table.alias("target_table")
        .merge(
            df_bronze_dataset.alias("source_table"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    print(
        f"Full snapshot synchronization for "
        f"{dataset_name} completed successfully"
    )

else:
    (
        df_bronze_dataset.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("time_period")
        .saveAsTable(target_table_name)
    )

    print(
        f"Initial load for {dataset_name} completed successfully"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# Validate source date
if source_date is None or not source_date.strip():
    raise ValueError("source_date parameter is missing.")

# Convert Eurostat dataset ID to Watermark dataset name
watermark_dataset_name = f"migr_{dataset_name}"

# Prepare the new Watermark value
watermark_update_df = (
    spark.createDataFrame(
        [(watermark_dataset_name, source_date)],
        ["dataset_name", "source_date"]
    )
    .select(
        col("dataset_name"),
        to_timestamp(col("source_date")).alias(
            "last_imported_source_date"
        )
    )
)

# Ensure the source date is valid
if watermark_update_df.filter(
    col("last_imported_source_date").isNull()
).limit(1).count() > 0:
    raise ValueError(f"Invalid source_date: {source_date}")

# Ensure exactly one Watermark record exists
matching_rows = (
    spark.table("control.fabric_watermarks")
    .filter(col("dataset_name") == watermark_dataset_name)
    .count()
)

if matching_rows != 1:
    raise ValueError(
        f"Expected one Watermark record for "
        f"{watermark_dataset_name}, found {matching_rows}"
    )

# Update Watermark after successful Silver synchronization
watermark_update_df.createOrReplaceTempView(
    "watermark_update"
)

spark.sql("""
    MERGE INTO control.fabric_watermarks AS target
    USING watermark_update AS source
    ON target.dataset_name = source.dataset_name
    WHEN MATCHED THEN UPDATE SET
        target.last_imported_source_date =
            source.last_imported_source_date
""")

print(
    f"Watermark updated successfully for "
    f"{watermark_dataset_name}: {source_date}"
)

# Verify the updated Watermark
display(
    spark.table("control.fabric_watermarks")
    .filter(col("dataset_name") == watermark_dataset_name)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

--
%%sql
UPDATE control.fabric_watermarks
SET last_imported_source_date = TIMESTAMP '2026-10-28 10:00:00.000000'
WHERE dataset_name = 'migr_asydcfstq';


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Eurostat_Asylum_Lakehouse.control.fabric_watermarks LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
