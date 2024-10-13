# Databricks notebook source
from delta.tables import *

# staging and target tables
staging_table = dbutils.widgets.get("stage_tb_zn")  #"hive_metastore.default.stage_zn"
target_table = dbutils.widgets.get("target_tb_zn")  #"hive_metastore.default.target_zn"


# # 1. Read the data from the stage table
stage_df = spark.read.table(staging_table)

print(target_table, staging_table)

if not spark._jsparkSession.catalog().tableExists(target_table):
    stage_df.write.format("delta").mode("overwrite").saveAsTable(target_table) 
else:
    # Perform delta table merge query for upsert based on tracking_num column
    # Merge condition
    merge_condition = "target.tracking_num = source.tracking_num"
    target_tb = DeltaTable.forName(spark, target_table)

    # Delete all the common records
    target_tb.alias("target")\
        .merge(stage_df.alias("source"), merge_condition)\
        .whenMatchedDelete()\
        .execute()
    # Append all the records in staging table to target table as duplicates records are already deleted
    stage_df.write.format("delta").mode("append").saveAsTable(target_table)


# Check the records count after upsert
target_df = spark.sql(f"Select count(*) from {target_table}")

# display(target_df)
print(f"Number of records after upsert operation : {target_df.first()[0]}")

