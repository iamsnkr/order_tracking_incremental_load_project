# Databricks notebook source

#Paths to the staging and target directorie

stage_dir = dbutils.widgets.get("staging_s3")  # "/FileStore/Order_Tracking/staging/"
target_dir = dbutils.widgets.get("archive_s3")  # "/FileStore/Order_Tracking/archive/"
staging_table = dbutils.widgets.get("stage_tb_zn")  #"hive_metastore.default.stage_zn"


print(stage_dir, target_dir, staging_table)

# Schema information
schema_info = """order_num int, tracking_num string, pck_recieved_date string, package_deliver_date string, status string, address string, last_update_timestamp string"""


# Read CSV file from dbfs path under staging_zn folder
df = spark.read.schema(schema_info).csv(stage_dir, header=True, inferSchema=False)

df.show(5)
df.printSchema()

# Create Delta table named stage_zn if it doesn't exist and overwrite the data in stage table
df.write.format("delta").mode("overwrite").saveAsTable(staging_table)

# Move each file to the target directory 
files = dbutils.fs.ls(stage_dir)
for file in files:
    # Construct the target path
    src_path = file.path
    
    target_path = target_dir + file.path.split("/")[-1]
   
    dbutils.fs.mv(src_path, target_path)
