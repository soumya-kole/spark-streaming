# Databricks notebook source
# MAGIC %md
# MAGIC ## Create stream from files

# COMMAND ----------

# dbutils.fs.ls("/mnt/formulaonedlstorage/raw/")

# COMMAND ----------

input_path = "/mnt/formulaonedlstorage/raw/streaming_input/"
checkpoint_path = "/mnt/formulaonedlstorage/raw/checkPointDir/"
output_path = "/mnt/formulaonedlstorage/raw/streaming_output/"


# COMMAND ----------

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1) 
    .load(input_path))


# COMMAND ----------

explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID","CustomerType", "PaymentMethod", "DeliveryType", "explode(InvoiceLineItems) as LineItem")


# COMMAND ----------

from pyspark.sql.functions import expr
flattened_df = (explode_df 
    .withColumn("ItemCode", expr("LineItem.ItemCode")) 
    .withColumn("ItemDescription", expr("LineItem.ItemDescription")) 
    .withColumn("ItemPrice", expr("LineItem.ItemPrice")) 
    .withColumn("ItemQty", expr("LineItem.ItemQty")) 
    .withColumn("TotalValue", expr("LineItem.TotalValue")) 
    .drop("LineItem"))


# COMMAND ----------

    invoiceWriterQuery = (flattened_df.writeStream
        .format("json")
        .queryName("Flattened Invoice Writer")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", "chk-point-dir")
        .trigger(processingTime="1 minute")
        .start())
    
    


# COMMAND ----------

print(invoiceWriterQuery.status)

# COMMAND ----------


