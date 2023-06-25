# Databricks notebook source
deduped_df = (spark
              .readStream
              .format("delta")
            #   .withWatermark("order_timestamp", "30 seconds")
              .table("product_base"))

# COMMAND ----------

def merge_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("product_microbatch")
    sql_query = """
    MERGE INTO product_scd2
        USING (
      select null as product_key, product_microbatch.* from product_microbatch
      union 
      select product_microbatch.product_id as product_key, product_microbatch.* from product_microbatch
      JOIN product_scd2 ON product_microbatch.product_id = product_scd2.product_id
        WHERE product_scd2.is_valid = true) staged_updates
    ON product_scd2.product_id = staged_updates.product_key 
    WHEN MATCHED AND product_scd2.is_valid = true 
         THEN UPDATE SET product_scd2.is_valid = false, product_scd2.valid_to = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT (product_id, price, valid_from, valid_to, is_valid)
          VALUES (staged_updates.product_id, staged_updates.price, current_timestamp(), null, true)
    """
    microBatchDF.sparkSession.sql(sql_query)


# COMMAND ----------



# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(merge_data)
                   .option("checkpointLocation", "dbfs:/mnt/bronze/scd2")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------



# COMMAND ----------


