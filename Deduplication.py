# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employee_bronze (id INT, salary INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_bronze values(10,103);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_silver (id INT, salary INT);

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("employee_bronze")
                   .dropDuplicates(["id"]))

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("emp_microbatch")
    
    sql_query = """
      MERGE INTO employee_silver a
      USING emp_microbatch b
      ON a.id=b.id 
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)


# COMMAND ----------

    # query = (deduped_df
    #          .writeStream
    #          .format("delta")
    #          .option("checkpointLocation", "dbfs:/mnt/bronze/employee")
    #          .toTable("employee_silver")
    #          )
    # query.awaitTermination()


# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/bronze/employee_modified")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from employee_silver;
