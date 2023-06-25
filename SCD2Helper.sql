-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/bronze/scd2", True)

-- COMMAND ----------

drop table product_base;
CREATE TABLE IF NOT EXISTS product_base
(product_id STRING, price BIGINT)

-- COMMAND ----------



-- COMMAND ----------

drop table product_scd2;
CREATE TABLE IF NOT EXISTS product_scd2
(product_id STRING, price BIGINT, valid_from Timestamp, valid_to Timestamp, is_valid BOOLEAN);

-- COMMAND ----------

insert into product_base
values(1,101)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

insert into product_base
values(2,102)

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

select * from product_scd2

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

select * from product_base

-- COMMAND ----------



-- COMMAND ----------

update product_base set price=99 where product_id=1 and price=100

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


