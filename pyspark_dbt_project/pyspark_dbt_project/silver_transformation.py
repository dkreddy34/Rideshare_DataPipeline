# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Customers**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


# COMMAND ----------

import os
import sys

# COMMAND ----------

current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

df_cust = spark.read.table("pysparkdbt.bronze.customers")

# COMMAND ----------

df_cust = df_cust.withColumn("domain", split(col('email'), '@')[1])

# COMMAND ----------

df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

# COMMAND ----------

df_cust = df_cust.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_cust = df_cust.drop('first_name', 'last_name')

# COMMAND ----------

import importlib
from utils import custom_utils
importlib.reload(custom_utils)
from utils.custom_utils import transformations

# COMMAND ----------

cust_obj = transformations()

cust_df_trns = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')

# COMMAND ----------

df_cust = cust_obj.process_timestamp(cust_df_trns)

# COMMAND ----------


if not spark.catalog.tableExists("pysparkdbt.silver.customers"):

    df_cust.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.customers")

else:

    cust_obj.upsert(spark, df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Drivers**

# COMMAND ----------

df_driver = spark.read.table("pysparkdbt.bronze.drivers")

# COMMAND ----------

df_driver = df_driver.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

# COMMAND ----------

df_driver = df_driver.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_driver = df_driver.drop('first_name', 'last_name')

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_driver, ['driver_id'], 'last_updated_timestamp')

# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):

    df_driver.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.drivers")
else:

    driver_obj.upsert(spark, df_driver, ['driver_id'], 'drivers', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Location**

# COMMAND ----------

df_loc = spark.read.table("pysparkdbt.bronze.locations")

# COMMAND ----------

loc_obj =  transformations()

# COMMAND ----------

df_loc = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(df_loc)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark.silver.locations"):

    df_loc.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_obj.upsert(df_loc, ['location_id'], 'locations', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Payments**

# COMMAND ----------

df_pay = spark.read.table("pysparkdbt.bronze.payments")

# COMMAND ----------

df_pay = df_pay.withColumn("online_payment_status",
            when( (col('payment_method') == 'Card') & ((col('payment_status') == 'Success')), "online-success" )
            .when( (col('payment_method') == 'Card') & ((col('payment_status') == 'Failed')), "online-failed" )
            .when( (col('payment_method') == 'Card') & ((col('payment_status') == 'Pending')), "online-pending" )
            .otherwise("offline"))
display(df_pay)

# COMMAND ----------

payment_obj = transformations()

# COMMAND ----------

df_pay = payment_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = payment_obj.process_timestamp(df_pay)
if not spark.catalog.tableExists("pysparkdbt.silver.payments"):

    df_pay.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.payments")
else:
    payment_obj.upsert(spark, df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Vehicles**

# COMMAND ----------

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")

# COMMAND ----------

df_veh = df_veh.withColumn("make", upper(col("make")))

# COMMAND ----------

display(df_veh)

# COMMAND ----------

veh_obj = transformations()

# COMMAND ----------

df_veh = veh_obj.dedup(df_veh, ['vehicle_id'], 'last_updated_timestamp')
df_veh = veh_obj.process_timestamp(df_veh)


# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):

    df_veh.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.vehicles")
else:
    veh_obj.upsert(spark, df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')