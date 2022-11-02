# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.help()


# COMMAND ----------

dbutils.widgets.text("p_data_source","")


# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")


# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

v_file_date


# COMMAND ----------

v_data_source

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine",True).json(f"dbfs:{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")