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

dbutils.widgets.text("p_file_date","2021-03-21")


# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

v_file_date


# COMMAND ----------

v_data_source

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING,nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read.schema(constructors_schema).json(f"dbfs:{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")