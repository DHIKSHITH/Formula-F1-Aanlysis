# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(f"file_date='{v_file_date}'")\
.select("race_year")\
.distinct()\
.collect()

# COMMAND ----------

race_year_list=[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

print(race_year_list)    

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col

driver_standings_df = race_results_df\
.groupBy("race_year","driver_name","driver_nationality","team")\
.agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank,asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

final_df=re_arrange_partition_column(final_df,"race_year")

# COMMAND ----------


overwrite_partition(final_df,'f1_presentation','driver_standings','race_year')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings