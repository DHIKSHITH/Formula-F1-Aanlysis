# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_json", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_json", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_json", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_json", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_folder_csv's", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_multiple_multiLine_json", 0, {"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

