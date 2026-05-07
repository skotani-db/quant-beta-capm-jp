# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src='https://bbb-databricks-demo-assets.s3.amazonaws.com/capm_dash.png' style="float: center" width="1250px"  />

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマの設定
# Unity Catalog の カタログ名・スキーマ名をウィジェットで設定します。
# 実行前に適切な値に変更してください。
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema_dlt_output", "capm_dlt_output", "DLT Output Schema")

catalog           = dbutils.widgets.get("catalog")
schema_dlt_output = dbutils.widgets.get("schema_dlt_output")

# COMMAND ----------

version_before_last = spark.sql(f"describe history {catalog}.{schema_dlt_output}.capm_gold limit 2").select("version").collect()[1][0]
spark.sql(f"select * from {catalog}.{schema_dlt_output}.capm_gold version as of {version_before_last}").display()

# COMMAND ----------
