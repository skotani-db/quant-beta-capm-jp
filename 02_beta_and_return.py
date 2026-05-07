# Databricks notebook source
import pyspark.pandas as ks
ks.set_option('compute.ops_on_diff_frames', True)
import numpy as np

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import isnan, when, count
from pyspark.sql.types import DateType, StringType
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import dlt

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマの設定
# Unity Catalog の カタログ名・スキーマ名をウィジェットで設定します。
# DLT パイプライン設定の "Configuration" で以下のキーを定義してください。
#   catalog       : 対象カタログ名（例: main）
#   schema_stock  : 株式データのスキーマ名（例: stock_market_historical_data）
#   schema_indices: 指数データのスキーマ名（例: indices_historical_data）
import os
catalog        = spark.conf.get("catalog",        "main")
schema_stock   = spark.conf.get("schema_stock",   "stock_market_historical_data")
schema_indices = spark.conf.get("schema_indices",  "indices_historical_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ステップ1: ブロンズ層 - 生データ
# MAGIC 
# MAGIC 全企業の日次終値データとS&P500指数データを結合します。

# COMMAND ----------

@dlt.table(name="capm_bronze")
def capm_bronze():
  capm_bronze_df = spark.sql(
    f"SELECT * FROM "
    f"(SELECT to_date(Date, 'yyyy-MM-dd') as DateSP500, Close as SP500 FROM {catalog}.{schema_indices}.sp_500) as idxs "
    f"INNER JOIN "
    f"(SELECT * FROM {catalog}.{schema_stock}.us_closing_100) as equities "
    f"ON idxs.DateSP500 = equities.Date;"
  ).drop('DateSP500').drop('Date')
  return capm_bronze_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ステップ2: シルバー層 - 収益率データ
# MAGIC 
# MAGIC 株式と指数の日次収益率を算出します。

# COMMAND ----------

# SP500がnullでないことを確認する制約
silverExpectations = {
    "sp500_not_null": "SP500 IS NOT NULL" 
}

@dlt.table(name="capm_silver")
@dlt.expect_all(silverExpectations)
def capm_gold():
  returns_ks = dlt.read('capm_bronze').to_koalas()
  
  # 各株式の日次収益率の対数を計算する
  returns_ks = np.log(returns_ks / returns_ks.shift(1))
  returns_ks = returns_ks.iloc[1:, :]
  return returns_ks.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ステップ3: ゴールド層 - ベータとCAPMデータ
# MAGIC 
# MAGIC 前のノートブックの数式に従い、ベータ値と期待収益率を計算します。

# COMMAND ----------

@dlt.table(name="capm_gold")
def betas_and_capmreturn():
  
  # 2022年8月4日時点の10年国債利回り
  r_f = 0.0268
  
  # ベンチマークとして使用するS&P500の2021年リターン
  r_m = 0.2689
  
  returns_ks = dlt.read('capm_silver').to_koalas()

  cov_ks = returns_ks.cov() * 250
  
  companies_dct = []
  betas_schema = StructType([ \
    StructField("Company", StringType(), True), \
    StructField("Beta", DoubleType(), True), \
    StructField("Return", DoubleType(), True)
  ])
  
  # S&P500の年率換算分散
  market_var = returns_ks['SP500'].var() * 250
  
  for t in cov_ks.columns[1:]:
    
    # 各企業の収益率とS&P500収益率の共分散
    cov_with_market = cov_ks[t].iloc[0]
    
    # 企業のベータ値
    beta = cov_with_market / market_var
    
    # CAPMの計算式
    capm = r_f + beta * (r_m - r_f)
    companies_dct.append({'Company': t, 'Beta': float(beta), 'Return' : float(capm)})
  return spark.createDataFrame(data = companies_dct, schema = betas_schema)
