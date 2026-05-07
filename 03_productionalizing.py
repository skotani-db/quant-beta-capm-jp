# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ソリューション全体の本番化
# MAGIC 
# MAGIC 多数のファイルと多くの処理ステップを扱える堅牢なデータパイプラインを構築するために、Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) を使用します。

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/DLT_DAG.png' style="float: center" width="1250px"  />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Delta Live Tables
# MAGIC 
# MAGIC すでに述べたように、Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables)（DLT）は、Delta Lake上で高品質なデータを提供する信頼性の高いデータパイプラインの構築と管理を容易にします。DLTは宣言的パイプライン開発、自動データテスト、監視と回復のための詳細な可視性によってETL開発と管理を簡素化します。
# MAGIC 
# MAGIC DLTがもたらす主な**機能**は以下のとおりです。
# MAGIC - **自動テスト**: DLTは下流ユーザーへの高品質なデータ提供を通じて、正確で有用なBI、データサイエンス、機械学習を実現します。検証と整合性チェックによって不良データがテーブルに流入するのを防ぎ、事前定義されたエラーポリシーによってデータ品質エラーを回避します。上の画像に示すように、パイプラインの最終ステップ（```capm_betas_and_returns```、株式ベータと期待収益率を計算する箇所）では、_全レコード（4,000件強）が正常に処理され、一件もドロップされませんでした_。
# MAGIC - **監視と容易な回復のための詳細な可視性**: パイプライン運用の統計情報とデータリネージを視覚的に追跡するツールで、パイプライン運用に関する深い可視性を得られます。```CAPM - 2. 株式ベータ＋収益率計算```ノートブックで概説した4つのステップすべて、その実行状況、潜在的なエラーを確認できます。
# MAGIC - **簡素化された管理とガバナンス**: DLTのUIにより、開発から本番環境へのデプロイ、権限管理、実行スケジュール、実行管理を含むデータパイプライン全体の管理が非常に容易になります。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. タイムトラベル
# MAGIC 
# MAGIC Deltaは企業がデータレイクに保存するビッグデータを自動的にバージョン管理するため、そのデータの任意の過去バージョンにアクセスできます。この時系列データ管理により、監査、誤った書き込みや削除のデータロールバック、レポートの再現が容易になり、データパイプラインがシンプルになります。

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマの設定
# Unity Catalog の カタログ名・スキーマ名をウィジェットで設定します。
# 実行前に適切な値に変更してください。
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema_stock", "stock_market_historical_data", "Stock Schema")

catalog      = dbutils.widgets.get("catalog")
schema_stock = dbutils.widgets.get("schema_stock")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ${catalog}.${schema_stock}.us_closing_100 VERSION AS OF 0

# COMMAND ----------
