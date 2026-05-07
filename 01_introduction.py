# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 株式ベータ計算とCAPM
# MAGIC 
# MAGIC このソリューションでは、米国上場4,000社以上のベータ値（S&P500やNYSEなどの市場全体と比較した株式のボラティリティ、すなわち系統的リスクの指標）を算出し、それを用いてCAPM（資本資産価格モデル）による各社の期待収益率を導出します。
# MAGIC 
# MAGIC 2010年1月1日から2022年7月末（12年以上）の日次データ（終値）を使用します。データは以下の ```Cmd 7``` セルに示されています。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 以下の数式（```数式1```）に示すように、企業のベータ値を導出するには、企業の収益率と市場ベンチマーク（ここではS&P500指数を使用）との共分散、および市場の分散を計算する必要があります。これらはどちらも非常に計算負荷の高い処理です。特に4,000社以上の12年以上のデータを扱う場合はなおさらです。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ```数式1``` 株式ベータの計算式
# MAGIC 
# MAGIC ## \\( \beta{_i} = \frac{\sigma{_i},{_m}}{\sigma{_m}} \\)
# MAGIC 
# MAGIC _各変数の意味_: <br />
# MAGIC - \\( \beta{_i} \\) は企業のベータ値、
# MAGIC - \\( \sigma{_i},{_m} \\) は株式収益率と市場ベンチマークの共分散、
# MAGIC - \\( \sigma{_m} \\) は市場の分散です。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ```数式2``` 資本資産価格モデル（CAPM）
# MAGIC 
# MAGIC ### \\(E(R{_e}) = R{_f} + \beta{_i} * (E(R{_m}) - R{_f}) \\)
# MAGIC 
# MAGIC _各変数の意味_: <br />
# MAGIC - \\(E(R{_e}) \\) は期待投資収益率、
# MAGIC - \\(R{_f} \\) はリスクフリーレート、
# MAGIC - \\(\beta{_i} \\) は株式のベータ値、
# MAGIC - \\(E(R{_m}) - R{_f} \\) はマーケットリスクプレミアムを表します。
# MAGIC 
# MAGIC リスクフリーレートには2022年8月4日時点の10年国債利回り（0.0268）を、市場収益率にはS&P500の2021年リターンを使用します。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 株式ベータ計算とCAPMにDatabricks Lakehouseを使う理由
# MAGIC  
# MAGIC 
# MAGIC 1. **データの量と多様性**: 4,000以上のファイル（米国上場ほぼ全銘柄の12年以上の日次OHLC＋出来高データ）があります。これらのファイルを手作業で管理・最適化するのは煩雑です。クエリ速度を向上させるために、[Delta Lake on Databricks](https://docs.databricks.com/delta/optimizations/file-mgmt.html)はクラウドストレージに格納されたデータのレイアウトを最適化する機能をサポートしています。Delta Lake on Databricksはビンパッキングとz-orderingの2種類のレイアウトアルゴリズムをサポートしています。
# MAGIC 2. **データ品質の確保とスキーマの検出・適用**
# MAGIC    * データファイルの一部には欠損値があります（データベンダーからのインポート時 - 以下の```Cmd 9```を参照）。欠損レコードが含まれないようにする必要があります。
# MAGIC    * 多数のファイルを扱い、外部ベンダーからデータを取り込む場合、ファイルのスキーマの変更（または不一致）によってデータの下流処理が壊れないようにする必要があります。[Delta Lakeのスキーマ適用](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)は書き込み時にスキーマ検証を行います。テーブルへのすべての新規書き込みは、書き込み時にターゲットテーブルのスキーマとの互換性がチェックされます。スキーマに互換性がない場合、Delta Lakeはトランザクション全体をキャンセル（データは書き込まれない）し、不一致をユーザーに通知する例外を発生させます。
# MAGIC 3. **データリネージ**: 多数のファイルと処理ステップを扱う場合、データ処理や欠損ファイルを追跡できる必要があります。ここで[Unity Catalog](https://www.databricks.com/product/unity-catalog)が役立ちます。Unity Catalogの[データリネージ](https://www.databricks.com/blog/2022/06/08/announcing-the-availability-of-data-lineage-with-unity-catalog.html)は実行時リネージを自動化し、すべてのワークロード、列レベルで機能し、ノートブック、スクリプト、サードパーティツール、ダッシュボードをカバーします。
# MAGIC 4. **タイムトラベル**: 特定の時点での株式ベータとCAPM計算に使用した生データのバージョンを確認したい場合はどうでしょうか。Deltaは企業がデータレイクに保存するビッグデータを自動的にバージョン管理するため、そのデータの任意の過去バージョンにアクセスできます。
# MAGIC 5. **スケール**: [Photon](https://www.databricks.com/product/photon)によって強化された[Databricks Runtime](https://docs.databricks.com/runtime/mlruntime.html)のバースト処理能力は、これらの非常に計算負荷の高い計算を非常に高速かつコスト効率よく実行できます。
# MAGIC 6. **データパイプライン**: データ品質を確認し、[制約](https://docs.databricks.com/delta/delta-constraints.html)を適用し、実行をスケジュールし、実行を監視できる堅牢なデータパイプラインでプロセス全体をオーケストレーションすることは、ソリューション全体を本番化するために重要です。Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables)（DLT）は、Delta Lake上で高品質なデータを提供する信頼性の高いデータパイプラインの構築と管理を容易にします。DLTは宣言的パイプライン開発、自動データテスト、監視と回復のための詳細な可視性によってETL開発と管理を簡素化します。
# MAGIC 7. **新規データの処理**: Databricksの[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)は、追加設定なしでクラウドストレージに到着する新しいデータファイルをインクリメンタルかつ効率的に処理します。\\(\beta{_i} \\) と \\(E(R{_e}) \\) を正確に測定するためには、頻繁に再計算する必要があります。
# MAGIC 8. **可視化**: 各社のベータと期待収益率を計算した後、Databricks SQLダッシュボードで全企業情報を可視化します。[Databricks SQL](https://www.databricks.com/product/databricks-sql)（DB SQL）はDatabricks Lakehouseプラットフォームのサーバーレスデータウェアハウスで、最大12倍優れたコストパフォーマンス、統一ガバナンスモデル、オープンフォーマットとAPI、お好みのツールで、ロックインなしにすべてのSQLおよびBIアプリケーションを大規模に実行できます。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # アーキテクチャ
# MAGIC 
# MAGIC これから構築するソリューションのアーキテクチャです。
# MAGIC <img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/capm_arch.png' style="float: center" width="1400px"  />

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマの設定
# Unity Catalog の カタログ名・スキーマ名をウィジェットで設定します。
# 実行前に適切な値に変更してください。
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema_stock", "stock_market_historical_data", "Stock Schema")
dbutils.widgets.text("schema_indices", "indices_historical_data", "Indices Schema")

catalog       = dbutils.widgets.get("catalog")
schema_stock  = dbutils.widgets.get("schema_stock")
schema_indices = dbutils.widgets.get("schema_indices")

# COMMAND ----------

# DBTITLE 1,ソースデータ読み込み用テーブルの作成
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog};
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_stock};
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_indices};
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${catalog}.${schema_stock}.us_closing_100;
# MAGIC 
# MAGIC CREATE TABLE ${catalog}.${schema_stock}.us_closing_100
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`s3a://db-gtm-industry-solutions/data/fsi/capm/us_closing_100/`;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ${catalog}.${schema_indices}.sp_500;
# MAGIC 
# MAGIC CREATE TABLE ${catalog}.${schema_indices}.sp_500
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`s3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog}.${schema_stock}.us_closing_100

# COMMAND ----------

closing_prices_df = spark.sql(f'select * from {catalog}.{schema_stock}.us_closing_100')

# COMMAND ----------

display(closing_prices_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this Notebook may be subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
