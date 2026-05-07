<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# 株式ベータ計算とCAPM

このソリューションでは、米国上場4,000社以上のベータ値（S&P500やNYSEなどの市場全体と比較した株式のボラティリティ、すなわち系統的リスクの指標）を算出し、それを用いてCAPM（資本資産価格モデル）による各社の期待収益率を導出します。

2010年1月1日から2022年7月末（12年以上）の日次データ（終値）を使用します。

## 株式ベータ計算とCAPMにDatabricks Lakehouseを使う理由

1. **データの量と多様性**: 4,000以上のファイル（米国上場ほぼ全銘柄の12年以上の日次OHLC＋出来高データ）があります。これらのファイルを手作業で管理・最適化するのは煩雑です。クエリ速度を向上させるために、[Delta Lake on Databricks](https://docs.databricks.com/delta/optimizations/file-mgmt.html)はクラウドストレージに格納されたデータのレイアウトを最適化する機能をサポートしています。Delta Lake on Databricksはビンパッキングとz-orderingの2種類のレイアウトアルゴリズムをサポートしています。
2. **データ品質の確保とスキーマの検出・適用**
   * データファイルの一部には欠損値があります（データベンダーからのインポート時 - 以下の```Cmd 9```を参照）。欠損レコードが含まれないようにする必要があります。
   * 多数のファイルを扱い、外部ベンダーからデータを取り込む場合、ファイルのスキーマの変更（または不一致）によってデータの下流処理が壊れないようにする必要があります。[Delta Lakeのスキーマ適用](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)は書き込み時にスキーマ検証を行います。テーブルへのすべての新規書き込みは、書き込み時にターゲットテーブルのスキーマとの互換性がチェックされます。スキーマに互換性がない場合、Delta Lakeはトランザクション全体をキャンセル（データは書き込まれない）し、不一致をユーザーに通知する例外を発生させます。
3. **データリネージ**: 多数のファイルと処理ステップを扱う場合、データ処理や欠損ファイルを追跡できる必要があります。ここで[Unity Catalog](https://www.databricks.com/product/unity-catalog)が役立ちます。Unity Catalogの[データリネージ](https://www.databricks.com/blog/2022/06/08/announcing-the-availability-of-data-lineage-with-unity-catalog.html)は実行時リネージを自動化し、すべてのワークロード、列レベルで機能し、ノートブック、スクリプト、サードパーティツール、ダッシュボードをカバーします。
4. **タイムトラベル**: 特定の時点での株式ベータとCAPM計算に使用した生データのバージョンを確認したい場合はどうでしょうか。Deltaは企業がデータレイクに保存するビッグデータを自動的にバージョン管理するため、そのデータの任意の過去バージョンにアクセスできます。この時系列データ管理により、監査、誤った書き込みや削除のデータロールバック、レポートの再現が容易になり、データパイプラインがシンプルになります。
5. **スケール**: [Photon](https://www.databricks.com/product/photon)によって強化された[Databricks Runtime](https://docs.databricks.com/runtime/mlruntime.html)のバースト処理能力は、これらの非常に計算負荷の高い計算を非常に高速かつコスト効率よく実行できます。
6. **データパイプライン**: データ品質を確認し、[制約](https://docs.databricks.com/delta/delta-constraints.html)を適用し、実行をスケジュールし、実行を監視できる堅牢なデータパイプラインでプロセス全体をオーケストレーションすることは、ソリューション全体を本番化するために重要です。Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables)（DLT）は、Delta Lake上で高品質なデータを提供する信頼性の高いデータパイプラインの構築と管理を容易にします。DLTは宣言的パイプライン開発、自動データテスト、監視と回復のための詳細な可視性によってETL開発と管理を簡素化します。
7. **新規データの処理**: Databricksの[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)は、追加設定なしでクラウドストレージに到着する新しいデータファイルをインクリメンタルかつ効率的に処理します。株式ベータと期待収益率を正確に測定するためには、頻繁に再計算する必要があります。
8. **可視化**: 各社のベータと期待収益率を計算した後、Databricks SQLダッシュボードで全企業情報を可視化します。[Databricks SQL](https://www.databricks.com/product/databricks-sql)（DB SQL）はDatabricks Lakehouseプラットフォーム上のサーバーレスデータウェアハウスで、最大12倍優れたコストパフォーマンス、統一ガバナンスモデル、オープンフォーマットとAPI、お好みのツールで、ロックインなしにすべてのSQLおよびBIアプリケーションを大規模に実行できます。

___

boris.banushev@databricks.com

___


<img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/capm_arch.png' />

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

このアクセラレータを実行するには、このリポジトリをDatabricksワークスペースにクローンしてください。RUNMEノートブックをDBR 11.0以降のランタイムを実行する任意のクラスターにアタッチし、Run-Allでノートブックを実行してください。アクセラレータパイプラインを記述するマルチステップジョブが作成され、リンクが提供されます。マルチステップジョブを実行してパイプラインがどのように動作するかを確認してください。

ジョブ設定はRUNMEノートブックにJSON形式で記述されています。アクセラレータの実行に関連するコストはユーザーの責任となります。
