# Databricks notebook source
# MAGIC %md
# MAGIC # 10_load_bronze_transactions
# MAGIC
# MAGIC Gerçek kaynak klasörü:
# MAGIC
# MAGIC `/Volumes/retail_marketing/source/source_files/master/`
# MAGIC
# MAGIC Bu notebook:
# MAGIC - `transaction_data.csv` dosyasından seçilen günü yükler.
# MAGIC - `coupon_redempt.csv` dosyasından seçilen günü yükler.
# MAGIC - Aynı `BatchID` tekrar çalıştırıldığında o batch'in önceki kayıtlarını silip güvenli biçimde yeniden yükler.
# MAGIC - Tablo bazlı ETL loglarını günceller.
# MAGIC

# COMMAND ----------

required_parameters = [
    "BatchID",
    "LoadMode",
    "ProcessDate",
    "ProcessDay",
    "ProcessWeek"
]

parameters = {}

for name in required_parameters:
    try:
        value = dbutils.widgets.get(name).strip()
    except Exception as exc:
        raise ValueError(
            f"{name} parametresi notebooka gelmedi."
        ) from exc

    if not value:
        raise ValueError(
            f"{name} parametresi boş geldi."
        )

    parameters[name] = value

print("Gelen parametreler:")
for key, value in parameters.items():
    print(f"{key} = {value!r}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_marketing;
# MAGIC
# MAGIC SELECT
# MAGIC     :BatchID AS BatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     CAST(:ProcessDay AS DECIMAL(10,0)) AS ProcessDay,
# MAGIC     UPPER(:LoadMode) AS LoadMode;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW etl_parameters AS
# MAGIC SELECT
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     CAST(:ProcessDay AS DECIMAL(10,0)) AS ProcessDay,
# MAGIC     UPPER(TRIM(:LoadMode)) AS LoadMode;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Kaynak dosyaları test et
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'transaction_data.csv' AS FileName, COUNT(*) AS TotalRowCount
# MAGIC FROM read_files(
# MAGIC     '/Volumes/retail_marketing/source/source_files/master/transaction_data.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'PERMISSIVE'
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'coupon_redempt.csv', COUNT(*)
# MAGIC FROM read_files(
# MAGIC     '/Volumes/retail_marketing/source/source_files/master/coupon_redempt.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'PERMISSIVE'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Batch kaydını başlat veya yeniden aç
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.control.etl_batch_control AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         BatchID,
# MAGIC         ProcessDate,
# MAGIC         ProcessDay,
# MAGIC         CAST(NULL AS DECIMAL(10,0)) AS ProcessWeek,
# MAGIC         LoadMode
# MAGIC     FROM etl_parameters
# MAGIC ) AS source
# MAGIC ON target.BatchID = source.BatchID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.ProcessDate  = source.ProcessDate,
# MAGIC     target.ProcessDay   = source.ProcessDay,
# MAGIC     target.LoadMode     = source.LoadMode,
# MAGIC     target.BatchStatus  = 'RUNNING',
# MAGIC     target.StartTime    = current_timestamp(),
# MAGIC     target.EndTime      = NULL,
# MAGIC     target.ErrorMessage = NULL,
# MAGIC     target.ETLTime      = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ProcessDay,
# MAGIC     ProcessWeek,
# MAGIC     LoadMode,
# MAGIC     BatchStatus,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     TriggerType,
# MAGIC     TriggeredBy,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.BatchID,
# MAGIC     source.ProcessDate,
# MAGIC     source.ProcessDay,
# MAGIC     source.ProcessWeek,
# MAGIC     source.LoadMode,
# MAGIC     'RUNNING',
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     'MANUAL',
# MAGIC     'Efe',
# MAGIC     NULL,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ProcessDay,
# MAGIC     LoadMode,
# MAGIC     BatchStatus,
# MAGIC     StartTime
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transaction kaynağını oku
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_source AS
# MAGIC SELECT
# MAGIC     TRY_CAST(household_key AS DECIMAL(10,0))     AS household_key,
# MAGIC     TRY_CAST(BASKET_ID AS DECIMAL(20,0))         AS basket_id,
# MAGIC     TRY_CAST(DAY AS DECIMAL(10,0))               AS day_no,
# MAGIC     TRY_CAST(PRODUCT_ID AS DECIMAL(10,0))        AS product_id,
# MAGIC     TRY_CAST(QUANTITY AS DECIMAL(18,4))          AS quantity,
# MAGIC     TRY_CAST(SALES_VALUE AS DECIMAL(18,4))       AS sales_value,
# MAGIC     TRY_CAST(STORE_ID AS DECIMAL(10,0))          AS store_id,
# MAGIC     TRY_CAST(RETAIL_DISC AS DECIMAL(18,4))       AS retail_disc,
# MAGIC     TRY_CAST(TRANS_TIME AS DECIMAL(10,0))        AS trans_time,
# MAGIC     TRY_CAST(WEEK_NO AS DECIMAL(10,0))           AS week_no,
# MAGIC     TRY_CAST(COUPON_DISC AS DECIMAL(18,4))       AS coupon_disc,
# MAGIC     TRY_CAST(COUPON_MATCH_DISC AS DECIMAL(18,4)) AS coupon_match_disc,
# MAGIC     _metadata.file_path                          AS SourceFile
# MAGIC FROM read_files(
# MAGIC     '/Volumes/retail_marketing/source/source_files/master/transaction_data.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'PERMISSIVE'
# MAGIC )
# MAGIC WHERE TRY_CAST(DAY AS DECIMAL(10,0))
# MAGIC       = CAST(:ProcessDay AS DECIMAL(10,0));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS SourceRowCount,
# MAGIC     MIN(day_no) AS MinDay,
# MAGIC     MAX(day_no) AS MaxDay,
# MAGIC     COUNT(DISTINCT basket_id) AS DistinctBasketCount,
# MAGIC     COUNT(DISTINCT product_id) AS DistinctProductCount
# MAGIC FROM transaction_source;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transaction logunu başlat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.audit.etl_table_load_log AS target
# MAGIC
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_BRONZE_TRANSACTION') AS ETLLogID,
# MAGIC         TRIM(:BatchID) AS BatchID,
# MAGIC         CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)) AS ProcessValue,
# MAGIC         (SELECT COUNT(*) FROM transaction_source) AS SourceRowCount
# MAGIC ) AS source
# MAGIC
# MAGIC ON target.ETLLogID = source.ETLLogID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.BatchID           = source.BatchID,
# MAGIC     target.LayerName         = 'BRONZE',
# MAGIC     target.SourceName        = 'transaction_data',
# MAGIC     target.SourceFile        = NULL,
# MAGIC     target.TargetTableName   = 'retail_marketing.bronze.transaction_data_raw',
# MAGIC     target.LoadStrategy      = 'INCREMENTAL_DAY_RELOAD',
# MAGIC     target.ProcessDate       = source.ProcessDate,
# MAGIC     target.ProcessValue      = source.ProcessValue,
# MAGIC     target.StartTime         = current_timestamp(),
# MAGIC     target.EndTime           = NULL,
# MAGIC     target.SourceRowCount    = source.SourceRowCount,
# MAGIC     target.InsertedRowCount  = 0,
# MAGIC     target.UpdatedRowCount   = 0,
# MAGIC     target.DeletedRowCount   = 0,
# MAGIC     target.RejectedRowCount  = 0,
# MAGIC     target.UnchangedRowCount = 0,
# MAGIC     target.LoadStatus        = 'STARTED',
# MAGIC     target.ErrorMessage      = NULL,
# MAGIC     target.ETLTime           = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ETLLogID,
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     SourceName,
# MAGIC     SourceFile,
# MAGIC     TargetTableName,
# MAGIC     LoadStrategy,
# MAGIC     ProcessDate,
# MAGIC     ProcessValue,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     UpdatedRowCount,
# MAGIC     DeletedRowCount,
# MAGIC     RejectedRowCount,
# MAGIC     UnchangedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.ETLLogID,
# MAGIC     source.BatchID,
# MAGIC     'BRONZE',
# MAGIC     'transaction_data',
# MAGIC     NULL,
# MAGIC     'retail_marketing.bronze.transaction_data_raw',
# MAGIC     'INCREMENTAL_DAY_RELOAD',
# MAGIC     source.ProcessDate,
# MAGIC     source.ProcessValue,
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     source.SourceRowCount,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     'STARTED',
# MAGIC     NULL,
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aynı batch'in eski Transaction kayıtlarını temizle ve yeniden yükle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.bronze.transaction_data_raw (
# MAGIC     household_key,
# MAGIC     basket_id,
# MAGIC     day_no,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     sales_value,
# MAGIC     store_id,
# MAGIC     retail_disc,
# MAGIC     trans_time,
# MAGIC     week_no,
# MAGIC     coupon_disc,
# MAGIC     coupon_match_disc,
# MAGIC     SourceFile,
# MAGIC     SourceSystem,
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ETLTime,
# MAGIC     RecordHash
# MAGIC )
# MAGIC SELECT
# MAGIC     source.household_key,
# MAGIC     source.basket_id,
# MAGIC     source.day_no,
# MAGIC     source.product_id,
# MAGIC     source.quantity,
# MAGIC     source.sales_value,
# MAGIC     source.store_id,
# MAGIC     source.retail_disc,
# MAGIC     source.trans_time,
# MAGIC     source.week_no,
# MAGIC     source.coupon_disc,
# MAGIC     source.coupon_match_disc,
# MAGIC     source.SourceFile,
# MAGIC     'DUNNHUMBY_COMPLETE_JOURNEY' AS SourceSystem,
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     current_timestamp() AS ETLTime,
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(source.household_key AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.basket_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.day_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.product_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.quantity AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.sales_value AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.store_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.retail_disc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.trans_time AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.week_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.coupon_disc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.coupon_match_disc AS STRING), '<NULL>')
# MAGIC         ),
# MAGIC         256
# MAGIC     ) AS RecordHash
# MAGIC FROM transaction_source AS source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM retail_marketing.control.etl_batch_control AS batch
# MAGIC     WHERE batch.BatchID = TRIM(:BatchID)
# MAGIC       AND batch.BatchStatus = 'RUNNING'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transaction mutabakatı ve log kapatma
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_reconciliation AS
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM transaction_source) AS SourceRowCount,
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC         FROM retail_marketing.bronze.transaction_data_raw
# MAGIC         WHERE BatchID = TRIM(:BatchID)
# MAGIC     ) AS InsertedRowCount;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_marketing.audit.etl_table_load_log
# MAGIC SET
# MAGIC     EndTime = current_timestamp(),
# MAGIC     SourceRowCount =
# MAGIC         (SELECT SourceRowCount FROM transaction_reconciliation),
# MAGIC     InsertedRowCount =
# MAGIC         (SELECT InsertedRowCount FROM transaction_reconciliation),
# MAGIC     LoadStatus =
# MAGIC         CASE
# MAGIC             WHEN (SELECT SourceRowCount FROM transaction_reconciliation) = 0
# MAGIC                 THEN 'FAILED'
# MAGIC             WHEN (SELECT SourceRowCount FROM transaction_reconciliation)
# MAGIC                = (SELECT InsertedRowCount FROM transaction_reconciliation)
# MAGIC                 THEN 'SUCCESS'
# MAGIC             ELSE 'FAILED'
# MAGIC         END,
# MAGIC     ErrorMessage =
# MAGIC         CASE
# MAGIC             WHEN (SELECT SourceRowCount FROM transaction_reconciliation) = 0
# MAGIC                 THEN CONCAT(
# MAGIC                     'transaction_data.csv içinde ProcessDay=',
# MAGIC                     CAST(:ProcessDay AS STRING),
# MAGIC                     ' için kayıt bulunamadı.'
# MAGIC                 )
# MAGIC             WHEN (SELECT SourceRowCount FROM transaction_reconciliation)
# MAGIC                = (SELECT InsertedRowCount FROM transaction_reconciliation)
# MAGIC                 THEN NULL
# MAGIC             ELSE CONCAT(
# MAGIC                 'Transaction kaynak-hedef satır farkı: ',
# MAGIC                 CAST(
# MAGIC                     (SELECT SourceRowCount - InsertedRowCount
# MAGIC                      FROM transaction_reconciliation)
# MAGIC                     AS STRING
# MAGIC                 )
# MAGIC             )
# MAGIC         END,
# MAGIC     ETLTime = current_timestamp()
# MAGIC WHERE ETLLogID = CONCAT(TRIM(:BatchID), '_BRONZE_TRANSACTION');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Coupon redemption kaynağını oku
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coupon_redemption_source AS
# MAGIC SELECT
# MAGIC     TRY_CAST(household_key AS DECIMAL(10,0)) AS household_key,
# MAGIC     TRY_CAST(DAY AS DECIMAL(10,0))           AS day_no,
# MAGIC     TRY_CAST(COUPON_UPC AS DECIMAL(20,0))    AS coupon_upc,
# MAGIC     TRY_CAST(CAMPAIGN AS DECIMAL(10,0))      AS campaign_id,
# MAGIC     _metadata.file_path                      AS SourceFile
# MAGIC FROM read_files(
# MAGIC     '/Volumes/retail_marketing/source/source_files/master/coupon_redempt.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'PERMISSIVE'
# MAGIC )
# MAGIC WHERE TRY_CAST(DAY AS DECIMAL(10,0))
# MAGIC       = CAST(:ProcessDay AS DECIMAL(10,0));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS SourceRowCount,
# MAGIC     MIN(day_no) AS MinDay,
# MAGIC     MAX(day_no) AS MaxDay
# MAGIC FROM coupon_redemption_source;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Coupon redemption logunu başlat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.audit.etl_table_load_log AS target
# MAGIC
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_BRONZE_COUPON_REDEMPTION') AS ETLLogID,
# MAGIC         TRIM(:BatchID) AS BatchID,
# MAGIC         CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)) AS ProcessValue,
# MAGIC         (SELECT COUNT(*) FROM coupon_redemption_source) AS SourceRowCount
# MAGIC ) AS source
# MAGIC
# MAGIC ON target.ETLLogID = source.ETLLogID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.BatchID           = source.BatchID,
# MAGIC     target.LayerName         = 'BRONZE',
# MAGIC     target.SourceName        = 'coupon_redemption',
# MAGIC     target.SourceFile        = NULL,
# MAGIC     target.TargetTableName   = 'retail_marketing.bronze.coupon_redemption_raw',
# MAGIC     target.LoadStrategy      = 'INCREMENTAL_DAY_RELOAD',
# MAGIC     target.ProcessDate       = source.ProcessDate,
# MAGIC     target.ProcessValue      = source.ProcessValue,
# MAGIC     target.StartTime         = current_timestamp(),
# MAGIC     target.EndTime           =
# MAGIC         CASE
# MAGIC             WHEN source.SourceRowCount = 0
# MAGIC             THEN current_timestamp()
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC     target.SourceRowCount    = source.SourceRowCount,
# MAGIC     target.InsertedRowCount  = 0,
# MAGIC     target.UpdatedRowCount   = 0,
# MAGIC     target.DeletedRowCount   = 0,
# MAGIC     target.RejectedRowCount  = 0,
# MAGIC     target.UnchangedRowCount = 0,
# MAGIC     target.LoadStatus        =
# MAGIC         CASE
# MAGIC             WHEN source.SourceRowCount = 0
# MAGIC             THEN 'SKIPPED'
# MAGIC             ELSE 'STARTED'
# MAGIC         END,
# MAGIC     target.ErrorMessage      =
# MAGIC         CASE
# MAGIC             WHEN source.SourceRowCount = 0
# MAGIC             THEN 'ProcessDay için kupon kullanım kaydı bulunamadı.'
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC     target.ETLTime = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ETLLogID,
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     SourceName,
# MAGIC     SourceFile,
# MAGIC     TargetTableName,
# MAGIC     LoadStrategy,
# MAGIC     ProcessDate,
# MAGIC     ProcessValue,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     UpdatedRowCount,
# MAGIC     DeletedRowCount,
# MAGIC     RejectedRowCount,
# MAGIC     UnchangedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.ETLLogID,
# MAGIC     source.BatchID,
# MAGIC     'BRONZE',
# MAGIC     'coupon_redemption',
# MAGIC     NULL,
# MAGIC     'retail_marketing.bronze.coupon_redemption_raw',
# MAGIC     'INCREMENTAL_DAY_RELOAD',
# MAGIC     source.ProcessDate,
# MAGIC     source.ProcessValue,
# MAGIC     current_timestamp(),
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0
# MAGIC         THEN current_timestamp()
# MAGIC         ELSE NULL
# MAGIC     END,
# MAGIC
# MAGIC     source.SourceRowCount,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0
# MAGIC         THEN 'SKIPPED'
# MAGIC         ELSE 'STARTED'
# MAGIC     END,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0
# MAGIC         THEN 'ProcessDay için kupon kullanım kaydı bulunamadı.'
# MAGIC         ELSE NULL
# MAGIC     END,
# MAGIC
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Aynı batch'in eski Coupon redemption kayıtlarını temizle ve yeniden yükle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.bronze.coupon_redemption_raw (
# MAGIC     household_key,
# MAGIC     day_no,
# MAGIC     coupon_upc,
# MAGIC     campaign_id,
# MAGIC     SourceFile,
# MAGIC     SourceSystem,
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ETLTime,
# MAGIC     RecordHash
# MAGIC )
# MAGIC SELECT
# MAGIC     source.household_key,
# MAGIC     source.day_no,
# MAGIC     source.coupon_upc,
# MAGIC     source.campaign_id,
# MAGIC     source.SourceFile,
# MAGIC     'DUNNHUMBY_COMPLETE_JOURNEY',
# MAGIC     TRIM(:BatchID),
# MAGIC     CAST(:ProcessDate AS DATE),
# MAGIC     current_timestamp(),
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(source.household_key AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.day_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.coupon_upc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(source.campaign_id AS STRING), '<NULL>')
# MAGIC         ),
# MAGIC         256
# MAGIC     )
# MAGIC FROM coupon_redemption_source AS source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM retail_marketing.control.etl_batch_control AS batch
# MAGIC     WHERE batch.BatchID = TRIM(:BatchID)
# MAGIC       AND batch.BatchStatus = 'RUNNING'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Coupon redemption logunu kapat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_marketing.audit.etl_table_load_log
# MAGIC SET
# MAGIC     EndTime = current_timestamp(),
# MAGIC     InsertedRowCount = (
# MAGIC         SELECT COUNT(*)
# MAGIC         FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC         WHERE BatchID = TRIM(:BatchID)
# MAGIC     ),
# MAGIC     LoadStatus =
# MAGIC         CASE
# MAGIC             WHEN SourceRowCount = 0 THEN 'SKIPPED'
# MAGIC             WHEN SourceRowCount = (
# MAGIC                 SELECT COUNT(*)
# MAGIC                 FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC                 WHERE BatchID = TRIM(:BatchID)
# MAGIC             ) THEN 'SUCCESS'
# MAGIC             ELSE 'FAILED'
# MAGIC         END,
# MAGIC     ErrorMessage =
# MAGIC         CASE
# MAGIC             WHEN SourceRowCount = 0
# MAGIC                 THEN 'ProcessDay için kupon kullanım kaydı bulunamadı.'
# MAGIC             WHEN SourceRowCount = (
# MAGIC                 SELECT COUNT(*)
# MAGIC                 FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC                 WHERE BatchID = TRIM(:BatchID)
# MAGIC             ) THEN NULL
# MAGIC             ELSE 'Coupon redemption kaynak-hedef satır sayıları eşleşmiyor.'
# MAGIC         END,
# MAGIC     ETLTime = current_timestamp()
# MAGIC WHERE ETLLogID =
# MAGIC       CONCAT(TRIM(:BatchID), '_BRONZE_COUPON_REDEMPTION');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Sonuç ve log kontrolleri
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'transaction_data' AS SourceName,
# MAGIC     COUNT(*) AS LoadedRowCount,
# MAGIC     MIN(day_no) AS MinDay,
# MAGIC     MAX(day_no) AS MaxDay,
# MAGIC     COUNT(DISTINCT RecordHash) AS DistinctRecordCount
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'coupon_redemption',
# MAGIC     COUNT(*),
# MAGIC     MIN(day_no),
# MAGIC     MAX(day_no),
# MAGIC     COUNT(DISTINCT RecordHash)
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SourceName,
# MAGIC     TargetTableName,
# MAGIC     LoadStatus,
# MAGIC     ProcessValue,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'BRONZE'
# MAGIC   AND SourceName IN ('transaction_data', 'coupon_redemption')
# MAGIC ORDER BY SourceName;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'transaction_data' AS SourceName,
# MAGIC     COUNT(*) AS WrongProcessValueCount
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND day_no <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'coupon_redemption',
# MAGIC     COUNT(*)
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND day_no <> CAST(:ProcessDay AS DECIMAL(10,0));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     day_no,
# MAGIC     COUNT(*) AS RowCount
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = 'RM_DAILY_D0002'
# MAGIC GROUP BY BatchID, day_no;