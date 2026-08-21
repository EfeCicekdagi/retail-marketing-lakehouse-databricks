# Databricks notebook source
# MAGIC %md
# MAGIC # 20_build_silver_transactions — Working Version
# MAGIC
# MAGIC Bu notebook iki Bronze işlem tablosunu Silver'a taşır:
# MAGIC
# MAGIC - `bronze.transaction_data_raw` → `silver.transaction_clean`
# MAGIC - `bronze.coupon_redemption_raw` → `silver.coupon_redemption_clean`
# MAGIC
# MAGIC Özellikler:
# MAGIC - Aynı `BatchID` tekrar çalıştırılabilir.
# MAGIC - Önce aynı batch'e ait eski Silver kayıtları silinir.
# MAGIC - Geçersiz kayıtlar Silver dışında bırakılır.
# MAGIC - Duplicate kayıtlar temizlenir.
# MAGIC - Yükleme sonuçları `audit.etl_table_load_log` tablosuna yazılır.
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
# MAGIC USE CATALOG retail_marketing
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     CAST(:ProcessDay AS DECIMAL(10,0)) AS ProcessDay
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch ve Bronze hazırlık kontrolü
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ProcessDay,
# MAGIC     ProcessWeek,
# MAGIC     LoadMode,
# MAGIC     BatchStatus
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'transaction_data_raw' AS SourceName,
# MAGIC     COUNT(*) AS BronzeRowCount
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'coupon_redemption_raw',
# MAGIC     COUNT(*)
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transaction kaynaklarını geçerli, geçersiz ve tekilleştirilmiş olarak hazırla
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_valid_source AS
# MAGIC SELECT
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
# MAGIC     ProcessDate,
# MAGIC     ETLTime,
# MAGIC     RecordHash
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND day_no = CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC   AND basket_id IS NOT NULL
# MAGIC   AND product_id IS NOT NULL
# MAGIC   AND day_no IS NOT NULL
# MAGIC   AND quantity IS NOT NULL
# MAGIC   AND quantity > 0
# MAGIC   AND sales_value IS NOT NULL
# MAGIC   AND sales_value >= 0
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_invalid_source AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN basket_id IS NULL THEN 'BASKET_ID_NULL'
# MAGIC         WHEN product_id IS NULL THEN 'PRODUCT_ID_NULL'
# MAGIC         WHEN day_no IS NULL THEN 'DAY_NULL'
# MAGIC         WHEN day_no <> CAST(:ProcessDay AS DECIMAL(10,0)) THEN 'WRONG_PROCESS_DAY'
# MAGIC         WHEN quantity IS NULL THEN 'QUANTITY_NULL'
# MAGIC         WHEN quantity <= 0 THEN 'INVALID_QUANTITY'
# MAGIC         WHEN sales_value IS NULL THEN 'SALES_VALUE_NULL'
# MAGIC         WHEN sales_value < 0 THEN 'NEGATIVE_SALES_VALUE'
# MAGIC         ELSE 'UNKNOWN_ERROR'
# MAGIC     END AS RejectReason
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND (
# MAGIC       basket_id IS NULL
# MAGIC       OR product_id IS NULL
# MAGIC       OR day_no IS NULL
# MAGIC       OR day_no <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC       OR quantity IS NULL
# MAGIC       OR quantity <= 0
# MAGIC       OR sales_value IS NULL
# MAGIC       OR sales_value < 0
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_deduplicated_source AS
# MAGIC SELECT
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
# MAGIC     ProcessDate,
# MAGIC     RecordHash
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         source.*,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY RecordHash
# MAGIC             ORDER BY ETLTime DESC
# MAGIC         ) AS RowNumber
# MAGIC     FROM transaction_valid_source AS source
# MAGIC )
# MAGIC WHERE RowNumber = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_silver_source AS
# MAGIC SELECT
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
# MAGIC
# MAGIC     sales_value AS net_sales_value,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN COALESCE(coupon_disc, 0) <> 0
# MAGIC           OR COALESCE(coupon_match_disc, 0) <> 0
# MAGIC         THEN 'Y'
# MAGIC         ELSE 'N'
# MAGIC     END AS has_coupon,
# MAGIC
# MAGIC     TRIM(:BatchID) AS SourceBatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(household_key AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(basket_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(day_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(product_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(quantity AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(sales_value AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(store_id AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(retail_disc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(trans_time AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(week_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(coupon_disc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(coupon_match_disc AS STRING), '<NULL>'),
# MAGIC             CASE
# MAGIC                 WHEN COALESCE(coupon_disc, 0) <> 0
# MAGIC                   OR COALESCE(coupon_match_disc, 0) <> 0
# MAGIC                 THEN 'Y'
# MAGIC                 ELSE 'N'
# MAGIC             END
# MAGIC         ),
# MAGIC         256
# MAGIC     ) AS RecordHash
# MAGIC FROM transaction_deduplicated_source
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transaction Silver logunu başlat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.audit.etl_table_load_log AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_SILVER_TRANSACTION') AS ETLLogID,
# MAGIC         TRIM(:BatchID) AS BatchID,
# MAGIC         CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)) AS ProcessValue,
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.bronze.transaction_data_raw
# MAGIC             WHERE BatchID = TRIM(:BatchID)
# MAGIC         ) AS SourceRowCount
# MAGIC ) AS source
# MAGIC ON target.ETLLogID = source.ETLLogID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.BatchID           = source.BatchID,
# MAGIC     target.LayerName         = 'SILVER',
# MAGIC     target.SourceName        = 'transaction_data_raw',
# MAGIC     target.TargetTableName   = 'retail_marketing.silver.transaction_clean',
# MAGIC     target.LoadStrategy          = 'CLEAN_DEDUPLICATE_RELOAD',
# MAGIC     target.ProcessDate       = source.ProcessDate,
# MAGIC     target.ProcessValue      = source.ProcessValue,
# MAGIC     target.StartTime         = current_timestamp(),
# MAGIC     target.EndTime           = NULL,
# MAGIC     target.SourceRowCount    = source.SourceRowCount,
# MAGIC     target.InsertedRowCount  = 0,
# MAGIC     target.UpdatedRowCount   = 0,
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
# MAGIC     TargetTableName,
# MAGIC     LoadStrategy,
# MAGIC     ProcessDate,
# MAGIC     ProcessValue,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     UpdatedRowCount,
# MAGIC     RejectedRowCount,
# MAGIC     UnchangedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.ETLLogID,
# MAGIC     source.BatchID,
# MAGIC     'SILVER',
# MAGIC     'transaction_data_raw',
# MAGIC     'retail_marketing.silver.transaction_clean',
# MAGIC     'CLEAN_DEDUPLICATE_RELOAD',
# MAGIC     source.ProcessDate,
# MAGIC     source.ProcessValue,
# MAGIC     current_timestamp(),
# MAGIC     NULL,
# MAGIC     source.SourceRowCount,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     'STARTED',
# MAGIC     NULL,
# MAGIC     current_timestamp()
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aynı batch'in eski Transaction Silver kayıtlarını temizle ve yükle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.silver.transaction_clean
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.silver.transaction_clean (
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
# MAGIC     net_sales_value,
# MAGIC     has_coupon,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     ETLTime,
# MAGIC     RecordHash
# MAGIC )
# MAGIC SELECT
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
# MAGIC     net_sales_value,
# MAGIC     has_coupon,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     current_timestamp(),
# MAGIC     RecordHash
# MAGIC FROM transaction_silver_source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM retail_marketing.control.etl_batch_control
# MAGIC     WHERE BatchID = TRIM(:BatchID)
# MAGIC       AND BatchStatus = 'RUNNING'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transaction mutabakatı ve log kapatma
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transaction_silver_reconciliation AS
# MAGIC SELECT
# MAGIC     bronze.BronzeRowCount,
# MAGIC     valid.ValidRowCount,
# MAGIC     invalid.RejectedRowCount,
# MAGIC     duplicate.DuplicateRowCount,
# MAGIC     silver.InsertedRowCount,
# MAGIC     bronze.BronzeRowCount
# MAGIC       - invalid.RejectedRowCount
# MAGIC       - duplicate.DuplicateRowCount
# MAGIC       - silver.InsertedRowCount AS DifferenceCount
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS BronzeRowCount
# MAGIC     FROM retail_marketing.bronze.transaction_data_raw
# MAGIC     WHERE BatchID = TRIM(:BatchID)
# MAGIC ) AS bronze
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS ValidRowCount
# MAGIC     FROM transaction_valid_source
# MAGIC ) AS valid
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS RejectedRowCount
# MAGIC     FROM transaction_invalid_source
# MAGIC ) AS invalid
# MAGIC CROSS JOIN (
# MAGIC     SELECT
# MAGIC         (SELECT COUNT(*) FROM transaction_valid_source)
# MAGIC         -
# MAGIC         (SELECT COUNT(*) FROM transaction_deduplicated_source)
# MAGIC         AS DuplicateRowCount
# MAGIC ) AS duplicate
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS InsertedRowCount
# MAGIC     FROM retail_marketing.silver.transaction_clean
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) AS silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM transaction_silver_reconciliation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_marketing.audit.etl_table_load_log
# MAGIC SET
# MAGIC     EndTime =
# MAGIC         current_timestamp(),
# MAGIC
# MAGIC     SourceRowCount =
# MAGIC         (SELECT BronzeRowCount FROM transaction_silver_reconciliation),
# MAGIC
# MAGIC     InsertedRowCount =
# MAGIC         (SELECT InsertedRowCount FROM transaction_silver_reconciliation),
# MAGIC
# MAGIC     RejectedRowCount =
# MAGIC         (SELECT RejectedRowCount FROM transaction_silver_reconciliation),
# MAGIC
# MAGIC     UnchangedRowCount =
# MAGIC         (SELECT DuplicateRowCount FROM transaction_silver_reconciliation),
# MAGIC
# MAGIC     LoadStatus =
# MAGIC         CASE
# MAGIC             WHEN (SELECT BronzeRowCount FROM transaction_silver_reconciliation) = 0
# MAGIC                 THEN 'FAILED'
# MAGIC             WHEN (SELECT DifferenceCount FROM transaction_silver_reconciliation) <> 0
# MAGIC                 THEN 'FAILED'
# MAGIC             WHEN (
# MAGIC                 SELECT RejectedRowCount + DuplicateRowCount
# MAGIC                 FROM transaction_silver_reconciliation
# MAGIC             ) > 0
# MAGIC                 THEN 'SUCCESS_WITH_WARNING'
# MAGIC             ELSE 'SUCCESS'
# MAGIC         END,
# MAGIC
# MAGIC     ErrorMessage =
# MAGIC         CASE
# MAGIC             WHEN (SELECT BronzeRowCount FROM transaction_silver_reconciliation) = 0
# MAGIC                 THEN 'Bronze transaction kaydı bulunamadı.'
# MAGIC             WHEN (SELECT DifferenceCount FROM transaction_silver_reconciliation) <> 0
# MAGIC                 THEN CONCAT(
# MAGIC                     'Transaction Silver mutabakat farkı: ',
# MAGIC                     CAST(
# MAGIC                         (SELECT DifferenceCount FROM transaction_silver_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     )
# MAGIC                 )
# MAGIC             WHEN (
# MAGIC                 SELECT RejectedRowCount + DuplicateRowCount
# MAGIC                 FROM transaction_silver_reconciliation
# MAGIC             ) > 0
# MAGIC                 THEN CONCAT(
# MAGIC                     'Rejected=',
# MAGIC                     CAST(
# MAGIC                         (SELECT RejectedRowCount FROM transaction_silver_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     ),
# MAGIC                     ', Duplicate=',
# MAGIC                     CAST(
# MAGIC                         (SELECT DuplicateRowCount FROM transaction_silver_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     )
# MAGIC                 )
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC
# MAGIC     ETLTime = current_timestamp()
# MAGIC WHERE ETLLogID = CONCAT(TRIM(:BatchID), '_SILVER_TRANSACTION')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     day_no,
# MAGIC     SourceBatchID,
# MAGIC     COUNT(*) AS RowCount
# MAGIC FROM transaction_silver_source
# MAGIC GROUP BY
# MAGIC     day_no,
# MAGIC     SourceBatchID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Coupon redemption kaynaklarını hazırla
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coupon_redemption_valid_source AS
# MAGIC SELECT
# MAGIC     household_key,
# MAGIC     day_no,
# MAGIC     coupon_upc,
# MAGIC     campaign_id,
# MAGIC     ProcessDate,
# MAGIC     ETLTime
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND household_key IS NOT NULL
# MAGIC   AND day_no IS NOT NULL
# MAGIC   AND coupon_upc IS NOT NULL
# MAGIC   AND day_no = CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coupon_redemption_invalid_source AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN household_key IS NULL THEN 'HOUSEHOLD_KEY_NULL'
# MAGIC         WHEN day_no IS NULL THEN 'DAY_NULL'
# MAGIC         WHEN coupon_upc IS NULL THEN 'COUPON_UPC_NULL'
# MAGIC         WHEN day_no <> CAST(:ProcessDay AS DECIMAL(10,0)) THEN 'WRONG_PROCESS_DAY'
# MAGIC         ELSE 'UNKNOWN_ERROR'
# MAGIC     END AS RejectReason
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND (
# MAGIC       household_key IS NULL
# MAGIC       OR day_no IS NULL
# MAGIC       OR coupon_upc IS NULL
# MAGIC       OR day_no <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coupon_redemption_silver_source AS
# MAGIC SELECT
# MAGIC     household_key,
# MAGIC     day_no,
# MAGIC     coupon_upc,
# MAGIC     campaign_id,
# MAGIC     TRIM(:BatchID) AS SourceBatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     SHA2(
# MAGIC         CONCAT_WS(
# MAGIC             '||',
# MAGIC             COALESCE(CAST(household_key AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(day_no AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(coupon_upc AS STRING), '<NULL>'),
# MAGIC             COALESCE(CAST(campaign_id AS STRING), '<NULL>')
# MAGIC         ),
# MAGIC         256
# MAGIC     ) AS RecordHash
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         source.*,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY
# MAGIC                 household_key,
# MAGIC                 day_no,
# MAGIC                 coupon_upc,
# MAGIC                 campaign_id
# MAGIC             ORDER BY ETLTime DESC
# MAGIC         ) AS RowNumber
# MAGIC     FROM coupon_redemption_valid_source AS source
# MAGIC )
# MAGIC WHERE RowNumber = 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Coupon redemption logunu başlat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.audit.etl_table_load_log AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_SILVER_COUPON_REDEMPTION') AS ETLLogID,
# MAGIC         TRIM(:BatchID) AS BatchID,
# MAGIC         CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)) AS ProcessValue,
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC             WHERE BatchID = TRIM(:BatchID)
# MAGIC         ) AS SourceRowCount
# MAGIC ) AS source
# MAGIC ON target.ETLLogID = source.ETLLogID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.BatchID           = source.BatchID,
# MAGIC     target.LayerName         = 'SILVER',
# MAGIC     target.SourceName        = 'coupon_redemption_raw',
# MAGIC     target.TargetTableName   = 'retail_marketing.silver.coupon_redemption_clean',
# MAGIC     target.LoadStrategy          = 'CLEAN_DEDUPLICATE_RELOAD',
# MAGIC     target.ProcessDate       = source.ProcessDate,
# MAGIC     target.ProcessValue      = source.ProcessValue,
# MAGIC     target.StartTime         = current_timestamp(),
# MAGIC     target.EndTime           = NULL,
# MAGIC     target.SourceRowCount    = source.SourceRowCount,
# MAGIC     target.InsertedRowCount  = 0,
# MAGIC     target.UpdatedRowCount   = 0,
# MAGIC     target.RejectedRowCount  = 0,
# MAGIC     target.UnchangedRowCount = 0,
# MAGIC     target.LoadStatus =
# MAGIC         CASE WHEN source.SourceRowCount = 0 THEN 'SKIPPED' ELSE 'STARTED' END,
# MAGIC     target.ErrorMessage =
# MAGIC         CASE
# MAGIC             WHEN source.SourceRowCount = 0
# MAGIC                 THEN 'Batch için coupon redemption kaydı bulunamadı.'
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC     target.ETLTime = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ETLLogID,
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     SourceName,
# MAGIC     TargetTableName,
# MAGIC     LoadStrategy,
# MAGIC     ProcessDate,
# MAGIC     ProcessValue,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     UpdatedRowCount,
# MAGIC     RejectedRowCount,
# MAGIC     UnchangedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.ETLLogID,
# MAGIC     source.BatchID,
# MAGIC     'SILVER',
# MAGIC     'coupon_redemption_raw',
# MAGIC     'retail_marketing.silver.coupon_redemption_clean',
# MAGIC     'CLEAN_DEDUPLICATE_RELOAD',
# MAGIC     source.ProcessDate,
# MAGIC     source.ProcessValue,
# MAGIC     current_timestamp(),
# MAGIC     CASE WHEN source.SourceRowCount = 0 THEN current_timestamp() ELSE NULL END,
# MAGIC     source.SourceRowCount,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     CASE WHEN source.SourceRowCount = 0 THEN 'SKIPPED' ELSE 'STARTED' END,
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0
# MAGIC             THEN 'Batch için coupon redemption kaydı bulunamadı.'
# MAGIC         ELSE NULL
# MAGIC     END,
# MAGIC     current_timestamp()
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aynı batch'in eski Coupon redemption Silver kayıtlarını temizle ve yükle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.silver.coupon_redemption_clean
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.silver.coupon_redemption_clean (
# MAGIC     household_key,
# MAGIC     day_no,
# MAGIC     coupon_upc,
# MAGIC     campaign_id,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     ETLTime,
# MAGIC     RecordHash
# MAGIC )
# MAGIC SELECT
# MAGIC     household_key,
# MAGIC     day_no,
# MAGIC     coupon_upc,
# MAGIC     campaign_id,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     current_timestamp(),
# MAGIC     RecordHash
# MAGIC FROM coupon_redemption_silver_source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM retail_marketing.control.etl_batch_control
# MAGIC     WHERE BatchID = TRIM(:BatchID)
# MAGIC       AND BatchStatus = 'RUNNING'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Coupon redemption mutabakatı ve log kapatma
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW coupon_redemption_reconciliation AS
# MAGIC SELECT
# MAGIC     bronze.BronzeRowCount,
# MAGIC     invalid.RejectedRowCount,
# MAGIC     duplicate.DuplicateRowCount,
# MAGIC     silver.InsertedRowCount,
# MAGIC     bronze.BronzeRowCount
# MAGIC       - invalid.RejectedRowCount
# MAGIC       - duplicate.DuplicateRowCount
# MAGIC       - silver.InsertedRowCount AS DifferenceCount
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS BronzeRowCount
# MAGIC     FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC     WHERE BatchID = TRIM(:BatchID)
# MAGIC ) AS bronze
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS RejectedRowCount
# MAGIC     FROM coupon_redemption_invalid_source
# MAGIC ) AS invalid
# MAGIC CROSS JOIN (
# MAGIC     SELECT
# MAGIC         (SELECT COUNT(*) FROM coupon_redemption_valid_source)
# MAGIC         -
# MAGIC         (SELECT COUNT(*) FROM coupon_redemption_silver_source)
# MAGIC         AS DuplicateRowCount
# MAGIC ) AS duplicate
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS InsertedRowCount
# MAGIC     FROM retail_marketing.silver.coupon_redemption_clean
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) AS silver
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_marketing.audit.etl_table_load_log
# MAGIC SET
# MAGIC     EndTime = current_timestamp(),
# MAGIC
# MAGIC     InsertedRowCount =
# MAGIC         (SELECT InsertedRowCount FROM coupon_redemption_reconciliation),
# MAGIC
# MAGIC     RejectedRowCount =
# MAGIC         (SELECT RejectedRowCount FROM coupon_redemption_reconciliation),
# MAGIC
# MAGIC     UnchangedRowCount =
# MAGIC         (SELECT DuplicateRowCount FROM coupon_redemption_reconciliation),
# MAGIC
# MAGIC     LoadStatus =
# MAGIC         CASE
# MAGIC             WHEN (SELECT BronzeRowCount FROM coupon_redemption_reconciliation) = 0
# MAGIC                 THEN 'SKIPPED'
# MAGIC             WHEN (SELECT DifferenceCount FROM coupon_redemption_reconciliation) <> 0
# MAGIC                 THEN 'FAILED'
# MAGIC             WHEN (
# MAGIC                 SELECT RejectedRowCount + DuplicateRowCount
# MAGIC                 FROM coupon_redemption_reconciliation
# MAGIC             ) > 0
# MAGIC                 THEN 'SUCCESS_WITH_WARNING'
# MAGIC             ELSE 'SUCCESS'
# MAGIC         END,
# MAGIC
# MAGIC     ErrorMessage =
# MAGIC         CASE
# MAGIC             WHEN (SELECT BronzeRowCount FROM coupon_redemption_reconciliation) = 0
# MAGIC                 THEN 'Batch için coupon redemption kaydı bulunamadı.'
# MAGIC             WHEN (SELECT DifferenceCount FROM coupon_redemption_reconciliation) <> 0
# MAGIC                 THEN CONCAT(
# MAGIC                     'Coupon redemption Silver mutabakat farkı: ',
# MAGIC                     CAST(
# MAGIC                         (SELECT DifferenceCount FROM coupon_redemption_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     )
# MAGIC                 )
# MAGIC             WHEN (
# MAGIC                 SELECT RejectedRowCount + DuplicateRowCount
# MAGIC                 FROM coupon_redemption_reconciliation
# MAGIC             ) > 0
# MAGIC                 THEN CONCAT(
# MAGIC                     'Rejected=',
# MAGIC                     CAST(
# MAGIC                         (SELECT RejectedRowCount FROM coupon_redemption_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     ),
# MAGIC                     ', Duplicate=',
# MAGIC                     CAST(
# MAGIC                         (SELECT DuplicateRowCount FROM coupon_redemption_reconciliation)
# MAGIC                         AS STRING
# MAGIC                     )
# MAGIC                 )
# MAGIC             ELSE NULL
# MAGIC         END,
# MAGIC
# MAGIC     ETLTime = current_timestamp()
# MAGIC WHERE ETLLogID = CONCAT(TRIM(:BatchID), '_SILVER_COUPON_REDEMPTION')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Sonuç ve log kontrolleri
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'transaction_clean' AS TableName,
# MAGIC     COUNT(*) AS RowCount,
# MAGIC     COUNT(DISTINCT RecordHash) AS DistinctRecordCount
# MAGIC FROM retail_marketing.silver.transaction_clean
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'coupon_redemption_clean',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT RecordHash)
# MAGIC FROM retail_marketing.silver.coupon_redemption_clean
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SourceName,
# MAGIC     TargetTableName,
# MAGIC     LoadStrategy,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     RejectedRowCount,
# MAGIC     UnchangedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'SILVER'
# MAGIC   AND SourceName IN ('transaction_data_raw', 'coupon_redemption_raw')
# MAGIC ORDER BY SourceName
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     basket_id,
# MAGIC     product_id,
# MAGIC     day_no,
# MAGIC     quantity,
# MAGIC     sales_value,
# MAGIC     net_sales_value,
# MAGIC     coupon_disc,
# MAGIC     coupon_match_disc,
# MAGIC     has_coupon,
# MAGIC     SourceBatchID
# MAGIC FROM retail_marketing.silver.transaction_clean
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ORDER BY basket_id, product_id
# MAGIC LIMIT 30
# MAGIC