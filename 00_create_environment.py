# Databricks notebook source
# MAGIC %md
# MAGIC # 51_finalize_batch — Working Version
# MAGIC
# MAGIC Bu notebook ETL batch'ini kalite ve yükleme sonuçlarına göre kapatır.
# MAGIC
# MAGIC Batch sonucu:
# MAGIC
# MAGIC - Kritik hata yoksa `SUCCESS`
# MAGIC - Kritik hata varsa `FAILED`
# MAGIC - Kritik hata yok ama uyarı varsa `SUCCESS_WITH_WARNING`
# MAGIC
# MAGIC Kontrol edilen katmanlar:
# MAGIC
# MAGIC - BRONZE
# MAGIC - SILVER
# MAGIC - DWH
# MAGIC - DM
# MAGIC
# MAGIC Aynı `BatchID` için tekrar çalıştırılabilir.
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
# MAGIC     TRIM(:BatchID) AS BatchID
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch mevcut mu?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ProcessDay,
# MAGIC     ProcessWeek,
# MAGIC     LoadMode,
# MAGIC     BatchStatus,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     ErrorMessage
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Katman bazlı ETL log özeti
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW batch_etl_summary AS
# MAGIC SELECT
# MAGIC     LayerName,
# MAGIC     COUNT(*) AS TotalLoadCount,
# MAGIC     SUM(CASE WHEN LoadStatus = 'SUCCESS' THEN 1 ELSE 0 END) AS SuccessCount,
# MAGIC     SUM(CASE WHEN LoadStatus = 'SUCCESS_WITH_WARNING' THEN 1 ELSE 0 END) AS WarningCount,
# MAGIC     SUM(CASE WHEN LoadStatus = 'SKIPPED' THEN 1 ELSE 0 END) AS SkippedCount,
# MAGIC     SUM(CASE WHEN LoadStatus = 'FAILED' THEN 1 ELSE 0 END) AS FailedCount,
# MAGIC     SUM(COALESCE(SourceRowCount, 0)) AS SourceRowCount,
# MAGIC     SUM(COALESCE(InsertedRowCount, 0)) AS InsertedRowCount,
# MAGIC     SUM(COALESCE(RejectedRowCount, 0)) AS RejectedRowCount,
# MAGIC     SUM(COALESCE(UnchangedRowCount, 0)) AS UnchangedRowCount
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC GROUP BY LayerName
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM batch_etl_summary
# MAGIC ORDER BY
# MAGIC     CASE LayerName
# MAGIC         WHEN 'BRONZE' THEN 1
# MAGIC         WHEN 'SILVER' THEN 2
# MAGIC         WHEN 'DWH' THEN 3
# MAGIC         WHEN 'DM' THEN 4
# MAGIC         ELSE 5
# MAGIC     END
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Katman bazlı Data Quality özeti
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW batch_dq_summary AS
# MAGIC SELECT
# MAGIC     LayerName,
# MAGIC     COUNT(*) AS TotalCheckCount,
# MAGIC     SUM(CASE WHEN CheckStatus = 'PASS' THEN 1 ELSE 0 END) AS PassCount,
# MAGIC     SUM(CASE WHEN CheckStatus = 'WARN' THEN 1 ELSE 0 END) AS WarnCount,
# MAGIC     SUM(CASE WHEN CheckStatus = 'FAIL' THEN 1 ELSE 0 END) AS FailCount,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN CheckStatus = 'FAIL'
# MAGIC              AND SeverityLevel IN ('ERROR', 'CRITICAL')
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS CriticalFailCount
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC GROUP BY LayerName
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM batch_dq_summary
# MAGIC ORDER BY
# MAGIC     CASE LayerName
# MAGIC         WHEN 'BRONZE' THEN 1
# MAGIC         WHEN 'SILVER' THEN 2
# MAGIC         WHEN 'DWH' THEN 3
# MAGIC         WHEN 'DM' THEN 4
# MAGIC         ELSE 5
# MAGIC     END
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Batch final durumunu hesapla
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW batch_final_status AS
# MAGIC SELECT
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT SUM(FailedCount)
# MAGIC         FROM batch_etl_summary
# MAGIC     ), 0) AS FailedLoadCount,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT SUM(WarningCount)
# MAGIC         FROM batch_etl_summary
# MAGIC     ), 0) AS WarningLoadCount,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT SUM(CriticalFailCount)
# MAGIC         FROM batch_dq_summary
# MAGIC     ), 0) AS CriticalDQFailCount,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT SUM(WarnCount)
# MAGIC         FROM batch_dq_summary
# MAGIC     ), 0) AS DQWarningCount,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT COUNT(DISTINCT LayerName)
# MAGIC         FROM retail_marketing.audit.etl_table_load_log
# MAGIC         WHERE BatchID = TRIM(:BatchID)
# MAGIC           AND LayerName IN ('BRONZE', 'SILVER', 'DWH', 'DM')
# MAGIC     ), 0) AS CompletedLoadLayerCount,
# MAGIC
# MAGIC     COALESCE((
# MAGIC         SELECT COUNT(DISTINCT LayerName)
# MAGIC         FROM retail_marketing.audit.data_quality_log
# MAGIC         WHERE BatchID = TRIM(:BatchID)
# MAGIC           AND LayerName IN ('BRONZE', 'SILVER', 'DWH', 'DM')
# MAGIC     ), 0) AS CompletedDQLayerCount
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN CompletedLoadLayerCount < 4 THEN 'FAILED'
# MAGIC         WHEN CompletedDQLayerCount < 4 THEN 'FAILED'
# MAGIC         WHEN FailedLoadCount > 0 THEN 'FAILED'
# MAGIC         WHEN CriticalDQFailCount > 0 THEN 'FAILED'
# MAGIC         WHEN WarningLoadCount > 0 OR DQWarningCount > 0
# MAGIC             THEN 'SUCCESS_WITH_WARNING'
# MAGIC         ELSE 'SUCCESS'
# MAGIC     END AS CalculatedBatchStatus
# MAGIC FROM batch_final_status
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch kontrol tablosunu kapat
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE retail_marketing.control.etl_batch_control
# MAGIC SET
# MAGIC     BatchStatus = (
# MAGIC         SELECT
# MAGIC             CASE
# MAGIC                 WHEN CompletedLoadLayerCount < 4 THEN 'FAILED'
# MAGIC                 WHEN CompletedDQLayerCount < 4 THEN 'FAILED'
# MAGIC                 WHEN FailedLoadCount > 0 THEN 'FAILED'
# MAGIC                 WHEN CriticalDQFailCount > 0 THEN 'FAILED'
# MAGIC                 WHEN WarningLoadCount > 0 OR DQWarningCount > 0
# MAGIC                     THEN 'SUCCESS_WITH_WARNING'
# MAGIC                 ELSE 'SUCCESS'
# MAGIC             END
# MAGIC         FROM batch_final_status
# MAGIC     ),
# MAGIC
# MAGIC     EndTime = current_timestamp(),
# MAGIC
# MAGIC     ErrorMessage = (
# MAGIC         SELECT
# MAGIC             CASE
# MAGIC                 WHEN CompletedLoadLayerCount < 4
# MAGIC                     THEN CONCAT(
# MAGIC                         'Eksik ETL katmanı var. Tamamlanan katman sayısı=',
# MAGIC                         CAST(CompletedLoadLayerCount AS STRING),
# MAGIC                         '/4'
# MAGIC                     )
# MAGIC
# MAGIC                 WHEN CompletedDQLayerCount < 4
# MAGIC                     THEN CONCAT(
# MAGIC                         'Eksik DQ katmanı var. Tamamlanan katman sayısı=',
# MAGIC                         CAST(CompletedDQLayerCount AS STRING),
# MAGIC                         '/4'
# MAGIC                     )
# MAGIC
# MAGIC                 WHEN FailedLoadCount > 0
# MAGIC                     THEN CONCAT(
# MAGIC                         'Başarısız ETL yükleme sayısı=',
# MAGIC                         CAST(FailedLoadCount AS STRING)
# MAGIC                     )
# MAGIC
# MAGIC                 WHEN CriticalDQFailCount > 0
# MAGIC                     THEN CONCAT(
# MAGIC                         'Kritik Data Quality hata sayısı=',
# MAGIC                         CAST(CriticalDQFailCount AS STRING)
# MAGIC                     )
# MAGIC
# MAGIC                 WHEN WarningLoadCount > 0 OR DQWarningCount > 0
# MAGIC                     THEN CONCAT(
# MAGIC                         'Batch uyarı ile tamamlandı. ETL warning=',
# MAGIC                         CAST(WarningLoadCount AS STRING),
# MAGIC                         ', DQ warning=',
# MAGIC                         CAST(DQWarningCount AS STRING)
# MAGIC                     )
# MAGIC
# MAGIC                 ELSE NULL
# MAGIC             END
# MAGIC         FROM batch_final_status
# MAGIC     ),
# MAGIC
# MAGIC     ETLTime = current_timestamp()
# MAGIC
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Final batch sonucu
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     ProcessDate,
# MAGIC     ProcessDay,
# MAGIC     ProcessWeek,
# MAGIC     LoadMode,
# MAGIC     BatchStatus,
# MAGIC     StartTime,
# MAGIC     EndTime,
# MAGIC     TIMESTAMPDIFF(SECOND, StartTime, EndTime) AS DurationSeconds,
# MAGIC     TriggerType,
# MAGIC     TriggeredBy,
# MAGIC     ErrorMessage,
# MAGIC     ETLTime
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Hatalı ETL kayıtlarını göster
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     LayerName,
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
# MAGIC   AND LoadStatus = 'FAILED'
# MAGIC ORDER BY LayerName, SourceName
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Kritik Data Quality hatalarını göster
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     LayerName,
# MAGIC     TableName,
# MAGIC     CheckName,
# MAGIC     CheckCategory,
# MAGIC     SeverityLevel,
# MAGIC     CheckedRowCount,
# MAGIC     FailedRowCount,
# MAGIC     FailureRate,
# MAGIC     CheckStatus,
# MAGIC     CheckDescription
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND CheckStatus = 'FAIL'
# MAGIC   AND SeverityLevel IN ('ERROR', 'CRITICAL')
# MAGIC ORDER BY LayerName, TableName, CheckName
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Batch genel özeti
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     b.BatchID,
# MAGIC     b.BatchStatus,
# MAGIC     b.ProcessDate,
# MAGIC     b.ProcessDay,
# MAGIC     b.ProcessWeek,
# MAGIC
# MAGIC     COALESCE(loads.TotalLoadCount, 0) AS TotalLoadCount,
# MAGIC     COALESCE(loads.SuccessCount, 0) AS SuccessLoadCount,
# MAGIC     COALESCE(loads.WarningCount, 0) AS WarningLoadCount,
# MAGIC     COALESCE(loads.SkippedCount, 0) AS SkippedLoadCount,
# MAGIC     COALESCE(loads.FailedCount, 0) AS FailedLoadCount,
# MAGIC
# MAGIC     COALESCE(dq.TotalCheckCount, 0) AS TotalDQCheckCount,
# MAGIC     COALESCE(dq.PassCount, 0) AS PassedDQCount,
# MAGIC     COALESCE(dq.WarnCount, 0) AS WarningDQCount,
# MAGIC     COALESCE(dq.FailCount, 0) AS FailedDQCount,
# MAGIC     COALESCE(dq.CriticalFailCount, 0) AS CriticalDQFailCount,
# MAGIC
# MAGIC     b.StartTime,
# MAGIC     b.EndTime,
# MAGIC     TIMESTAMPDIFF(SECOND, b.StartTime, b.EndTime) AS DurationSeconds,
# MAGIC     b.ErrorMessage
# MAGIC
# MAGIC FROM retail_marketing.control.etl_batch_control b
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT
# MAGIC         BatchID,
# MAGIC         COUNT(*) AS TotalLoadCount,
# MAGIC         SUM(CASE WHEN LoadStatus = 'SUCCESS' THEN 1 ELSE 0 END) AS SuccessCount,
# MAGIC         SUM(CASE WHEN LoadStatus = 'SUCCESS_WITH_WARNING' THEN 1 ELSE 0 END) AS WarningCount,
# MAGIC         SUM(CASE WHEN LoadStatus = 'SKIPPED' THEN 1 ELSE 0 END) AS SkippedCount,
# MAGIC         SUM(CASE WHEN LoadStatus = 'FAILED' THEN 1 ELSE 0 END) AS FailedCount
# MAGIC     FROM retail_marketing.audit.etl_table_load_log
# MAGIC     GROUP BY BatchID
# MAGIC ) loads
# MAGIC     ON loads.BatchID = b.BatchID
# MAGIC
# MAGIC LEFT JOIN (
# MAGIC     SELECT
# MAGIC         BatchID,
# MAGIC         COUNT(*) AS TotalCheckCount,
# MAGIC         SUM(CASE WHEN CheckStatus = 'PASS' THEN 1 ELSE 0 END) AS PassCount,
# MAGIC         SUM(CASE WHEN CheckStatus = 'WARN' THEN 1 ELSE 0 END) AS WarnCount,
# MAGIC         SUM(CASE WHEN CheckStatus = 'FAIL' THEN 1 ELSE 0 END) AS FailCount,
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN CheckStatus = 'FAIL'
# MAGIC                  AND SeverityLevel IN ('ERROR', 'CRITICAL')
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS CriticalFailCount
# MAGIC     FROM retail_marketing.audit.data_quality_log
# MAGIC     GROUP BY BatchID
# MAGIC ) dq
# MAGIC     ON dq.BatchID = b.BatchID
# MAGIC
# MAGIC WHERE b.BatchID = TRIM(:BatchID)
# MAGIC