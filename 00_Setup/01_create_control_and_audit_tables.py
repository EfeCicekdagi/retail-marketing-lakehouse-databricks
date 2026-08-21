# Databricks notebook source
# DBTITLE 1,Çalışma alanını seç
# MAGIC %sql
# MAGIC USE CATALOG retail_marketing;
# MAGIC USE SCHEMA control;
# MAGIC
# MAGIC SELECT
# MAGIC     current_catalog() AS CurrentCatalog,
# MAGIC     current_schema()  AS CurrentSchema;

# COMMAND ----------

# DBTITLE 1,Batch kontrol tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.control.etl_batch_control
# MAGIC (
# MAGIC     BatchID         STRING        NOT NULL,
# MAGIC     ProcessDate     DATE          NOT NULL,
# MAGIC     ProcessDay      DECIMAL(10,0),
# MAGIC     ProcessWeek     DECIMAL(10,0),
# MAGIC     LoadMode        STRING        NOT NULL,
# MAGIC     BatchStatus     STRING        NOT NULL,
# MAGIC     StartTime       TIMESTAMP     NOT NULL,
# MAGIC     EndTime         TIMESTAMP,
# MAGIC     TriggerType     STRING,
# MAGIC     TriggeredBy     STRING,
# MAGIC     ErrorMessage    STRING,
# MAGIC     ETLTime         TIMESTAMP     NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Her uçtan uca ETL çalışmasının başlangıç, bitiş ve durum bilgileri';

# COMMAND ----------

# DBTITLE 1,Watermark tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.control.source_watermark
# MAGIC (
# MAGIC     SourceName            STRING        NOT NULL,
# MAGIC     WatermarkType         STRING        NOT NULL,
# MAGIC     LastProcessValue      DECIMAL(20,0),
# MAGIC     LastProcessDate       DATE,
# MAGIC     LastSuccessfulBatchID STRING,
# MAGIC     LastSuccessTime       TIMESTAMP,
# MAGIC     IsActive              STRING        NOT NULL,
# MAGIC     ETLTime               TIMESTAMP     NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Kaynakların en son başarılı işlenen DAY, WEEK_NO veya snapshot bilgisi';

# COMMAND ----------

# DBTITLE 1,Başlangıç watermark kayıtları
# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.control.source_watermark AS target
# MAGIC
# MAGIC USING
# MAGIC (
# MAGIC     SELECT
# MAGIC         'transaction_data' AS SourceName,
# MAGIC         'DAY' AS WatermarkType,
# MAGIC         CAST(0 AS DECIMAL(20,0)) AS LastProcessValue
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'coupon_redemption',
# MAGIC         'DAY',
# MAGIC         CAST(0 AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'causal_data',
# MAGIC         'WEEK_NO',
# MAGIC         CAST(0 AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'product',
# MAGIC         'SNAPSHOT',
# MAGIC         CAST(NULL AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'household_demographic',
# MAGIC         'SNAPSHOT',
# MAGIC         CAST(NULL AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'campaign_desc',
# MAGIC         'SNAPSHOT',
# MAGIC         CAST(NULL AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'campaign_target',
# MAGIC         'SNAPSHOT',
# MAGIC         CAST(NULL AS DECIMAL(20,0))
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'coupon',
# MAGIC         'SNAPSHOT',
# MAGIC         CAST(NULL AS DECIMAL(20,0))
# MAGIC ) AS source
# MAGIC
# MAGIC ON target.SourceName = source.SourceName
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.WatermarkType = source.WatermarkType,
# MAGIC     target.IsActive      = 'Y',
# MAGIC     target.ETLTime       = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC (
# MAGIC     SourceName,
# MAGIC     WatermarkType,
# MAGIC     LastProcessValue,
# MAGIC     LastProcessDate,
# MAGIC     LastSuccessfulBatchID,
# MAGIC     LastSuccessTime,
# MAGIC     IsActive,
# MAGIC     ETLTime
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC     source.SourceName,
# MAGIC     source.WatermarkType,
# MAGIC     source.LastProcessValue,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'Y',
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Tablo yükleme logu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.audit.etl_table_load_log
# MAGIC (
# MAGIC     ETLLogID          STRING        NOT NULL,
# MAGIC     BatchID           STRING        NOT NULL,
# MAGIC     LayerName         STRING        NOT NULL,
# MAGIC     SourceName        STRING,
# MAGIC     TargetTableName   STRING        NOT NULL,
# MAGIC     LoadType          STRING        NOT NULL,
# MAGIC     ProcessDate       DATE,
# MAGIC     ProcessValue      DECIMAL(20,0),
# MAGIC     StartTime         TIMESTAMP     NOT NULL,
# MAGIC     EndTime           TIMESTAMP,
# MAGIC     SourceRowCount    DECIMAL(20,0),
# MAGIC     InsertedRowCount  DECIMAL(20,0),
# MAGIC     UpdatedRowCount   DECIMAL(20,0),
# MAGIC     RejectedRowCount  DECIMAL(20,0),
# MAGIC     UnchangedRowCount DECIMAL(20,0),
# MAGIC     LoadStatus        STRING        NOT NULL,
# MAGIC     ErrorMessage      STRING,
# MAGIC     ETLTime           TIMESTAMP     NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Tablo ve katman bazında ETL yükleme sonuçları';

# COMMAND ----------

# DBTITLE 1,Veri kalite logu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.audit.data_quality_log
# MAGIC (
# MAGIC     DQLogID          STRING        NOT NULL,
# MAGIC     BatchID          STRING        NOT NULL,
# MAGIC     LayerName        STRING        NOT NULL,
# MAGIC     TableName        STRING        NOT NULL,
# MAGIC     CheckName        STRING        NOT NULL,
# MAGIC     CheckCategory    STRING        NOT NULL,
# MAGIC     SeverityLevel    STRING        NOT NULL,
# MAGIC     CheckedRowCount  DECIMAL(20,0),
# MAGIC     PassedRowCount   DECIMAL(20,0),
# MAGIC     FailedRowCount   DECIMAL(20,0),
# MAGIC     FailureRate      DECIMAL(18,6),
# MAGIC     CheckStatus      STRING        NOT NULL,
# MAGIC     CheckDescription STRING,
# MAGIC     ETLTime          TIMESTAMP     NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze, Silver ve DWH katmanlarında çalıştırılan veri kalite kontrolleri';

# COMMAND ----------

# DBTITLE 1,Tabloları kontrol et
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_marketing.control;

# COMMAND ----------

# DBTITLE 1,Watermark kayıtlarını kontrol et
# MAGIC %sql
# MAGIC SELECT
# MAGIC     SourceName,
# MAGIC     WatermarkType,
# MAGIC     LastProcessValue,
# MAGIC     LastProcessDate,
# MAGIC     LastSuccessfulBatchID,
# MAGIC     LastSuccessTime,
# MAGIC     IsActive
# MAGIC FROM retail_marketing.control.source_watermark
# MAGIC ORDER BY SourceName;

# COMMAND ----------

# DBTITLE 1,Tablo şemalarını kontrol et
# MAGIC %sql
# MAGIC DESCRIBE TABLE retail_marketing.control.etl_batch_control;
# MAGIC DESCRIBE TABLE retail_marketing.control.source_watermark;
# MAGIC DESCRIBE TABLE retail_marketing.audit.etl_table_load_log;
# MAGIC DESCRIBE TABLE retail_marketing.audit.data_quality_log;

# COMMAND ----------

# DBTITLE 1,Log sistemini test et
# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.control.etl_batch_control
# MAGIC (
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
# MAGIC VALUES
# MAGIC (
# MAGIC     'SETUP_TEST_BATCH',
# MAGIC     current_date(),
# MAGIC     0,
# MAGIC     0,
# MAGIC     'INITIAL',
# MAGIC     'SUCCESS',
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     'MANUAL',
# MAGIC     'Efe',
# MAGIC     NULL,
# MAGIC     current_timestamp()
# MAGIC );