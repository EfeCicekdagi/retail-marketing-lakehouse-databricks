# Databricks notebook source
# MAGIC %md
# MAGIC # 32_validate_dwh — Working Version
# MAGIC
# MAGIC Bu notebook seçilen batch için DWH katmanını doğrular ve sonuçları
# MAGIC `retail_marketing.audit.data_quality_log` tablosuna yazar.
# MAGIC
# MAGIC Kontroller:
# MAGIC
# MAGIC - Dimension tablolarının doluluğu
# MAGIC - Fact tablolarının doluluğu
# MAGIC - Dimension natural key benzersizliği
# MAGIC - Fact grain benzersizliği
# MAGIC - Surrogate key referential integrity
# MAGIC - Unknown member kullanımı
# MAGIC - Fact kaynak-hedef satır mutabakatı
# MAGIC - DWH yükleme loglarının durumu
# MAGIC
# MAGIC Parametreler:
# MAGIC
# MAGIC - `BatchID`
# MAGIC - `ProcessDay`
# MAGIC - `ProcessWeek`
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
# MAGIC     CAST(:ProcessDay AS DECIMAL(10,0)) AS ProcessDay,
# MAGIC     CAST(:ProcessWeek AS DECIMAL(10,0)) AS ProcessWeek
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch kontrolü
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
# MAGIC     EndTime
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aynı batch'e ait eski DWH kalite loglarını temizle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DWH'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DWH kalite sonuçlarını hazırla
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dwh_dq_results AS
# MAGIC
# MAGIC -- 1) Product dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_PRODUCT_NOT_EMPTY') AS DQLogID,
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC     'DWH' AS LayerName,
# MAGIC     'retail_marketing.dwh.dim_product' AS TableName,
# MAGIC     'dim_product_not_empty' AS CheckName,
# MAGIC     'RECONCILIATION' AS CheckCategory,
# MAGIC     'CRITICAL' AS SeverityLevel,
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)) AS CheckedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)) AS PassedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)) AS FailedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)) AS FailureRate,
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END AS CheckStatus,
# MAGIC     'dim_product yalnızca unknown kaydından oluşmamalıdır.' AS CheckDescription
# MAGIC FROM retail_marketing.dwh.dim_product
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2) Household dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_HOUSEHOLD_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_household',
# MAGIC     'dim_household_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_household yalnızca unknown kaydından oluşmamalıdır.'
# MAGIC FROM retail_marketing.dwh.dim_household
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3) Campaign dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_CAMPAIGN_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_campaign',
# MAGIC     'dim_campaign_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_campaign yalnızca unknown kaydından oluşmamalıdır.'
# MAGIC FROM retail_marketing.dwh.dim_campaign
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4) Store dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_STORE_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_store',
# MAGIC     'dim_store_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_store yalnızca unknown kaydından oluşmamalıdır.'
# MAGIC FROM retail_marketing.dwh.dim_store
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 5) Coupon dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_COUPON_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_coupon',
# MAGIC     'dim_coupon_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_coupon yalnızca unknown kaydından oluşmamalıdır.'
# MAGIC FROM retail_marketing.dwh.dim_coupon
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 6) Day dimension dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_DAY_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_day',
# MAGIC     'dim_day_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 1 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 1 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_day yalnızca unknown kaydından oluşmamalıdır.'
# MAGIC FROM retail_marketing.dwh.dim_day
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 7) Sales fact dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_SALES_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_sales_transaction',
# MAGIC     'fact_sales_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch için fact_sales_transaction boş olmamalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 8) Campaign target fact dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_CAMPAIGN_TARGET_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_campaign_target',
# MAGIC     'fact_campaign_target_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch için fact_campaign_target boş olmamalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_campaign_target
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 9) Promotion weekly fact dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_PROMOTION_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_promotion_weekly',
# MAGIC     'fact_promotion_weekly_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch ve hafta için fact_promotion_weekly boş olmamalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_promotion_weekly
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC   AND WeekNumber = CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 10) Product natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_PRODUCT_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_product',
# MAGIC     'product_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_product WHERE ProductKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_product içinde ProductID benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT ProductID
# MAGIC     FROM retail_marketing.dwh.dim_product
# MAGIC     WHERE ProductKey <> -1
# MAGIC     GROUP BY ProductID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 11) Household natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_HOUSEHOLD_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_household',
# MAGIC     'household_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_household WHERE HouseholdKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_household içinde HouseholdID benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT HouseholdID
# MAGIC     FROM retail_marketing.dwh.dim_household
# MAGIC     WHERE HouseholdKey <> -1
# MAGIC     GROUP BY HouseholdID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 12) Campaign natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_CAMPAIGN_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_campaign',
# MAGIC     'campaign_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_campaign WHERE CampaignKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_campaign içinde CampaignID benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT CampaignID
# MAGIC     FROM retail_marketing.dwh.dim_campaign
# MAGIC     WHERE CampaignKey <> -1
# MAGIC     GROUP BY CampaignID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 13) Store natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_STORE_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_store',
# MAGIC     'store_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_store WHERE StoreKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_store içinde StoreID benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT StoreID
# MAGIC     FROM retail_marketing.dwh.dim_store
# MAGIC     WHERE StoreKey <> -1
# MAGIC     GROUP BY StoreID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 14) Coupon natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_COUPON_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_coupon',
# MAGIC     'coupon_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_coupon WHERE CouponKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_coupon içinde CouponUPC benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT CouponUPC
# MAGIC     FROM retail_marketing.dwh.dim_coupon
# MAGIC     WHERE CouponKey <> -1
# MAGIC     GROUP BY CouponUPC
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 15) Day natural key unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_DIM_DAY_NK_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.dim_day',
# MAGIC     'day_natural_key_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.dim_day WHERE DayKey <> -1) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'dim_day içinde DayNumber benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT DayNumber
# MAGIC     FROM retail_marketing.dwh.dim_day
# MAGIC     WHERE DayKey <> -1
# MAGIC     GROUP BY DayNumber
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 16) Sales fact grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_SALES_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_sales_transaction',
# MAGIC     'sales_fact_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.fact_sales_transaction WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Sales fact grain için RecordHash benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT RecordHash
# MAGIC     FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY RecordHash
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 17) Coupon redemption fact grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_COUPON_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_coupon_redemption',
# MAGIC     'coupon_redemption_fact_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.fact_coupon_redemption WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Coupon redemption fact grain için RecordHash benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT RecordHash
# MAGIC     FROM retail_marketing.dwh.fact_coupon_redemption
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY RecordHash
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 18) Campaign target fact grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_CAMPAIGN_TARGET_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_campaign_target',
# MAGIC     'campaign_target_fact_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.fact_campaign_target WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Campaign target fact grain için RecordHash benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT RecordHash
# MAGIC     FROM retail_marketing.dwh.fact_campaign_target
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY RecordHash
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 19) Promotion weekly fact grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_PROMOTION_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_promotion_weekly',
# MAGIC     'promotion_weekly_fact_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dwh.fact_promotion_weekly WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Promotion weekly fact grain için RecordHash benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT RecordHash
# MAGIC     FROM retail_marketing.dwh.fact_promotion_weekly
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY RecordHash
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 20) Sales fact dimension RI
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_SALES_RI'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_sales_transaction',
# MAGIC     'sales_fact_dimension_referential_integrity',
# MAGIC     'REFERENTIAL_INTEGRITY', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR h.HouseholdKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR h.HouseholdKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR h.HouseholdKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR h.HouseholdKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Sales fact surrogate key değerleri ilgili dimension tablosunda bulunmalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction f
# MAGIC LEFT JOIN retail_marketing.dwh.dim_product p
# MAGIC     ON p.ProductKey = f.ProductKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_household h
# MAGIC     ON h.HouseholdKey = f.HouseholdKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_store s
# MAGIC     ON s.StoreKey = f.StoreKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_day d
# MAGIC     ON d.DayKey = f.DayKey
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 21) Coupon fact dimension RI
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_COUPON_RI'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_coupon_redemption',
# MAGIC     'coupon_fact_dimension_referential_integrity',
# MAGIC     'REFERENTIAL_INTEGRITY', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CouponKey IS NULL
# MAGIC               OR cam.CampaignKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CouponKey IS NULL
# MAGIC               OR cam.CampaignKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CouponKey IS NULL
# MAGIC               OR cam.CampaignKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CouponKey IS NULL
# MAGIC               OR cam.CampaignKey IS NULL
# MAGIC               OR d.DayKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Coupon redemption fact surrogate key değerleri ilgili dimension tablosunda bulunmalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_coupon_redemption f
# MAGIC LEFT JOIN retail_marketing.dwh.dim_household h
# MAGIC     ON h.HouseholdKey = f.HouseholdKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_coupon c
# MAGIC     ON c.CouponKey = f.CouponKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_campaign cam
# MAGIC     ON cam.CampaignKey = f.CampaignKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_day d
# MAGIC     ON d.DayKey = f.DayKey
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 22) Campaign target fact RI
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_CAMPAIGN_TARGET_RI'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_campaign_target',
# MAGIC     'campaign_target_fact_dimension_referential_integrity',
# MAGIC     'REFERENTIAL_INTEGRITY', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CampaignKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CampaignKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CampaignKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN h.HouseholdKey IS NULL
# MAGIC               OR c.CampaignKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Campaign target fact surrogate key değerleri ilgili dimension tablosunda bulunmalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_campaign_target f
# MAGIC LEFT JOIN retail_marketing.dwh.dim_household h
# MAGIC     ON h.HouseholdKey = f.HouseholdKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_campaign c
# MAGIC     ON c.CampaignKey = f.CampaignKey
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 23) Promotion fact RI
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_PROMOTION_RI'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_promotion_weekly',
# MAGIC     'promotion_fact_dimension_referential_integrity',
# MAGIC     'REFERENTIAL_INTEGRITY', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN p.ProductKey IS NULL
# MAGIC               OR s.StoreKey IS NULL
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Promotion fact surrogate key değerleri ilgili dimension tablosunda bulunmalıdır.'
# MAGIC FROM retail_marketing.dwh.fact_promotion_weekly f
# MAGIC LEFT JOIN retail_marketing.dwh.dim_product p
# MAGIC     ON p.ProductKey = f.ProductKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_store s
# MAGIC     ON s.StoreKey = f.StoreKey
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 24) Sales fact unknown key oranı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_FACT_SALES_UNKNOWN_KEYS'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_sales_transaction',
# MAGIC     'sales_fact_unknown_key_usage',
# MAGIC     'REFERENTIAL_INTEGRITY', 'WARNING',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN ProductKey = -1
# MAGIC               OR HouseholdKey = -1
# MAGIC               OR StoreKey = -1
# MAGIC               OR DayKey = -1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN ProductKey = -1
# MAGIC               OR HouseholdKey = -1
# MAGIC               OR StoreKey = -1
# MAGIC               OR DayKey = -1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN ProductKey = -1
# MAGIC               OR HouseholdKey = -1
# MAGIC               OR StoreKey = -1
# MAGIC               OR DayKey = -1
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN ProductKey = -1
# MAGIC               OR HouseholdKey = -1
# MAGIC               OR StoreKey = -1
# MAGIC               OR DayKey = -1
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'WARN'
# MAGIC     END,
# MAGIC     'Sales fact içindeki unknown surrogate key kullanımı gözlemlenir.'
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 25) Silver -> Sales fact mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_SALES_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_sales_transaction',
# MAGIC     'silver_to_sales_fact_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(source.SourceRowCount AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN source.SourceRowCount = target.TargetRowCount THEN source.SourceRowCount ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(ABS(source.SourceRowCount - target.TargetRowCount) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN source.SourceRowCount = 0 THEN 0
# MAGIC         ELSE ABS(source.SourceRowCount - target.TargetRowCount) / source.SourceRowCount END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE WHEN source.SourceRowCount = target.TargetRowCount THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Silver transaction satır sayısı ile sales fact satır sayısı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS SourceRowCount
# MAGIC     FROM retail_marketing.silver.transaction_clean
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC       AND day_no = CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS TargetRowCount
# MAGIC     FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 26) Silver -> Campaign target fact mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_CAMPAIGN_TARGET_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_campaign_target',
# MAGIC     'silver_to_campaign_target_fact_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(source.SourceRowCount AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN source.SourceRowCount = target.TargetRowCount THEN source.SourceRowCount ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(ABS(source.SourceRowCount - target.TargetRowCount) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN source.SourceRowCount = 0 THEN 0
# MAGIC         ELSE ABS(source.SourceRowCount - target.TargetRowCount) / source.SourceRowCount END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE WHEN source.SourceRowCount = target.TargetRowCount THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Silver campaign target satır sayısı ile fact satır sayısı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS SourceRowCount
# MAGIC     FROM retail_marketing.silver.campaign_target_clean
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS TargetRowCount
# MAGIC     FROM retail_marketing.dwh.fact_campaign_target
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 27) Silver -> Promotion fact mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_PROMOTION_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.dwh.fact_promotion_weekly',
# MAGIC     'silver_to_promotion_fact_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(source.SourceRowCount AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN source.SourceRowCount = target.TargetRowCount THEN source.SourceRowCount ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(ABS(source.SourceRowCount - target.TargetRowCount) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN source.SourceRowCount = 0 THEN 0
# MAGIC         ELSE ABS(source.SourceRowCount - target.TargetRowCount) / source.SourceRowCount END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE WHEN source.SourceRowCount = target.TargetRowCount THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Silver promotion weekly satır sayısı ile fact satır sayısı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS SourceRowCount
# MAGIC     FROM retail_marketing.silver.promotion_weekly_clean
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC       AND week_no = CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COUNT(*) AS TargetRowCount
# MAGIC     FROM retail_marketing.dwh.fact_promotion_weekly
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 28) DWH load log durumu
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DWH_LOAD_STATUS'),
# MAGIC     TRIM(:BatchID), 'DWH',
# MAGIC     'retail_marketing.audit.etl_table_load_log',
# MAGIC     'dwh_load_status_check',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN LoadStatus IN ('SUCCESS', 'SUCCESS_WITH_WARNING', 'SKIPPED')
# MAGIC             THEN 0 ELSE 1 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 1
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN LoadStatus IN ('SUCCESS', 'SUCCESS_WITH_WARNING', 'SKIPPED')
# MAGIC             THEN 0 ELSE 1 END), 0) END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 1
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN LoadStatus IN ('SUCCESS', 'SUCCESS_WITH_WARNING', 'SKIPPED')
# MAGIC             THEN 0 ELSE 1 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COUNT(*) = 0 THEN 'FAIL'
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN LoadStatus IN ('SUCCESS', 'SUCCESS_WITH_WARNING', 'SKIPPED')
# MAGIC             THEN 0 ELSE 1 END), 0) = 0
# MAGIC         THEN 'PASS'
# MAGIC         ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'DWH yükleme logları SUCCESS, SUCCESS_WITH_WARNING veya SKIPPED olmalıdır.'
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DWH'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sonuçları audit tablosuna yaz
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.audit.data_quality_log (
# MAGIC     DQLogID,
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     TableName,
# MAGIC     CheckName,
# MAGIC     CheckCategory,
# MAGIC     SeverityLevel,
# MAGIC     CheckedRowCount,
# MAGIC     PassedRowCount,
# MAGIC     FailedRowCount,
# MAGIC     FailureRate,
# MAGIC     CheckStatus,
# MAGIC     CheckDescription,
# MAGIC     ETLTime
# MAGIC )
# MAGIC SELECT
# MAGIC     DQLogID,
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     TableName,
# MAGIC     CheckName,
# MAGIC     CheckCategory,
# MAGIC     SeverityLevel,
# MAGIC     CheckedRowCount,
# MAGIC     PassedRowCount,
# MAGIC     FailedRowCount,
# MAGIC     FailureRate,
# MAGIC     CheckStatus,
# MAGIC     CheckDescription,
# MAGIC     current_timestamp()
# MAGIC FROM dwh_dq_results
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Detaylı DWH kalite sonuçları
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CheckName,
# MAGIC     TableName,
# MAGIC     CheckCategory,
# MAGIC     SeverityLevel,
# MAGIC     CheckedRowCount,
# MAGIC     PassedRowCount,
# MAGIC     FailedRowCount,
# MAGIC     FailureRate,
# MAGIC     CheckStatus,
# MAGIC     CheckDescription
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DWH'
# MAGIC ORDER BY
# MAGIC     CASE CheckStatus
# MAGIC         WHEN 'FAIL' THEN 1
# MAGIC         WHEN 'WARN' THEN 2
# MAGIC         WHEN 'PASS' THEN 3
# MAGIC         ELSE 4
# MAGIC     END,
# MAGIC     TableName,
# MAGIC     CheckName
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Özet
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CheckStatus,
# MAGIC     COUNT(*) AS CheckCount
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DWH'
# MAGIC GROUP BY CheckStatus
# MAGIC ORDER BY CheckStatus
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS CriticalFailureCount
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DWH'
# MAGIC   AND CheckStatus = 'FAIL'
# MAGIC   AND SeverityLevel IN ('ERROR', 'CRITICAL')
# MAGIC