# Databricks notebook source
# MAGIC %md
# MAGIC # 41_validate_dm_marketing — Working Version
# MAGIC
# MAGIC Bu notebook pazarlama Data Mart katmanını doğrular ve sonuçları
# MAGIC `retail_marketing.audit.data_quality_log` tablosuna yazar.
# MAGIC
# MAGIC Kontroller:
# MAGIC
# MAGIC - Dört Data Mart tablosunun doluluğu
# MAGIC - Gün ve hafta parametre uyumu
# MAGIC - Data Mart grain benzersizliği
# MAGIC - Oran ve tutar alanlarının mantıksal sınırları
# MAGIC - DWH → Data Mart mutabakatı
# MAGIC - Data Mart yükleme loglarının durumu
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
# MAGIC     BatchStatus
# MAGIC FROM retail_marketing.control.etl_batch_control
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aynı batch'e ait eski DM kalite loglarını temizle
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DM'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Mart kalite sonuçlarını hazırla
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dm_dq_results AS
# MAGIC
# MAGIC -- 1) Daily sales dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_DAILY_SALES_NOT_EMPTY') AS DQLogID,
# MAGIC     TRIM(:BatchID) AS BatchID,
# MAGIC     'DM' AS LayerName,
# MAGIC     'retail_marketing.dm_marketing.dm_daily_sales' AS TableName,
# MAGIC     'dm_daily_sales_not_empty' AS CheckName,
# MAGIC     'RECONCILIATION' AS CheckCategory,
# MAGIC     'CRITICAL' AS SeverityLevel,
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)) AS CheckedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)) AS PassedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)) AS FailedRowCount,
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)) AS FailureRate,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS CheckStatus,
# MAGIC     'İlgili batch için günlük satış Data Mart kaydı bulunmalıdır.' AS CheckDescription
# MAGIC FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2) Product sales dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PRODUCT_SALES_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_product_sales',
# MAGIC     'dm_product_sales_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch için ürün satış Data Mart kaydı bulunmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3) Campaign performance dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_CAMPAIGN_PERFORMANCE_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_campaign_performance',
# MAGIC     'dm_campaign_performance_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch için kampanya performans Data Mart kaydı bulunmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4) Promotion performance dolu olmalı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PROMOTION_PERFORMANCE_NOT_EMPTY'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_promotion_performance',
# MAGIC     'dm_promotion_performance_not_empty',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 0 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(20,0)),
# MAGIC     CAST(CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'İlgili batch ve hafta için promosyon performans Data Mart kaydı bulunmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC   AND WeekNumber = CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 5) Daily sales gün kontrolü
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_DAILY_SALES_DAY'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_daily_sales',
# MAGIC     'dm_daily_sales_process_day_check',
# MAGIC     'BUSINESS_RULE', 'ERROR',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN DayNumber IS NULL
# MAGIC               OR DayNumber <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN DayNumber IS NULL
# MAGIC               OR DayNumber <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN DayNumber IS NULL
# MAGIC               OR DayNumber <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN DayNumber IS NULL
# MAGIC               OR DayNumber <> CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'dm_daily_sales içindeki DayNumber, ProcessDay ile aynı olmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 6) Promotion hafta kontrolü
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PROMOTION_WEEK'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_promotion_performance',
# MAGIC     'dm_promotion_process_week_check',
# MAGIC     'BUSINESS_RULE', 'ERROR',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN WeekNumber IS NULL
# MAGIC               OR WeekNumber <> CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN WeekNumber IS NULL
# MAGIC               OR WeekNumber <> CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN WeekNumber IS NULL
# MAGIC               OR WeekNumber <> CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN WeekNumber IS NULL
# MAGIC               OR WeekNumber <> CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'dm_promotion_performance içindeki WeekNumber, ProcessWeek ile aynı olmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 7) Daily sales grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_DAILY_SALES_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_daily_sales',
# MAGIC     'dm_daily_sales_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dm_marketing.dm_daily_sales WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Daily sales grain için DayKey benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT DayKey
# MAGIC     FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY DayKey
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 8) Product sales grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PRODUCT_SALES_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_product_sales',
# MAGIC     'dm_product_sales_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dm_marketing.dm_product_sales WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Product sales grain için ProductKey benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT ProductKey
# MAGIC     FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY ProductKey
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 9) Campaign performance grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_CAMPAIGN_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_campaign_performance',
# MAGIC     'dm_campaign_performance_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dm_marketing.dm_campaign_performance WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Campaign performance grain için CampaignKey benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT CampaignKey
# MAGIC     FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY CampaignKey
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 10) Promotion grain unique
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PROMOTION_GRAIN_UNIQUE'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_promotion_performance',
# MAGIC     'dm_promotion_performance_grain_unique',
# MAGIC     'UNIQUENESS', 'CRITICAL',
# MAGIC     CAST((SELECT COUNT(*) FROM retail_marketing.dm_marketing.dm_promotion_performance WHERE SourceBatchID = TRIM(:BatchID)) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(20,0)),
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(NULL AS DECIMAL(18,6)),
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC     'Promotion performance grain için ProductKey + StoreKey + WeekNumber + HasDisplay + HasMailer benzersiz olmalıdır.'
# MAGIC FROM (
# MAGIC     SELECT ProductKey, StoreKey, WeekNumber, HasDisplay, HasMailer
# MAGIC     FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY ProductKey, StoreKey, WeekNumber, HasDisplay, HasMailer
# MAGIC     HAVING COUNT(*) > 1
# MAGIC ) d
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 11) Daily sales metric kuralları
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_DAILY_SALES_METRICS'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_daily_sales',
# MAGIC     'dm_daily_sales_metric_rules',
# MAGIC     'BUSINESS_RULE', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR ProductCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageBasketAmount < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR ProductCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageBasketAmount < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR ProductCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageBasketAmount < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR ProductCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageBasketAmount < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Daily sales ölçüleri negatif olmamalı; DiscountRate 0 ile 1 arasında olmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 12) Product sales metric kuralları
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PRODUCT_SALES_METRICS'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_product_sales',
# MAGIC     'dm_product_sales_metric_rules',
# MAGIC     'BUSINESS_RULE', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageUnitNetSales < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageUnitNetSales < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageUnitNetSales < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN BasketCount < 0
# MAGIC               OR HouseholdCount < 0
# MAGIC               OR StoreCount < 0
# MAGIC               OR TotalQuantity < 0
# MAGIC               OR GrossSalesAmount < 0
# MAGIC               OR TotalDiscountAmount < 0
# MAGIC               OR NetSalesAmount < 0
# MAGIC               OR AverageUnitNetSales < 0
# MAGIC               OR DiscountRate < 0
# MAGIC               OR DiscountRate > 1
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Product sales ölçüleri negatif olmamalı; DiscountRate 0 ile 1 arasında olmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 13) Campaign metric kuralları
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_CAMPAIGN_METRICS'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_campaign_performance',
# MAGIC     'dm_campaign_metric_rules',
# MAGIC     'BUSINESS_RULE', 'CRITICAL',
# MAGIC     CAST(COUNT(*) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         COUNT(*) - COALESCE(SUM(CASE
# MAGIC             WHEN TargetHouseholdCount < 0
# MAGIC               OR RedeemingHouseholdCount < 0
# MAGIC               OR RedemptionCount < 0
# MAGIC               OR DistinctCouponCount < 0
# MAGIC               OR RedemptionRate < 0
# MAGIC               OR RedemptionRate > 1
# MAGIC               OR RedeemingHouseholdCount > TargetHouseholdCount
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         COALESCE(SUM(CASE
# MAGIC             WHEN TargetHouseholdCount < 0
# MAGIC               OR RedeemingHouseholdCount < 0
# MAGIC               OR RedemptionCount < 0
# MAGIC               OR DistinctCouponCount < 0
# MAGIC               OR RedemptionRate < 0
# MAGIC               OR RedemptionRate > 1
# MAGIC               OR RedeemingHouseholdCount > TargetHouseholdCount
# MAGIC             THEN 1 ELSE 0 END), 0)
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN COUNT(*) = 0 THEN 0
# MAGIC         ELSE COALESCE(SUM(CASE
# MAGIC             WHEN TargetHouseholdCount < 0
# MAGIC               OR RedeemingHouseholdCount < 0
# MAGIC               OR RedemptionCount < 0
# MAGIC               OR DistinctCouponCount < 0
# MAGIC               OR RedemptionRate < 0
# MAGIC               OR RedemptionRate > 1
# MAGIC               OR RedeemingHouseholdCount > TargetHouseholdCount
# MAGIC             THEN 1 ELSE 0 END), 0) / COUNT(*) END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN COALESCE(SUM(CASE
# MAGIC             WHEN TargetHouseholdCount < 0
# MAGIC               OR RedeemingHouseholdCount < 0
# MAGIC               OR RedemptionCount < 0
# MAGIC               OR DistinctCouponCount < 0
# MAGIC               OR RedemptionRate < 0
# MAGIC               OR RedemptionRate > 1
# MAGIC               OR RedeemingHouseholdCount > TargetHouseholdCount
# MAGIC             THEN 1 ELSE 0 END), 0) = 0
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'Campaign ölçüleri negatif olmamalı; RedemptionRate 0 ile 1 arasında olmalıdır.'
# MAGIC FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 14) Daily sales net satış mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_DAILY_SALES_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_daily_sales',
# MAGIC     'dwh_to_dm_daily_sales_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(1 AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 1 ELSE 0 END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 0 ELSE 1 END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 0 ELSE 1 END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'DWH sales fact NetSalesAmount toplamı ile dm_daily_sales toplamı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COALESCE(SUM(NetSalesAmount), 0) AS SourceAmount
# MAGIC     FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COALESCE(SUM(NetSalesAmount), 0) AS TargetAmount
# MAGIC     FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 15) Product sales net satış mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_PRODUCT_SALES_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_product_sales',
# MAGIC     'dwh_to_dm_product_sales_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(1 AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 1 ELSE 0 END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 0 ELSE 1 END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(
# MAGIC         CASE WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 0 ELSE 1 END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN ABS(source.SourceAmount - target.TargetAmount) < 0.01
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'DWH sales fact NetSalesAmount toplamı ile dm_product_sales toplamı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COALESCE(SUM(NetSalesAmount), 0) AS SourceAmount
# MAGIC     FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COALESCE(SUM(NetSalesAmount), 0) AS TargetAmount
# MAGIC     FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 16) Campaign target mutabakatı
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_CAMPAIGN_TARGET_RECONCILIATION'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.dm_marketing.dm_campaign_performance',
# MAGIC     'dwh_to_dm_campaign_target_reconciliation',
# MAGIC     'RECONCILIATION', 'CRITICAL',
# MAGIC     CAST(source.SourceCount AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN source.SourceCount = target.TargetCount
# MAGIC         THEN source.SourceCount ELSE 0 END
# MAGIC         AS DECIMAL(20,0)
# MAGIC     ),
# MAGIC     CAST(ABS(source.SourceCount - target.TargetCount) AS DECIMAL(20,0)),
# MAGIC     CAST(
# MAGIC         CASE WHEN source.SourceCount = 0 THEN 0
# MAGIC         ELSE ABS(source.SourceCount - target.TargetCount) / source.SourceCount END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ),
# MAGIC     CASE
# MAGIC         WHEN source.SourceCount = target.TargetCount
# MAGIC         THEN 'PASS' ELSE 'FAIL'
# MAGIC     END,
# MAGIC     'fact_campaign_target toplamı ile DM TargetHouseholdCount toplamı eşleşmelidir.'
# MAGIC FROM (
# MAGIC     SELECT COUNT(*) AS SourceCount
# MAGIC     FROM retail_marketing.dwh.fact_campaign_target
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) source
# MAGIC CROSS JOIN (
# MAGIC     SELECT COALESCE(SUM(TargetHouseholdCount), 0) AS TargetCount
# MAGIC     FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ) target
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 17) DM load status
# MAGIC SELECT
# MAGIC     CONCAT(TRIM(:BatchID), '_DQ_DM_LOAD_STATUS'),
# MAGIC     TRIM(:BatchID), 'DM',
# MAGIC     'retail_marketing.audit.etl_table_load_log',
# MAGIC     'dm_load_status_check',
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
# MAGIC     'Data Mart yükleme logları SUCCESS, SUCCESS_WITH_WARNING veya SKIPPED olmalıdır.'
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DM'
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
# MAGIC FROM dm_dq_results
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Detaylı Data Mart kalite sonuçları
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
# MAGIC   AND LayerName = 'DM'
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
# MAGIC   AND LayerName = 'DM'
# MAGIC GROUP BY CheckStatus
# MAGIC ORDER BY CheckStatus
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS CriticalFailureCount
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DM'
# MAGIC   AND CheckStatus = 'FAIL'
# MAGIC   AND SeverityLevel IN ('ERROR', 'CRITICAL')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Başarı kriteri
# MAGIC
# MAGIC `CriticalFailureCount = 0` ise pazarlama Data Mart katmanı raporlama ve dashboard aşamasına hazırdır.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CampaignKey,
# MAGIC     HouseholdKey,
# MAGIC     COUNT(*) AS DuplicateCount
# MAGIC FROM retail_marketing.dwh.fact_campaign_target
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC GROUP BY
# MAGIC     CampaignKey,
# MAGIC     HouseholdKey
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY DuplicateCount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(DuplicateCount - 1) AS ExtraDuplicateRows
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         CampaignKey,
# MAGIC         HouseholdKey,
# MAGIC         COUNT(*) AS DuplicateCount
# MAGIC     FROM retail_marketing.dwh.fact_campaign_target
# MAGIC     WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC     GROUP BY
# MAGIC         CampaignKey,
# MAGIC         HouseholdKey
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );