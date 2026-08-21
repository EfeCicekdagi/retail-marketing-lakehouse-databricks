# Databricks notebook source
# MAGIC %md
# MAGIC # 50_create_reporting_views — Working Version
# MAGIC
# MAGIC Bu notebook pazarlama Data Mart katmanı üzerinde dashboard ve raporlama için hazır view'lar oluşturur.
# MAGIC
# MAGIC Oluşturulan view'lar:
# MAGIC
# MAGIC - `dm_marketing.vw_daily_sales_kpi`
# MAGIC - `dm_marketing.vw_top_products`
# MAGIC - `dm_marketing.vw_campaign_performance`
# MAGIC - `dm_marketing.vw_promotion_performance`
# MAGIC - `dm_marketing.vw_etl_batch_summary`
# MAGIC - `dm_marketing.vw_data_quality_summary`
# MAGIC
# MAGIC Bu view'lar Databricks SQL Dashboard, Power BI veya benzeri BI araçlarında doğrudan kullanılabilir.
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

# MAGIC %md
# MAGIC ## 1. Günlük satış KPI view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_daily_sales_kpi AS
# MAGIC SELECT
# MAGIC     DayKey,
# MAGIC     DayNumber,
# MAGIC     WeekNumber,
# MAGIC     BasketCount,
# MAGIC     HouseholdCount,
# MAGIC     ProductCount,
# MAGIC     StoreCount,
# MAGIC     TotalQuantity,
# MAGIC     GrossSalesAmount,
# MAGIC     TotalDiscountAmount,
# MAGIC     CouponTransactionCount,
# MAGIC     AverageBasketAmount,
# MAGIC     DiscountRate,
# MAGIC     CASE
# MAGIC         WHEN BasketCount = 0 THEN 0
# MAGIC         ELSE CouponTransactionCount / BasketCount
# MAGIC     END AS CouponTransactionRate,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC     GrossSalesAmount - TotalDiscountAmount AS NetSalesAmount
# MAGIC FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ürün performans view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_top_products AS
# MAGIC SELECT
# MAGIC     ProductKey,
# MAGIC     ProductID,
# MAGIC     Department,
# MAGIC     Brand,
# MAGIC     CommodityDescription,
# MAGIC     BasketCount,
# MAGIC     HouseholdCount,
# MAGIC     StoreCount,
# MAGIC     TotalQuantity,
# MAGIC     GrossSalesAmount,
# MAGIC     TotalDiscountAmount,
# MAGIC     NetSalesAmount,
# MAGIC     CouponTransactionCount,
# MAGIC     AverageUnitNetSales,
# MAGIC     DiscountRate,
# MAGIC     CASE
# MAGIC         WHEN BasketCount = 0 THEN 0
# MAGIC         ELSE CouponTransactionCount / BasketCount
# MAGIC     END AS CouponUsageRate,
# MAGIC     DENSE_RANK() OVER (
# MAGIC         PARTITION BY SourceBatchID
# MAGIC         ORDER BY NetSalesAmount DESC
# MAGIC     ) AS NetSalesRank,
# MAGIC     DENSE_RANK() OVER (
# MAGIC         PARTITION BY SourceBatchID
# MAGIC         ORDER BY TotalQuantity DESC
# MAGIC     ) AS QuantityRank,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Kampanya performans view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_campaign_performance AS
# MAGIC SELECT
# MAGIC     CampaignKey,
# MAGIC     CampaignID,
# MAGIC     CampaignDescription,
# MAGIC     StartDay,
# MAGIC     EndDay,
# MAGIC     CampaignDuration,
# MAGIC     TargetHouseholdCount,
# MAGIC     RedeemingHouseholdCount,
# MAGIC     RedemptionCount,
# MAGIC     DistinctCouponCount,
# MAGIC     RedemptionRate,
# MAGIC     CASE
# MAGIC         WHEN TargetHouseholdCount = 0 THEN 'NO_TARGET'
# MAGIC         WHEN RedemptionRate >= 0.20 THEN 'HIGH'
# MAGIC         WHEN RedemptionRate >= 0.10 THEN 'MEDIUM'
# MAGIC         WHEN RedemptionRate > 0 THEN 'LOW'
# MAGIC         ELSE 'NO_REDEMPTION'
# MAGIC     END AS CampaignPerformanceLevel,
# MAGIC     DENSE_RANK() OVER (
# MAGIC         PARTITION BY SourceBatchID
# MAGIC         ORDER BY RedemptionRate DESC
# MAGIC     ) AS RedemptionRateRank,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Promosyon performans view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_promotion_performance AS
# MAGIC SELECT
# MAGIC     ProductKey,
# MAGIC     ProductID,
# MAGIC     StoreKey,
# MAGIC     StoreID,
# MAGIC     WeekNumber,
# MAGIC     HasDisplay,
# MAGIC     HasMailer,
# MAGIC     CASE
# MAGIC         WHEN HasDisplay = 'Y' AND HasMailer = 'Y' THEN 'DISPLAY_AND_MAILER'
# MAGIC         WHEN HasDisplay = 'Y' THEN 'DISPLAY_ONLY'
# MAGIC         WHEN HasMailer = 'Y' THEN 'MAILER_ONLY'
# MAGIC         ELSE 'NO_PROMOTION'
# MAGIC     END AS PromotionType,
# MAGIC     PromotionCount,
# MAGIC     BasketCount,
# MAGIC     TotalQuantity,
# MAGIC     GrossSalesAmount,
# MAGIC     TotalDiscountAmount,
# MAGIC     NetSalesAmount,
# MAGIC     AverageBasketAmount,
# MAGIC     CASE
# MAGIC         WHEN GrossSalesAmount = 0 THEN 0
# MAGIC         ELSE TotalDiscountAmount / GrossSalesAmount
# MAGIC     END AS DiscountRate,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ETL batch özet view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_etl_batch_summary AS
# MAGIC SELECT
# MAGIC     b.BatchID,
# MAGIC     b.ProcessDate,
# MAGIC     b.ProcessDay,
# MAGIC     b.ProcessWeek,
# MAGIC     b.LoadMode,
# MAGIC     b.BatchStatus,
# MAGIC     b.StartTime AS BatchStartTime,
# MAGIC     b.EndTime AS BatchEndTime,
# MAGIC     COUNT(l.ETLLogID) AS TableLoadCount,
# MAGIC     SUM(CASE WHEN l.LoadStatus = 'SUCCESS' THEN 1 ELSE 0 END) AS SuccessCount,
# MAGIC     SUM(CASE WHEN l.LoadStatus = 'SUCCESS_WITH_WARNING' THEN 1 ELSE 0 END) AS WarningCount,
# MAGIC     SUM(CASE WHEN l.LoadStatus = 'SKIPPED' THEN 1 ELSE 0 END) AS SkippedCount,
# MAGIC     SUM(CASE WHEN l.LoadStatus = 'FAILED' THEN 1 ELSE 0 END) AS FailedCount,
# MAGIC     SUM(COALESCE(l.SourceRowCount, 0)) AS TotalSourceRowCount,
# MAGIC     SUM(COALESCE(l.InsertedRowCount, 0)) AS TotalInsertedRowCount,
# MAGIC     SUM(COALESCE(l.RejectedRowCount, 0)) AS TotalRejectedRowCount,
# MAGIC     SUM(COALESCE(l.UnchangedRowCount, 0)) AS TotalUnchangedRowCount
# MAGIC FROM retail_marketing.control.etl_batch_control b
# MAGIC LEFT JOIN retail_marketing.audit.etl_table_load_log l
# MAGIC     ON l.BatchID = b.BatchID
# MAGIC GROUP BY
# MAGIC     b.BatchID,
# MAGIC     b.ProcessDate,
# MAGIC     b.ProcessDay,
# MAGIC     b.ProcessWeek,
# MAGIC     b.LoadMode,
# MAGIC     b.BatchStatus,
# MAGIC     b.StartTime,
# MAGIC     b.EndTime
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data quality özet view
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW retail_marketing.dm_marketing.vw_data_quality_summary AS
# MAGIC SELECT
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     CheckStatus,
# MAGIC     SeverityLevel,
# MAGIC     COUNT(*) AS CheckCount,
# MAGIC     SUM(COALESCE(CheckedRowCount, 0)) AS CheckedRowCount,
# MAGIC     SUM(COALESCE(FailedRowCount, 0)) AS FailedRowCount,
# MAGIC     CASE
# MAGIC         WHEN SUM(COALESCE(CheckedRowCount, 0)) = 0 THEN 0
# MAGIC         ELSE SUM(COALESCE(FailedRowCount, 0))
# MAGIC              / SUM(COALESCE(CheckedRowCount, 0))
# MAGIC     END AS FailureRate
# MAGIC FROM retail_marketing.audit.data_quality_log
# MAGIC GROUP BY
# MAGIC     BatchID,
# MAGIC     LayerName,
# MAGIC     CheckStatus,
# MAGIC     SeverityLevel
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. View kontrolleri
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN retail_marketing.dm_marketing
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_marketing.dm_marketing.vw_daily_sales_kpi
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ORDER BY DayNumber
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ProductID,
# MAGIC     Department,
# MAGIC     Brand,
# MAGIC     NetSalesAmount,
# MAGIC     TotalQuantity,
# MAGIC     DiscountRate,
# MAGIC     NetSalesRank
# MAGIC FROM retail_marketing.dm_marketing.vw_top_products
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ORDER BY NetSalesRank
# MAGIC LIMIT 20
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CampaignID,
# MAGIC     CampaignDescription,
# MAGIC     TargetHouseholdCount,
# MAGIC     RedeemingHouseholdCount,
# MAGIC     RedemptionCount,
# MAGIC     RedemptionRate,
# MAGIC     CampaignPerformanceLevel,
# MAGIC     RedemptionRateRank
# MAGIC FROM retail_marketing.dm_marketing.vw_campaign_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ORDER BY RedemptionRateRank
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PromotionType,
# MAGIC     COUNT(*) AS PromotionRowCount,
# MAGIC     SUM(BasketCount) AS BasketCount,
# MAGIC     SUM(NetSalesAmount) AS NetSalesAmount,
# MAGIC     AVG(DiscountRate) AS AverageDiscountRate
# MAGIC FROM retail_marketing.dm_marketing.vw_promotion_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC GROUP BY PromotionType
# MAGIC ORDER BY NetSalesAmount DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_marketing.dm_marketing.vw_etl_batch_summary
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_marketing.dm_marketing.vw_data_quality_summary
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC ORDER BY LayerName, CheckStatus, SeverityLevel
# MAGIC