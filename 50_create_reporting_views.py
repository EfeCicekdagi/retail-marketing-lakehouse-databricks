# Databricks notebook source
# MAGIC %md
# MAGIC # 40_build_dm_marketing — Working Version
# MAGIC
# MAGIC Bu notebook DWH katmanından pazarlama odaklı Data Mart tablolarını oluşturur.
# MAGIC
# MAGIC Oluşturulan tablolar:
# MAGIC
# MAGIC - `dm_marketing.dm_daily_sales`
# MAGIC - `dm_marketing.dm_product_sales`
# MAGIC - `dm_marketing.dm_campaign_performance`
# MAGIC - `dm_marketing.dm_promotion_performance`
# MAGIC
# MAGIC ## Amaç
# MAGIC
# MAGIC - Günlük satış performansını izlemek
# MAGIC - Ürün bazlı satış ve indirim performansını analiz etmek
# MAGIC - Kampanya hedefleme ve kupon kullanım performansını ölçmek
# MAGIC - Promosyonlu ürünlerin satış etkisini incelemek
# MAGIC
# MAGIC Aynı `BatchID` tekrar çalıştırıldığında ilgili batch kayıtları silinip yeniden üretilir.
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
# MAGIC     CAST(:ProcessDay AS DECIMAL(10,0)) AS ProcessDay,
# MAGIC     CAST(:ProcessWeek AS DECIMAL(10,0)) AS ProcessWeek
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Mart tablolarını oluştur
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.dm_marketing.dm_daily_sales (
# MAGIC     DayKey BIGINT,
# MAGIC     DayNumber DECIMAL(10,0),
# MAGIC     WeekNumber DECIMAL(10,0),
# MAGIC     BasketCount BIGINT,
# MAGIC     HouseholdCount BIGINT,
# MAGIC     ProductCount BIGINT,
# MAGIC     StoreCount BIGINT,
# MAGIC     TotalQuantity DECIMAL(20,4),
# MAGIC     GrossSalesAmount DECIMAL(20,4),
# MAGIC     TotalDiscountAmount DECIMAL(20,4),
# MAGIC     NetSalesAmount DECIMAL(20,4),
# MAGIC     CouponTransactionCount BIGINT,
# MAGIC     AverageBasketAmount DECIMAL(20,4),
# MAGIC     DiscountRate DECIMAL(18,6),
# MAGIC     SourceBatchID STRING,
# MAGIC     ProcessDate DATE,
# MAGIC     CreatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.dm_marketing.dm_product_sales (
# MAGIC     ProductKey BIGINT,
# MAGIC     ProductID DECIMAL(10,0),
# MAGIC     Department STRING,
# MAGIC     Brand STRING,
# MAGIC     CommodityDescription STRING,
# MAGIC     BasketCount BIGINT,
# MAGIC     HouseholdCount BIGINT,
# MAGIC     StoreCount BIGINT,
# MAGIC     TotalQuantity DECIMAL(20,4),
# MAGIC     GrossSalesAmount DECIMAL(20,4),
# MAGIC     TotalDiscountAmount DECIMAL(20,4),
# MAGIC     NetSalesAmount DECIMAL(20,4),
# MAGIC     CouponTransactionCount BIGINT,
# MAGIC     AverageUnitNetSales DECIMAL(20,4),
# MAGIC     DiscountRate DECIMAL(18,6),
# MAGIC     SourceBatchID STRING,
# MAGIC     ProcessDate DATE,
# MAGIC     CreatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.dm_marketing.dm_campaign_performance (
# MAGIC     CampaignKey BIGINT,
# MAGIC     CampaignID DECIMAL(10,0),
# MAGIC     CampaignDescription STRING,
# MAGIC     StartDay DECIMAL(10,0),
# MAGIC     EndDay DECIMAL(10,0),
# MAGIC     CampaignDuration DECIMAL(10,0),
# MAGIC     TargetHouseholdCount BIGINT,
# MAGIC     RedeemingHouseholdCount BIGINT,
# MAGIC     RedemptionCount BIGINT,
# MAGIC     DistinctCouponCount BIGINT,
# MAGIC     RedemptionRate DECIMAL(18,6),
# MAGIC     SourceBatchID STRING,
# MAGIC     ProcessDate DATE,
# MAGIC     CreatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.dm_marketing.dm_promotion_performance (
# MAGIC     ProductKey BIGINT,
# MAGIC     ProductID DECIMAL(10,0),
# MAGIC     StoreKey BIGINT,
# MAGIC     StoreID DECIMAL(10,0),
# MAGIC     WeekNumber DECIMAL(10,0),
# MAGIC     HasDisplay STRING,
# MAGIC     HasMailer STRING,
# MAGIC     PromotionCount BIGINT,
# MAGIC     BasketCount BIGINT,
# MAGIC     TotalQuantity DECIMAL(20,4),
# MAGIC     GrossSalesAmount DECIMAL(20,4),
# MAGIC     TotalDiscountAmount DECIMAL(20,4),
# MAGIC     NetSalesAmount DECIMAL(20,4),
# MAGIC     AverageBasketAmount DECIMAL(20,4),
# MAGIC     SourceBatchID STRING,
# MAGIC     ProcessDate DATE,
# MAGIC     CreatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Günlük satış Data Mart
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.dm_marketing.dm_daily_sales (
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
# MAGIC     NetSalesAmount,
# MAGIC     CouponTransactionCount,
# MAGIC     AverageBasketAmount,
# MAGIC     DiscountRate,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC )
# MAGIC SELECT
# MAGIC     f.DayKey,
# MAGIC     d.DayNumber,
# MAGIC     MAX(f.WeekNumber) AS WeekNumber,
# MAGIC     COUNT(DISTINCT f.BasketID) AS BasketCount,
# MAGIC     COUNT(DISTINCT CASE WHEN f.HouseholdKey <> -1 THEN f.HouseholdKey END) AS HouseholdCount,
# MAGIC     COUNT(DISTINCT CASE WHEN f.ProductKey <> -1 THEN f.ProductKey END) AS ProductCount,
# MAGIC     COUNT(DISTINCT CASE WHEN f.StoreKey <> -1 THEN f.StoreKey END) AS StoreCount,
# MAGIC     SUM(f.Quantity) AS TotalQuantity,
# MAGIC     SUM(f.GrossSalesAmount) AS GrossSalesAmount,
# MAGIC     SUM(f.TotalDiscountAmount) AS TotalDiscountAmount,
# MAGIC     SUM(f.NetSalesAmount) AS NetSalesAmount,
# MAGIC     SUM(CASE WHEN f.HasCoupon = 'Y' THEN 1 ELSE 0 END) AS CouponTransactionCount,
# MAGIC     CASE
# MAGIC         WHEN COUNT(DISTINCT f.BasketID) = 0 THEN 0
# MAGIC         ELSE SUM(f.NetSalesAmount) / COUNT(DISTINCT f.BasketID)
# MAGIC     END AS AverageBasketAmount,
# MAGIC     CASE
# MAGIC         WHEN SUM(f.GrossSalesAmount) = 0 THEN 0
# MAGIC         ELSE SUM(f.TotalDiscountAmount) / SUM(f.GrossSalesAmount)
# MAGIC     END AS DiscountRate,
# MAGIC     TRIM(:BatchID),
# MAGIC     CAST(:ProcessDate AS DATE),
# MAGIC     current_timestamp()
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction f
# MAGIC LEFT JOIN retail_marketing.dwh.dim_day d
# MAGIC     ON d.DayKey = f.DayKey
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC   AND d.DayNumber = CAST(:ProcessDay AS DECIMAL(10,0))
# MAGIC GROUP BY
# MAGIC     f.DayKey,
# MAGIC     d.DayNumber
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ürün satış Data Mart
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.dm_marketing.dm_product_sales (
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
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC )
# MAGIC SELECT
# MAGIC     f.ProductKey,
# MAGIC     p.ProductID,
# MAGIC     p.Department,
# MAGIC     p.Brand,
# MAGIC     p.CommodityDescription,
# MAGIC
# MAGIC     COUNT(DISTINCT f.BasketID) AS BasketCount,
# MAGIC
# MAGIC     COUNT(
# MAGIC         DISTINCT CASE
# MAGIC             WHEN f.HouseholdKey <> -1 THEN f.HouseholdKey
# MAGIC         END
# MAGIC     ) AS HouseholdCount,
# MAGIC
# MAGIC     COUNT(
# MAGIC         DISTINCT CASE
# MAGIC             WHEN f.StoreKey <> -1 THEN f.StoreKey
# MAGIC         END
# MAGIC     ) AS StoreCount,
# MAGIC
# MAGIC     SUM(f.Quantity) AS TotalQuantity,
# MAGIC     SUM(f.GrossSalesAmount) AS GrossSalesAmount,
# MAGIC     SUM(f.TotalDiscountAmount) AS TotalDiscountAmount,
# MAGIC     SUM(f.NetSalesAmount) AS NetSalesAmount,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN f.HasCoupon = 'Y' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS CouponTransactionCount,
# MAGIC
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN SUM(f.Quantity) = 0 THEN 0
# MAGIC             ELSE SUM(f.NetSalesAmount) / SUM(f.Quantity)
# MAGIC         END
# MAGIC         AS DECIMAL(20,4)
# MAGIC     ) AS AverageUnitNetSales,
# MAGIC
# MAGIC     CAST(
# MAGIC         CASE
# MAGIC             WHEN SUM(f.NetSalesAmount) + SUM(f.TotalDiscountAmount) = 0
# MAGIC                 THEN 0
# MAGIC             ELSE
# MAGIC                 SUM(f.TotalDiscountAmount)
# MAGIC                 /
# MAGIC                 (
# MAGIC                     SUM(f.NetSalesAmount)
# MAGIC                     + SUM(f.TotalDiscountAmount)
# MAGIC                 )
# MAGIC         END
# MAGIC         AS DECIMAL(18,6)
# MAGIC     ) AS DiscountRate,
# MAGIC
# MAGIC     TRIM(:BatchID) AS SourceBatchID,
# MAGIC     CAST(:ProcessDate AS DATE) AS ProcessDate,
# MAGIC     current_timestamp() AS CreatedAt
# MAGIC
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction f
# MAGIC
# MAGIC LEFT JOIN retail_marketing.dwh.dim_product p
# MAGIC     ON p.ProductKey = f.ProductKey
# MAGIC
# MAGIC WHERE f.SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC GROUP BY
# MAGIC     f.ProductKey,
# MAGIC     p.ProductID,
# MAGIC     p.Department,
# MAGIC     p.Brand,
# MAGIC     p.CommodityDescription;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Kampanya performans Data Mart
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW campaign_target_summary AS
# MAGIC SELECT
# MAGIC     CampaignKey,
# MAGIC     COUNT(*) AS TargetHouseholdCount
# MAGIC FROM retail_marketing.dwh.fact_campaign_target
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC GROUP BY CampaignKey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW campaign_redemption_summary AS
# MAGIC SELECT
# MAGIC     CampaignKey,
# MAGIC     COUNT(DISTINCT HouseholdKey) AS RedeemingHouseholdCount,
# MAGIC     SUM(RedemptionCount) AS RedemptionCount,
# MAGIC     COUNT(DISTINCT CouponKey) AS DistinctCouponCount
# MAGIC FROM retail_marketing.dwh.fact_coupon_redemption
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC GROUP BY CampaignKey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.dm_marketing.dm_campaign_performance (
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
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC )
# MAGIC SELECT
# MAGIC     c.CampaignKey,
# MAGIC     c.CampaignID,
# MAGIC     c.CampaignDescription,
# MAGIC     c.StartDay,
# MAGIC     c.EndDay,
# MAGIC     c.CampaignDuration,
# MAGIC     COALESCE(t.TargetHouseholdCount, 0) AS TargetHouseholdCount,
# MAGIC     COALESCE(r.RedeemingHouseholdCount, 0) AS RedeemingHouseholdCount,
# MAGIC     COALESCE(r.RedemptionCount, 0) AS RedemptionCount,
# MAGIC     COALESCE(r.DistinctCouponCount, 0) AS DistinctCouponCount,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(t.TargetHouseholdCount, 0) = 0 THEN 0
# MAGIC         ELSE COALESCE(r.RedeemingHouseholdCount, 0) / t.TargetHouseholdCount
# MAGIC     END AS RedemptionRate,
# MAGIC     TRIM(:BatchID),
# MAGIC     CAST(:ProcessDate AS DATE),
# MAGIC     current_timestamp()
# MAGIC FROM retail_marketing.dwh.dim_campaign c
# MAGIC LEFT JOIN campaign_target_summary t
# MAGIC     ON t.CampaignKey = c.CampaignKey
# MAGIC LEFT JOIN campaign_redemption_summary r
# MAGIC     ON r.CampaignKey = c.CampaignKey
# MAGIC WHERE c.CampaignKey <> -1
# MAGIC   AND (
# MAGIC       t.CampaignKey IS NOT NULL
# MAGIC       OR r.CampaignKey IS NOT NULL
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Promosyon performans Data Mart
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW promotion_sales_summary AS
# MAGIC SELECT
# MAGIC     ProductKey,
# MAGIC     StoreKey,
# MAGIC     WeekNumber,
# MAGIC     COUNT(DISTINCT BasketID) AS BasketCount,
# MAGIC     SUM(Quantity) AS TotalQuantity,
# MAGIC     SUM(GrossSalesAmount) AS GrossSalesAmount,
# MAGIC     SUM(TotalDiscountAmount) AS TotalDiscountAmount,
# MAGIC     SUM(NetSalesAmount) AS NetSalesAmount
# MAGIC FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC   AND WeekNumber = CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC GROUP BY
# MAGIC     ProductKey,
# MAGIC     StoreKey,
# MAGIC     WeekNumber
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_marketing.dm_marketing.dm_promotion_performance (
# MAGIC     ProductKey,
# MAGIC     ProductID,
# MAGIC     StoreKey,
# MAGIC     StoreID,
# MAGIC     WeekNumber,
# MAGIC     HasDisplay,
# MAGIC     HasMailer,
# MAGIC     PromotionCount,
# MAGIC     BasketCount,
# MAGIC     TotalQuantity,
# MAGIC     GrossSalesAmount,
# MAGIC     TotalDiscountAmount,
# MAGIC     NetSalesAmount,
# MAGIC     AverageBasketAmount,
# MAGIC     SourceBatchID,
# MAGIC     ProcessDate,
# MAGIC     CreatedAt
# MAGIC )
# MAGIC SELECT
# MAGIC     pr.ProductKey,
# MAGIC     p.ProductID,
# MAGIC     pr.StoreKey,
# MAGIC     s.StoreID,
# MAGIC     pr.WeekNumber,
# MAGIC     pr.HasDisplay,
# MAGIC     pr.HasMailer,
# MAGIC     SUM(pr.PromotionCount) AS PromotionCount,
# MAGIC     COALESCE(MAX(sa.BasketCount), 0) AS BasketCount,
# MAGIC     COALESCE(MAX(sa.TotalQuantity), 0) AS TotalQuantity,
# MAGIC     COALESCE(MAX(sa.GrossSalesAmount), 0) AS GrossSalesAmount,
# MAGIC     COALESCE(MAX(sa.TotalDiscountAmount), 0) AS TotalDiscountAmount,
# MAGIC     COALESCE(MAX(sa.NetSalesAmount), 0) AS NetSalesAmount,
# MAGIC     CASE
# MAGIC         WHEN COALESCE(MAX(sa.BasketCount), 0) = 0 THEN 0
# MAGIC         ELSE COALESCE(MAX(sa.NetSalesAmount), 0) / MAX(sa.BasketCount)
# MAGIC     END AS AverageBasketAmount,
# MAGIC     TRIM(:BatchID),
# MAGIC     CAST(:ProcessDate AS DATE),
# MAGIC     current_timestamp()
# MAGIC FROM retail_marketing.dwh.fact_promotion_weekly pr
# MAGIC LEFT JOIN promotion_sales_summary sa
# MAGIC     ON sa.ProductKey = pr.ProductKey
# MAGIC    AND sa.StoreKey = pr.StoreKey
# MAGIC    AND sa.WeekNumber = pr.WeekNumber
# MAGIC LEFT JOIN retail_marketing.dwh.dim_product p
# MAGIC     ON p.ProductKey = pr.ProductKey
# MAGIC LEFT JOIN retail_marketing.dwh.dim_store s
# MAGIC     ON s.StoreKey = pr.StoreKey
# MAGIC WHERE pr.SourceBatchID = TRIM(:BatchID)
# MAGIC   AND pr.WeekNumber = CAST(:ProcessWeek AS DECIMAL(10,0))
# MAGIC GROUP BY
# MAGIC     pr.ProductKey,
# MAGIC     p.ProductID,
# MAGIC     pr.StoreKey,
# MAGIC     s.StoreID,
# MAGIC     pr.WeekNumber,
# MAGIC     pr.HasDisplay,
# MAGIC     pr.HasMailer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Mart yükleme loglarını yaz
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_marketing.audit.etl_table_load_log AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_DM_DAILY_SALES') AS ETLLogID,
# MAGIC         TRIM(:BatchID) AS BatchID,
# MAGIC         'fact_sales_transaction' AS SourceName,
# MAGIC         'retail_marketing.dm_marketing.dm_daily_sales' AS TargetTableName,
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)) AS ProcessValue,
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         ) AS SourceRowCount,
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         ) AS InsertedRowCount
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_DM_PRODUCT_SALES'),
# MAGIC         TRIM(:BatchID),
# MAGIC         'fact_sales_transaction',
# MAGIC         'retail_marketing.dm_marketing.dm_product_sales',
# MAGIC         CAST(:ProcessDay AS DECIMAL(20,0)),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dwh.fact_sales_transaction
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         ),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         )
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_DM_CAMPAIGN_PERFORMANCE'),
# MAGIC         TRIM(:BatchID),
# MAGIC         'fact_campaign_target+fact_coupon_redemption',
# MAGIC         'retail_marketing.dm_marketing.dm_campaign_performance',
# MAGIC         CAST(NULL AS DECIMAL(20,0)),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dwh.fact_campaign_target
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         ),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         )
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         CONCAT(TRIM(:BatchID), '_DM_PROMOTION_PERFORMANCE'),
# MAGIC         TRIM(:BatchID),
# MAGIC         'fact_promotion_weekly+fact_sales_transaction',
# MAGIC         'retail_marketing.dm_marketing.dm_promotion_performance',
# MAGIC         CAST(:ProcessWeek AS DECIMAL(20,0)),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dwh.fact_promotion_weekly
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         ),
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC             FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC             WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC         )
# MAGIC ) source
# MAGIC ON target.ETLLogID = source.ETLLogID
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.BatchID = source.BatchID,
# MAGIC     target.LayerName = 'DM',
# MAGIC     target.SourceName = source.SourceName,
# MAGIC     target.TargetTableName = source.TargetTableName,
# MAGIC     target.LoadStrategy = 'AGGREGATE_RELOAD',
# MAGIC     target.ProcessDate = CAST(:ProcessDate AS DATE),
# MAGIC     target.ProcessValue = source.ProcessValue,
# MAGIC     target.StartTime = current_timestamp(),
# MAGIC     target.EndTime = current_timestamp(),
# MAGIC     target.SourceRowCount = source.SourceRowCount,
# MAGIC     target.InsertedRowCount = source.InsertedRowCount,
# MAGIC     target.UpdatedRowCount = 0,
# MAGIC     target.RejectedRowCount = 0,
# MAGIC     target.UnchangedRowCount = 0,
# MAGIC     target.LoadStatus = CASE
# MAGIC         WHEN source.SourceRowCount = 0 THEN 'FAILED'
# MAGIC         WHEN source.InsertedRowCount = 0 THEN 'FAILED'
# MAGIC         ELSE 'SUCCESS'
# MAGIC     END,
# MAGIC     target.ErrorMessage = CASE
# MAGIC         WHEN source.SourceRowCount = 0 THEN 'Data Mart kaynağı boş.'
# MAGIC         WHEN source.InsertedRowCount = 0 THEN 'Data Mart hedefinde kayıt oluşmadı.'
# MAGIC         ELSE NULL
# MAGIC     END,
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
# MAGIC     'DM',
# MAGIC     source.SourceName,
# MAGIC     source.TargetTableName,
# MAGIC     'AGGREGATE_RELOAD',
# MAGIC     CAST(:ProcessDate AS DATE),
# MAGIC     source.ProcessValue,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     source.SourceRowCount,
# MAGIC     source.InsertedRowCount,
# MAGIC     0,
# MAGIC     0,
# MAGIC     0,
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0 THEN 'FAILED'
# MAGIC         WHEN source.InsertedRowCount = 0 THEN 'FAILED'
# MAGIC         ELSE 'SUCCESS'
# MAGIC     END,
# MAGIC     CASE
# MAGIC         WHEN source.SourceRowCount = 0 THEN 'Data Mart kaynağı boş.'
# MAGIC         WHEN source.InsertedRowCount = 0 THEN 'Data Mart hedefinde kayıt oluşmadı.'
# MAGIC         ELSE NULL
# MAGIC     END,
# MAGIC     current_timestamp()
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sonuç kontrolleri
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dm_daily_sales' AS DataMartTable, COUNT(*) AS RowCount
# MAGIC FROM retail_marketing.dm_marketing.dm_daily_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_product_sales', COUNT(*)
# MAGIC FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_campaign_performance', COUNT(*)
# MAGIC FROM retail_marketing.dm_marketing.dm_campaign_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dm_promotion_performance', COUNT(*)
# MAGIC FROM retail_marketing.dm_marketing.dm_promotion_performance
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SourceName,
# MAGIC     TargetTableName,
# MAGIC     ProcessValue,
# MAGIC     SourceRowCount,
# MAGIC     InsertedRowCount,
# MAGIC     LoadStatus,
# MAGIC     ErrorMessage
# MAGIC FROM retail_marketing.audit.etl_table_load_log
# MAGIC WHERE BatchID = TRIM(:BatchID)
# MAGIC   AND LayerName = 'DM'
# MAGIC ORDER BY TargetTableName
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ProductID,
# MAGIC     Department,
# MAGIC     Brand,
# MAGIC     BasketCount,
# MAGIC     TotalQuantity,
# MAGIC     GrossSalesAmount,
# MAGIC     TotalDiscountAmount,
# MAGIC     NetSalesAmount,
# MAGIC     DiscountRate
# MAGIC FROM retail_marketing.dm_marketing.dm_product_sales
# MAGIC WHERE SourceBatchID = TRIM(:BatchID)
# MAGIC ORDER BY NetSalesAmount DESC
# MAGIC LIMIT 20
# MAGIC