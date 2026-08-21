# Databricks notebook source
# DBTITLE 1,Çalışma alanını seç
# MAGIC %sql
# MAGIC USE CATALOG retail_marketing;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC SELECT
# MAGIC     current_catalog() AS CurrentCatalog,
# MAGIC     current_schema()  AS CurrentSchema;

# COMMAND ----------

# DBTITLE 1,Transaction Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.transaction_data_raw
# MAGIC (
# MAGIC     household_key       DECIMAL(10,0),
# MAGIC     basket_id           DECIMAL(20,0),
# MAGIC     day_no              DECIMAL(10,0),
# MAGIC     product_id          DECIMAL(10,0),
# MAGIC     quantity            DECIMAL(18,4),
# MAGIC     sales_value         DECIMAL(18,4),
# MAGIC     store_id            DECIMAL(10,0),
# MAGIC     retail_disc         DECIMAL(18,4),
# MAGIC     trans_time          DECIMAL(10,0),
# MAGIC     week_no             DECIMAL(10,0),
# MAGIC     coupon_disc         DECIMAL(18,4),
# MAGIC     coupon_match_disc   DECIMAL(18,4),
# MAGIC
# MAGIC     SourceFile          STRING,
# MAGIC     SourceSystem        STRING,
# MAGIC     BatchID             STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'transaction_data.csv kaynağının ham günlük satış kayıtları';

# COMMAND ----------

# DBTITLE 1,Coupon redemption Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.coupon_redemption_raw
# MAGIC (
# MAGIC     household_key       DECIMAL(10,0),
# MAGIC     day_no              DECIMAL(10,0),
# MAGIC     coupon_upc          DECIMAL(20,0),
# MAGIC     campaign_id         DECIMAL(10,0),
# MAGIC
# MAGIC     SourceFile          STRING,
# MAGIC     SourceSystem        STRING,
# MAGIC     BatchID             STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'coupon_redempt.csv kaynağının ham kupon kullanım kayıtları';

# COMMAND ----------

# DBTITLE 1,Product Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.product_raw
# MAGIC (
# MAGIC     product_id             DECIMAL(10,0),
# MAGIC     manufacturer           DECIMAL(10,0),
# MAGIC     department             STRING,
# MAGIC     brand                  STRING,
# MAGIC     commodity_desc         STRING,
# MAGIC     sub_commodity_desc     STRING,
# MAGIC     curr_size_of_product   STRING,
# MAGIC
# MAGIC     SourceFile             STRING,
# MAGIC     SourceSystem           STRING,
# MAGIC     BatchID                STRING,
# MAGIC     ProcessDate            DATE,
# MAGIC     ETLTime                TIMESTAMP,
# MAGIC     RecordHash             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'product.csv kaynağının ham ürün ana veri kayıtları';

# COMMAND ----------

# DBTITLE 1,Household demographic Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.household_demographic_raw
# MAGIC (
# MAGIC     age_desc               STRING,
# MAGIC     marital_status_code    STRING,
# MAGIC     income_desc            STRING,
# MAGIC     homeowner_desc         STRING,
# MAGIC     hh_comp_desc           STRING,
# MAGIC     household_size_desc    STRING,
# MAGIC     kid_category_desc      STRING,
# MAGIC     household_key          DECIMAL(10,0),
# MAGIC
# MAGIC     SourceFile             STRING,
# MAGIC     SourceSystem           STRING,
# MAGIC     BatchID                STRING,
# MAGIC     ProcessDate            DATE,
# MAGIC     ETLTime                TIMESTAMP,
# MAGIC     RecordHash             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'hh_demographic.csv kaynağının ham hane demografik kayıtları';

# COMMAND ----------

# DBTITLE 1,Campaign description Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.campaign_desc_raw
# MAGIC (
# MAGIC     description          STRING,
# MAGIC     campaign_id          DECIMAL(10,0),
# MAGIC     start_day            DECIMAL(10,0),
# MAGIC     end_day              DECIMAL(10,0),
# MAGIC
# MAGIC     SourceFile           STRING,
# MAGIC     SourceSystem         STRING,
# MAGIC     BatchID              STRING,
# MAGIC     ProcessDate          DATE,
# MAGIC     ETLTime              TIMESTAMP,
# MAGIC     RecordHash           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'campaign_desc.csv kaynağının ham kampanya tanım kayıtları';

# COMMAND ----------

# DBTITLE 1,Campaign target Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.campaign_target_raw
# MAGIC (
# MAGIC     description          STRING,
# MAGIC     household_key        DECIMAL(10,0),
# MAGIC     campaign_id          DECIMAL(10,0),
# MAGIC
# MAGIC     SourceFile           STRING,
# MAGIC     SourceSystem         STRING,
# MAGIC     BatchID              STRING,
# MAGIC     ProcessDate          DATE,
# MAGIC     ETLTime              TIMESTAMP,
# MAGIC     RecordHash           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'campaign_table.csv kaynağının ham kampanya-hedef hane ilişkileri';

# COMMAND ----------

# DBTITLE 1,Coupon Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.coupon_raw
# MAGIC (
# MAGIC     coupon_upc           DECIMAL(20,0),
# MAGIC     product_id           DECIMAL(10,0),
# MAGIC     campaign_id          DECIMAL(10,0),
# MAGIC
# MAGIC     SourceFile           STRING,
# MAGIC     SourceSystem         STRING,
# MAGIC     BatchID              STRING,
# MAGIC     ProcessDate          DATE,
# MAGIC     ETLTime              TIMESTAMP,
# MAGIC     RecordHash           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'coupon.csv kaynağının ham kupon, ürün ve kampanya ilişkileri';

# COMMAND ----------

# DBTITLE 1,Causal data Bronze tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.bronze.causal_data_raw
# MAGIC (
# MAGIC     product_id           DECIMAL(10,0),
# MAGIC     store_id             DECIMAL(10,0),
# MAGIC     week_no              DECIMAL(10,0),
# MAGIC     display_code         STRING,
# MAGIC     mailer_code          STRING,
# MAGIC
# MAGIC     SourceFile           STRING,
# MAGIC     SourceSystem         STRING,
# MAGIC     BatchID              STRING,
# MAGIC     ProcessDate          DATE,
# MAGIC     ETLTime              TIMESTAMP,
# MAGIC     RecordHash           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'causal_data.csv kaynağının ham ürün, mağaza ve haftalık promosyon kayıtları';

# COMMAND ----------

# DBTITLE 1,Silver transaction tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.transaction_clean
# MAGIC (
# MAGIC     household_key       DECIMAL(10,0),
# MAGIC     basket_id           DECIMAL(20,0) NOT NULL,
# MAGIC     day_no              DECIMAL(10,0) NOT NULL,
# MAGIC     product_id          DECIMAL(10,0) NOT NULL,
# MAGIC     quantity            DECIMAL(18,4),
# MAGIC     sales_value         DECIMAL(18,4),
# MAGIC     store_id            DECIMAL(10,0),
# MAGIC     retail_disc         DECIMAL(18,4),
# MAGIC     trans_time          DECIMAL(10,0),
# MAGIC     week_no             DECIMAL(10,0),
# MAGIC     coupon_disc         DECIMAL(18,4),
# MAGIC     coupon_match_disc   DECIMAL(18,4),
# MAGIC
# MAGIC     net_sales_value     DECIMAL(18,4),
# MAGIC     has_coupon          STRING,
# MAGIC
# MAGIC     SourceBatchID       STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş ve doğrulanmış satış işlem kayıtları';

# COMMAND ----------

# DBTITLE 1,Silver coupon redemption tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.coupon_redemption_clean
# MAGIC (
# MAGIC     household_key       DECIMAL(10,0) NOT NULL,
# MAGIC     day_no              DECIMAL(10,0) NOT NULL,
# MAGIC     coupon_upc          DECIMAL(20,0) NOT NULL,
# MAGIC     campaign_id         DECIMAL(10,0),
# MAGIC
# MAGIC     SourceBatchID       STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş ve duplicate kayıtları yönetilmiş kupon kullanım kayıtları';

# COMMAND ----------

# DBTITLE 1,Silver product tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.product_clean
# MAGIC (
# MAGIC     product_id             DECIMAL(10,0) NOT NULL,
# MAGIC     manufacturer           DECIMAL(10,0),
# MAGIC     department             STRING,
# MAGIC     brand                  STRING,
# MAGIC     commodity_desc         STRING,
# MAGIC     sub_commodity_desc     STRING,
# MAGIC     curr_size_of_product   STRING,
# MAGIC
# MAGIC     SourceBatchID          STRING,
# MAGIC     ProcessDate            DATE,
# MAGIC     ETLTime                TIMESTAMP,
# MAGIC     RecordHash             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş ve standardize edilmiş ürün ana verisi';

# COMMAND ----------

# DBTITLE 1,Silver household demographic tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.household_demographic_clean
# MAGIC (
# MAGIC     household_key          DECIMAL(10,0) NOT NULL,
# MAGIC     age_desc               STRING,
# MAGIC     marital_status_code    STRING,
# MAGIC     income_desc            STRING,
# MAGIC     homeowner_desc         STRING,
# MAGIC     hh_comp_desc           STRING,
# MAGIC     household_size_desc    STRING,
# MAGIC     kid_category_desc      STRING,
# MAGIC
# MAGIC     SourceBatchID          STRING,
# MAGIC     ProcessDate            DATE,
# MAGIC     ETLTime                TIMESTAMP,
# MAGIC     RecordHash             STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş hane demografik ana verisi';

# COMMAND ----------

# DBTITLE 1,Silver campaign tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.campaign_clean
# MAGIC (
# MAGIC     campaign_id          DECIMAL(10,0) NOT NULL,
# MAGIC     description          STRING,
# MAGIC     start_day            DECIMAL(10,0),
# MAGIC     end_day              DECIMAL(10,0),
# MAGIC     campaign_duration    DECIMAL(10,0),
# MAGIC
# MAGIC     SourceBatchID        STRING,
# MAGIC     ProcessDate          DATE,
# MAGIC     ETLTime              TIMESTAMP,
# MAGIC     RecordHash           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş kampanya tanım kayıtları';

# COMMAND ----------

# DBTITLE 1,Silver campaign target tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.campaign_target_clean
# MAGIC (
# MAGIC     household_key       DECIMAL(10,0) NOT NULL,
# MAGIC     campaign_id         DECIMAL(10,0) NOT NULL,
# MAGIC     description         STRING,
# MAGIC
# MAGIC     SourceBatchID       STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş kampanya-hedef hane ilişkileri';

# COMMAND ----------

# DBTITLE 1,Silver coupon-product tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.coupon_product_clean
# MAGIC (
# MAGIC     coupon_upc          DECIMAL(20,0) NOT NULL,
# MAGIC     product_id          DECIMAL(10,0) NOT NULL,
# MAGIC     campaign_id         DECIMAL(10,0),
# MAGIC
# MAGIC     SourceBatchID       STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Duplicate kayıtları temizlenmiş kupon, ürün ve kampanya ilişkileri';

# COMMAND ----------

# DBTITLE 1,Silver promotion weekly tablosu
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_marketing.silver.promotion_weekly_clean
# MAGIC (
# MAGIC     product_id          DECIMAL(10,0) NOT NULL,
# MAGIC     store_id            DECIMAL(10,0) NOT NULL,
# MAGIC     week_no             DECIMAL(10,0) NOT NULL,
# MAGIC     display_code        STRING,
# MAGIC     mailer_code         STRING,
# MAGIC     has_display         STRING,
# MAGIC     has_mailer          STRING,
# MAGIC
# MAGIC     SourceBatchID       STRING,
# MAGIC     ProcessDate         DATE,
# MAGIC     ETLTime             TIMESTAMP,
# MAGIC     RecordHash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Temizlenmiş ürün, mağaza ve haftalık promosyon kayıtları';

# COMMAND ----------

# DBTITLE 1,Bronze tablolarını kontrol et
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_marketing.bronze;

# COMMAND ----------

# DBTITLE 1,Silver tablolarını kontrol et
# MAGIC %sql
# MAGIC SHOW TABLES IN retail_marketing.silver;

# COMMAND ----------

# DBTITLE 1,Bütün tabloların boş olduğunu kontrol et
# MAGIC %sql
# MAGIC SELECT 'bronze.transaction_data_raw' AS TableName, COUNT(*) AS RowCount
# MAGIC FROM retail_marketing.bronze.transaction_data_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.coupon_redemption_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.coupon_redemption_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.product_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.product_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.household_demographic_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.household_demographic_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.campaign_desc_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.campaign_desc_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.campaign_target_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.campaign_target_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.coupon_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.coupon_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze.causal_data_raw', COUNT(*)
# MAGIC FROM retail_marketing.bronze.causal_data_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.transaction_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.transaction_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.coupon_redemption_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.coupon_redemption_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.product_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.product_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.household_demographic_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.household_demographic_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.campaign_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.campaign_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.campaign_target_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.campaign_target_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.coupon_product_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.coupon_product_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'silver.promotion_weekly_clean', COUNT(*)
# MAGIC FROM retail_marketing.silver.promotion_weekly_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE retail_marketing.audit.data_quality_log;