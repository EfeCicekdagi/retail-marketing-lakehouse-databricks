# Databricks notebook source
# DBTITLE 1,Catalog oluştur
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_marketing
# MAGIC COMMENT 'Retail Marketing Lakehouse, DWH ve Data Mart projesi';
# MAGIC
# MAGIC USE CATALOG retail_marketing;

# COMMAND ----------

# DBTITLE 1,Şemaları oluştur
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.source
# MAGIC COMMENT 'Kaynak CSV dosyaları ve volume alanları';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.bronze
# MAGIC COMMENT 'Kaynak verilerin ham Delta tabloları';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.silver
# MAGIC COMMENT 'Temizlenmiş ve standardize edilmiş veriler';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.dwh
# MAGIC COMMENT 'Kimball boyutsal veri ambarı';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.dm_marketing
# MAGIC COMMENT 'Marketing analizleri için Data Mart tabloları';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.control
# MAGIC COMMENT 'ETL batch ve watermark kontrol tabloları';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_marketing.audit
# MAGIC COMMENT 'ETL log, kalite ve mutabakat tabloları';

# COMMAND ----------

# DBTITLE 1,Şemaları kontrol et
# MAGIC %sql
# MAGIC SHOW SCHEMAS IN retail_marketing;

# COMMAND ----------

# DBTITLE 1,Kaynak Volume oluştur
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS retail_marketing.source.project_files
# MAGIC COMMENT 'Retail Marketing kaynak, landing, reject ve geçici dosya alanı';

# COMMAND ----------

# DBTITLE 1,Volume kontrolü
# MAGIC %sql
# MAGIC SHOW VOLUMES IN retail_marketing.source;

# COMMAND ----------

# DBTITLE 1,Mevcut volume bilgilerini göster
# MAGIC %sql
# MAGIC SELECT
# MAGIC     volume_catalog,
# MAGIC     volume_schema,
# MAGIC     volume_name,
# MAGIC     volume_type,
# MAGIC     storage_location,
# MAGIC     comment
# MAGIC FROM retail_marketing.information_schema.volumes
# MAGIC WHERE volume_schema = 'source'
# MAGIC ORDER BY volume_name;

# COMMAND ----------

# DBTITLE 1,Aktif çalışma alanını seç
# MAGIC %sql
# MAGIC USE CATALOG retail_marketing;
# MAGIC USE SCHEMA source;
# MAGIC
# MAGIC SELECT
# MAGIC     current_catalog() AS CurrentCatalog,
# MAGIC     current_schema() AS CurrentSchema;

# COMMAND ----------

# DBTITLE 1,Alt klasörlerin oluşturulması
base_path = "/Volumes/retail_marketing/source/project_files"

folder_paths = [
    f"{base_path}/source",
    f"{base_path}/landing",
    f"{base_path}/rejected",
    f"{base_path}/archive"
]

for folder_path in folder_paths:
    dbutils.fs.mkdirs(folder_path)
    print(f"Hazır: {folder_path}")

# COMMAND ----------

# DBTITLE 1,Klasör kontrolü
display(
    dbutils.fs.ls(
        "/Volumes/retail_marketing/source/project_files"
    )
)