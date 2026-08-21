# Databricks notebook source
# MAGIC %md
# MAGIC # 00_prepare_run_parameters
# MAGIC
# MAGIC Bu notebook job zincirinin ilk task'ıdır.
# MAGIC
# MAGIC Yaptığı işlemler:
# MAGIC
# MAGIC - Son başarılı `ProcessDay` değerini bulur.
# MAGIC - Bir sonraki veri gününü hesaplar.
# MAGIC - `ProcessWeek` değerini otomatik üretir.
# MAGIC - Yeni `BatchID` üretir.
# MAGIC - Parametreleri sonraki task'lara `taskValues` ile aktarır.
# MAGIC - Kaynak CSV'deki en büyük gün aşılırsa job'u durdurur.
# MAGIC
# MAGIC Bu notebook Python task olarak çalıştırılmalıdır.
# MAGIC

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

CATALOG = "retail_marketing"

TRANSACTION_SOURCE_FILE = (
    "/Volumes/retail_marketing/source/source_files/master/"
    "transaction_data.csv"
)

CAUSAL_SOURCE_FILE = (
    "/Volumes/retail_marketing/source/source_files/master/"
    "causal_data.csv"
)

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Kaynak veri setindeki gün aralığını bul
# MAGIC

# COMMAND ----------

# Transaction veri setindeki gün aralığı
source_days = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(TRANSACTION_SOURCE_FILE)
         .select(
             F.col("DAY").cast("int").alias("ProcessDay")
         )
         .where(
             F.col("ProcessDay").isNotNull()
         )
)

source_day_row = source_days.agg(
    F.min("ProcessDay").alias("MinSourceDay"),
    F.max("ProcessDay").alias("MaxSourceDay"),
    F.countDistinct("ProcessDay").alias("DistinctSourceDayCount")
).first()

min_source_day = int(source_day_row["MinSourceDay"])
max_source_day = int(source_day_row["MaxSourceDay"])
distinct_source_day_count = int(
    source_day_row["DistinctSourceDayCount"]
)


# Causal veri setindeki hafta aralığı
source_weeks = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(CAUSAL_SOURCE_FILE)
         .select(
             F.col("WEEK_NO").cast("int").alias("ProcessWeek")
         )
         .where(
             F.col("ProcessWeek").isNotNull()
         )
)

source_week_row = source_weeks.agg(
    F.min("ProcessWeek").alias("MinSourceWeek"),
    F.max("ProcessWeek").alias("MaxSourceWeek"),
    F.countDistinct("ProcessWeek").alias(
        "DistinctSourceWeekCount"
    )
).first()

min_source_week = int(source_week_row["MinSourceWeek"])
max_source_week = int(source_week_row["MaxSourceWeek"])
distinct_source_week_count = int(
    source_week_row["DistinctSourceWeekCount"]
)


print({
    "MinSourceDay": min_source_day,
    "MaxSourceDay": max_source_day,
    "DistinctSourceDayCount": distinct_source_day_count,
    "MinSourceWeek": min_source_week,
    "MaxSourceWeek": max_source_week,
    "DistinctSourceWeekCount": distinct_source_week_count
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Son başarılı batch gününü bul
# MAGIC

# COMMAND ----------

last_success_row = spark.sql(f'''
SELECT
    MAX(CAST(ProcessDay AS INT)) AS LastSuccessfulDay
FROM {CATALOG}.control.etl_batch_control
WHERE BatchStatus IN ('SUCCESS', 'SUCCESS_WITH_WARNING')
''').first()

last_successful_day = last_success_row["LastSuccessfulDay"]

if last_successful_day is None:
    process_day = min_source_day
    load_mode = "INITIAL"
else:
    process_day = int(last_successful_day) + 1
    load_mode = "INCREMENTAL"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gün sınırı ve çalışma parametreleri
# MAGIC

# COMMAND ----------

if process_day > max_source_day:
    raise RuntimeError(
        f"Kaynak veri günleri tamamlandı. "
        f"Hesaplanan ProcessDay={process_day}, "
        f"MaxSourceDay={max_source_day}."
    )


# Gün numarasını kaynak veri setindeki hafta numarasına çevir
process_week = (
    min_source_week
    + ((process_day - min_source_day) // 7)
)


if process_week > max_source_week:
    raise RuntimeError(
        f"Kaynak veri haftaları tamamlandı. "
        f"Hesaplanan ProcessWeek={process_week}, "
        f"MaxSourceWeek={max_source_week}."
    )


process_date = datetime.now().date().isoformat()
batch_id = f"RM_DAILY_D{process_day:04d}"


run_parameters = {
    "BatchID": batch_id,
    "LoadMode": load_mode,
    "ProcessDate": process_date,
    "ProcessDay": str(process_day),
    "ProcessWeek": str(process_week)
}


print("Hesaplanan parametreler:")

for key, value in run_parameters.items():
    print(f"{key} = {value}")


run_parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Değerleri sonraki task'lara aktar
# MAGIC

# COMMAND ----------

for key, value in run_parameters.items():
    dbutils.jobs.taskValues.set(key=key, value=value)

print("Üretilen job parametreleri:")
for key, value in run_parameters.items():
    print(f"{key} = {value}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sonraki task'larda kullanılacak referanslar
# MAGIC
# MAGIC Task adı `prepare_run_parameters` olmalıdır.
# MAGIC
# MAGIC ```text
# MAGIC {{tasks.prepare_run_parameters.values.BatchID}}
# MAGIC {{tasks.prepare_run_parameters.values.LoadMode}}
# MAGIC {{tasks.prepare_run_parameters.values.ProcessDate}}
# MAGIC {{tasks.prepare_run_parameters.values.ProcessDay}}
# MAGIC {{tasks.prepare_run_parameters.values.ProcessWeek}}
# MAGIC ```
# MAGIC