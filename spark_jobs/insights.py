import os
os.environ["PYSPARK_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"



from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, corr,
    date_trunc, round as spark_round
)


spark = SparkSession.builder \
    .appName("MalariaInsights") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN_PATH = "hdfs://localhost:9000/malaria/clean/cases_parquet"
HDFS_INSIGHTS_PATH = "hdfs://localhost:9000/malaria/insights"

df = spark.read.parquet(HDFS_CLEAN_PATH)
df.cache()

print(f"[Insights] {df.count()} lignes chargées pour analyse.\n")

# =========================================================
# INSIGHT 1 — Taux de positivité par région
# =========================================================
print("=" * 60)
print("INSIGHT 1 — Taux de positivité par région")
print("=" * 60)

insight1 = (
    df.groupBy("region_id")
    .agg(
        count("*").alias("total_cases"),
        spark_sum(when(col("rapid_test_result") == "positive", 1).otherwise(0)).alias("positive_cases")
    )
    .withColumn(
        "positivity_rate_pct",
        spark_round((col("positive_cases") / col("total_cases")) * 100, 2)
    )
    .orderBy(col("positivity_rate_pct").desc())
)

insight1.show(truncate=False)
insight1.write.mode("overwrite").parquet(f"{HDFS_INSIGHTS_PATH}/positivity_by_region")

# =========================================================
# INSIGHT 2 — Corrélation pluviométrie / humidité vs positivité
# =========================================================
print("=" * 60)
print("INSIGHT 2 — Corrélation climat / positivité")
print("=" * 60)

df_numeric = df.withColumn(
    "is_positive", when(col("rapid_test_result") == "positive", 1).otherwise(0)
)

corr_rainfall = df_numeric.stat.corr("rainfall_mm", "is_positive")
corr_humidity = df_numeric.stat.corr("humidity", "is_positive")
corr_temp = df_numeric.stat.corr("ambient_temperature", "is_positive")

print(f"Corrélation pluviométrie <-> positivité : {corr_rainfall:.4f}")
print(f"Corrélation humidité     <-> positivité : {corr_humidity:.4f}")
print(f"Corrélation température  <-> positivité : {corr_temp:.4f}")

insight2 = spark.createDataFrame([
    ("rainfall_mm", float(corr_rainfall)),
    ("humidity", float(corr_humidity)),
    ("ambient_temperature", float(corr_temp)),
], ["feature", "correlation_with_positivity"])

insight2.write.mode("overwrite").parquet(f"{HDFS_INSIGHTS_PATH}/climate_correlation")

# =========================================================
# INSIGHT 3 — Répartition démographique des cas positifs (âge/genre)
# =========================================================
print("=" * 60)
print("INSIGHT 3 — Répartition démographique des cas positifs")
print("=" * 60)

positive_cases = df.filter(col("rapid_test_result") == "positive")

insight3 = (
    positive_cases
    .withColumn(
        "age_group",
        when(col("patient_age") < 5, "0-4 (enfants)")
        .when(col("patient_age") < 15, "5-14")
        .when(col("patient_age") < 45, "15-44")
        .when(col("patient_age") < 65, "45-64")
        .otherwise("65+")
    )
    .groupBy("age_group", "gender")
    .agg(count("*").alias("positive_case_count"))
    .orderBy("age_group", "gender")
)

insight3.show(truncate=False)
insight3.write.mode("overwrite").parquet(f"{HDFS_INSIGHTS_PATH}/demographics")

# =========================================================
# INSIGHT 4 (bonus) — Délai symptômes -> nombre de symptômes moyen par résultat
# =========================================================
print("=" * 60)
print("INSIGHT 4 (bonus) — Nombre moyen de symptômes par résultat")
print("=" * 60)

insight4 = (
    df.groupBy("rapid_test_result")
    .agg(spark_round(avg("symptom_count"), 2).alias("avg_symptom_count"))
)
insight4.show(truncate=False)

print("\n[Insights] Terminé. Résultats écrits dans HDFS sous /malaria/insights/")

spark.stop()