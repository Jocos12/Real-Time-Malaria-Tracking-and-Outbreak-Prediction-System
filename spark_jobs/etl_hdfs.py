import os
os.environ["PYSPARK_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, size

# --- Initialisation Spark ---
spark = SparkSession.builder \
    .appName("MalariaETL") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_RAW_PATH = "hdfs://localhost:9000/malaria/raw/malaria-cases/*/*/*/*.json"
HDFS_CLEAN_PATH = "hdfs://localhost:9000/malaria/clean/cases_parquet"

print(f"[ETL] Lecture des données brutes depuis {HDFS_RAW_PATH} ...")

# --- Lecture des JSON bruts écrits par Kafka Connect HDFS Sink ---
df_raw = spark.read.json(HDFS_RAW_PATH)

print(f"[ETL] {df_raw.count()} lignes brutes lues.")
df_raw.printSchema()

# --- Nettoyage ---
df_clean = (
    df_raw
    # Dédoublonnage par case_id (au cas où un message aurait été retraité)
    .dropDuplicates(["case_id"])
    # Cast du timestamp string -> vrai type timestamp
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    # On retire les lignes avec des champs essentiels manquants
    .filter(col("case_id").isNotNull())
    .filter(col("region_id").isNotNull())
    .filter(col("rapid_test_result").isNotNull())
    # Normalisation du résultat de test (au cas où variantes de casse)
    .withColumn(
        "rapid_test_result",
        when(col("rapid_test_result").isin("positive", "Positive", "POSITIVE"), "positive")
        .when(col("rapid_test_result").isin("negative", "Negative", "NEGATIVE"), "negative")
        .otherwise(col("rapid_test_result"))
    )
    # Nombre de symptômes (utile pour les insights et le futur ML)
    .withColumn("symptom_count", size(col("symptoms")))
)

print(f"[ETL] {df_clean.count()} lignes après nettoyage/dédoublonnage.")

# --- Écriture en Parquet dans HDFS (format optimisé pour la suite) ---
df_clean.write.mode("overwrite").parquet(HDFS_CLEAN_PATH)

print(f"[ETL] Données propres écrites dans {HDFS_CLEAN_PATH}")

spark.stop()