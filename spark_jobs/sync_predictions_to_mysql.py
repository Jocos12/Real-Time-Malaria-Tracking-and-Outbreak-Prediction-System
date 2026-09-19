import os
os.environ["PYSPARK_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"

from pyspark.sql import SparkSession

# --- Config MySQL (adapte user/password si besoin) ---
MYSQL_URL = "jdbc:mysql://localhost:3308/malaria_db?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_USER = "malaria_user"
MYSQL_PASSWORD = "Prince_Jocos9"   # <-- remplace par ton vrai mot de passe
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

spark = SparkSession.builder \
    .appName("SyncPredictionsToMySQL") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.jars", r"J:\Courses\Masters\Courses\Big_Data_Essentiels\Final_Exam\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

jdbc_props = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": MYSQL_DRIVER,
}

# --- 1. Sync case_predictions ---
print("[Sync] Lecture des prédictions individuelles (case_risk) depuis HDFS...")
case_df = spark.read.parquet("hdfs://localhost:9000/malaria/predictions/case_risk")
print(f"[Sync] {case_df.count()} lignes à écrire dans case_predictions.")

case_df.write \
    .mode("overwrite") \
    .jdbc(url=MYSQL_URL, table="case_predictions", properties=jdbc_props)

print("[Sync] case_predictions mis à jour dans MySQL.")

# --- 2. Sync outbreak_predictions ---
print("[Sync] Lecture des prédictions de flambée (outbreak_risk) depuis HDFS...")
outbreak_df = spark.read.parquet("hdfs://localhost:9000/malaria/predictions/outbreak_risk")
print(f"[Sync] {outbreak_df.count()} lignes à écrire dans outbreak_predictions.")

outbreak_df.write \
    .mode("overwrite") \
    .jdbc(url=MYSQL_URL, table="outbreak_predictions", properties=jdbc_props)

print("[Sync] outbreak_predictions mis à jour dans MySQL.")

spark.stop()
print("[Sync] Terminé.")