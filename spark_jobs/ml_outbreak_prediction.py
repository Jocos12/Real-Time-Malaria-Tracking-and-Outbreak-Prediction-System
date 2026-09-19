import os
os.environ["PYSPARK_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, avg, count, window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

MYSQL_JAR = r"J:\Courses\Masters\Courses\Big_Data_Essentiels\Final_Exam\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar"

spark = SparkSession.builder \
    .appName("MalariaOutbreakModel") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.jars", MYSQL_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN_PATH = "hdfs://localhost:9000/malaria/clean/cases_parquet"
MODEL_PATH = "hdfs://localhost:9000/malaria/models/outbreak_gbt"
PRED_PATH = "hdfs://localhost:9000/malaria/predictions/outbreak_risk"

MYSQL_URL = "jdbc:mysql://localhost:3308/malaria_db?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_PROPS = {
    "user": "malaria_user",
    "password": "Prince_Jocos9",
    "driver": "com.mysql.cj.jdbc.Driver",
}

# NOTE PEDAGOGIQUE : en production, on agregerait par region + JOUR pour predire
# une flambee a 7 jours. Pour cette demo, le generateur n'a tourne que quelques
# minutes (donnees concentrees sur une seule heure), donc on agrege par region +
# fenetre de 1 MINUTE pour obtenir assez de lignes d'entrainement. C'est un choix
# de granularite adapte a la demo, pas au design final vise par le cahier des charges.
AGG_WINDOW = "1 minute"

print(f"[ML-Outbreak] Chargement et agrégation par région / fenêtre de {AGG_WINDOW}...")
df = spark.read.parquet(HDFS_CLEAN_PATH)

# --- Agrégation région + fenêtre temporelle ---
daily = (
    df.groupBy("region_id", window(col("timestamp"), AGG_WINDOW))
    .agg(
        count("*").alias("total_cases"),
        spark_sum(when(col("rapid_test_result") == "positive", 1).otherwise(0)).alias("positive_cases"),
        avg("rainfall_mm").alias("avg_rainfall"),
        avg("humidity").alias("avg_humidity"),
        avg("ambient_temperature").alias("avg_temperature"),
        avg("symptom_count").alias("avg_symptoms"),
    )
    .withColumn("date", col("window.start"))
    .drop("window")
    .cache()
)

n_rows = daily.count()
print(f"[ML-Outbreak] {n_rows} lignes région x fenêtre disponibles pour l'entraînement.")

# --- Seuil d'épidémie : > 75e percentile des positive_cases (adaptatif selon le volume de données) ---
threshold = daily.approxQuantile("positive_cases", [0.75], 0.01)[0]
print(f"[ML-Outbreak] Seuil de flambée (75e percentile) : {threshold} cas positifs/fenêtre")

daily = daily.withColumn(
    "is_outbreak", when(col("positive_cases") > threshold, 1.0).otherwise(0.0)
)

print("[ML-Outbreak] Distribution des labels :")
daily.groupBy("is_outbreak").count().show()

# --- Encodage région ---
region_indexer = StringIndexer(inputCol="region_id", outputCol="region_idx", handleInvalid="keep")

feature_cols = [
    "region_idx", "total_cases", "avg_rainfall",
    "avg_humidity", "avg_temperature", "avg_symptoms"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

gbt = GBTClassifier(
    labelCol="is_outbreak", featuresCol="features",
    maxIter=20, maxDepth=5, maxBins=32, seed=42
)

pipeline = Pipeline(stages=[region_indexer, assembler, gbt])

train_df, test_df = daily.randomSplit([0.8, 0.2], seed=42)
train_df = train_df.cache()
test_df = test_df.cache()
print(f"[ML-Outbreak] Train: {train_df.count()} lignes | Test: {test_df.count()} lignes")

print("[ML-Outbreak] Entraînement du modèle GBT...")
model = pipeline.fit(train_df)

predictions = model.transform(test_df)

auc_eval = BinaryClassificationEvaluator(labelCol="is_outbreak", metricName="areaUnderROC")
auc = auc_eval.evaluate(predictions)

f1_eval = MulticlassClassificationEvaluator(labelCol="is_outbreak", predictionCol="prediction", metricName="f1")
f1 = f1_eval.evaluate(predictions)

precision_eval = MulticlassClassificationEvaluator(labelCol="is_outbreak", predictionCol="prediction", metricName="weightedPrecision")
precision = precision_eval.evaluate(predictions)

recall_eval = MulticlassClassificationEvaluator(labelCol="is_outbreak", predictionCol="prediction", metricName="weightedRecall")
recall = recall_eval.evaluate(predictions)

print("=" * 60)
print("MODELE 2 - PREDICTION DE FLAMBEE EPIDEMIQUE PAR REGION / FENETRE")
print("=" * 60)
print(f"AUC       : {auc:.4f}")
print(f"F1-score  : {f1:.4f}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")

# --- Feature importance (utile pour le panneau d'explication du dashboard) ---
gbt_model = model.stages[-1]
importances = gbt_model.featureImportances
print("\nImportance des features :")
for name, importance in zip(feature_cols, importances.toArray()):
    print(f"  {name}: {importance:.4f}")

# --- Sauvegarde du modèle ---
model.write().overwrite().save(MODEL_PATH)
print(f"\n[ML-Outbreak] Modèle sauvegardé dans {MODEL_PATH}")

# --- Prédictions batch sur toutes les données (simulation "scoring quotidien") ---
all_predictions = model.transform(daily).select(
    "region_id", "date", "total_cases", "positive_cases", "is_outbreak", "prediction"
)
all_predictions.write.mode("overwrite").parquet(PRED_PATH)
print(f"[ML-Outbreak] Prédictions écrites dans {PRED_PATH}")

# --- Écriture de la feature importance RÉELLE dans MySQL ---
importance_rows = [
    ("outbreak", name, float(score))
    for name, score in zip(feature_cols, importances.toArray())
]
importance_df = spark.createDataFrame(importance_rows, ["model_name", "feature_name", "importance"])

existing = spark.read.jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)
if existing.filter(existing.model_name == "outbreak").count() > 0:
    print("[ML-Outbreak] Anciennes lignes 'outbreak' détectées — écriture en overwrite complet de la table.")
    remaining = existing.filter(existing.model_name != "outbreak").drop("id", "trained_at")
    combined = remaining.unionByName(importance_df, allowMissingColumns=True)
    combined.write.mode("overwrite").jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)
else:
    importance_df.write.mode("append").jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)

print("[ML-Outbreak] Feature importance écrite dans MySQL (table feature_importance).")

spark.stop()