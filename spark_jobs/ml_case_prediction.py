import os
os.environ["PYSPARK_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\malaria-bigdata-project\venv\Scripts\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

MYSQL_JAR = r"J:\Courses\Masters\Courses\Big_Data_Essentiels\Final_Exam\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar"

spark = SparkSession.builder \
    .appName("MalariaCaseRiskModel") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.jars", MYSQL_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_CLEAN_PATH = "hdfs://localhost:9000/malaria/clean/cases_parquet"
MODEL_PATH = "hdfs://localhost:9000/malaria/models/case_risk_rf"
PRED_PATH = "hdfs://localhost:9000/malaria/predictions/case_risk"

MYSQL_URL = "jdbc:mysql://localhost:3308/malaria_db?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_PROPS = {
    "user": "malaria_user",
    "password": "Prince_Jocos9",
    "driver": "com.mysql.cj.jdbc.Driver",
}

print("[ML-Case] Chargement des données...")
df = spark.read.parquet(HDFS_CLEAN_PATH)

# --- Label : 1 si positif, 0 sinon ---
df = df.withColumn("label", when(col("rapid_test_result") == "positive", 1.0).otherwise(0.0))

# --- Encodage des variables catégorielles ---
region_indexer = StringIndexer(inputCol="region_id", outputCol="region_idx", handleInvalid="keep")
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")

feature_cols = [
    "patient_age", "region_idx", "gender_idx",
    "body_temperature", "ambient_temperature", "humidity", "rainfall_mm",
    "symptom_count"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

rf = RandomForestClassifier(
    labelCol="label", featuresCol="features",
    numTrees=100, maxDepth=8, seed=42
)

pipeline = Pipeline(stages=[region_indexer, gender_indexer, assembler, rf])

# --- Split train/test ---
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"[ML-Case] Train: {train_df.count()} lignes | Test: {test_df.count()} lignes")

print("[ML-Case] Entraînement du modèle RandomForest...")
model = pipeline.fit(train_df)

# --- Évaluation ---
predictions = model.transform(test_df)

auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = auc_eval.evaluate(predictions)

f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1 = f1_eval.evaluate(predictions)

precision_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
precision = precision_eval.evaluate(predictions)

recall_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
recall = recall_eval.evaluate(predictions)

acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = acc_eval.evaluate(predictions)

print("=" * 60)
print("MODELE 1 - PREDICTION DE RISQUE PAR CAS INDIVIDUEL")
print("=" * 60)
print(f"AUC       : {auc:.4f}")
print(f"Accuracy  : {accuracy:.4f}")
print(f"F1-score  : {f1:.4f}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")

# --- Feature importance (utile pour le dashboard "prediction explanation") ---
rf_model = model.stages[-1]
importances = rf_model.featureImportances
print("\nImportance des features :")
for name, imp in zip(feature_cols, importances.toArray()):
    print(f"  {name}: {imp:.4f}")

# --- Sauvegarde du modèle (déploiement) ---
model.write().overwrite().save(MODEL_PATH)
print(f"\n[ML-Case] Modèle sauvegardé dans {MODEL_PATH}")

# --- Prédictions réelles sur toutes les données (simulation batch scoring) ---
all_predictions = model.transform(df).select(
    "case_id", "region_id", "patient_age", "gender", "label", "prediction"
)
all_predictions.write.mode("overwrite").parquet(PRED_PATH)
print(f"[ML-Case] Prédictions écrites dans {PRED_PATH}")

# --- Écriture de la feature importance RÉELLE dans MySQL ---
importance_rows = [
    ("case_risk", name, float(score))
    for name, score in zip(feature_cols, importances.toArray())
]
importance_df = spark.createDataFrame(importance_rows, ["model_name", "feature_name", "importance"])

# On supprime d'abord les anciennes lignes de ce modèle pour éviter les doublons à chaque relance
existing = spark.read.jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)
if existing.filter(existing.model_name == "case_risk").count() > 0:
    print("[ML-Case] Anciennes lignes 'case_risk' détectées — écriture en overwrite complet de la table.")
    remaining = existing.filter(existing.model_name != "case_risk").drop("id", "trained_at")
    combined = remaining.unionByName(importance_df, allowMissingColumns=True)
    combined.write.mode("overwrite").jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)
else:
    importance_df.write.mode("append").jdbc(url=MYSQL_URL, table="feature_importance", properties=MYSQL_PROPS)

print("[ML-Case] Feature importance écrite dans MySQL (table feature_importance).")

spark.stop()