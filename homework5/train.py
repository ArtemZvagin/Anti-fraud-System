import os
import mlflow

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from create_features import FeatureCreator

spark = (
    SparkSession
        .builder
        .appName("App")
        .getOrCreate()
)

schema = StructType([
         StructField('transaction_id', IntegerType(), True),
         StructField('tx_datetime', TimestampType(), True),
         StructField('customer_id', IntegerType(), True),
         StructField('terminal_id', IntegerType(), True),
         StructField('tx_amount', DoubleType(), True),
         StructField('tx_time_seconds', IntegerType(), True),
         StructField('tx_time_days', IntegerType(), True),
         StructField('tx_fraud', IntegerType(), True),
         StructField('tx_fraud_scenario', IntegerType(), True)
         ])

df = spark.read.parquet("s3a://otus-course/small_dataset.parquet", schema=schema, header=False)



feature_creator = FeatureCreator()

catColumns = ['time_of_day']
outputCatColumns = [col + '_ohe' for col in catColumns]
encoder = OneHotEncoder(inputCols=catColumns, outputCols=outputCatColumns)


numericColumns = ['tx_amount', 'terminal_id', 'hour']
assembler = VectorAssembler(inputCols=numericColumns + outputCatColumns, outputCol='features')


scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
model = GBTClassifier(labelCol="tx_fraud", featuresCol="scaled_features")

pipeline = Pipeline(stages=[feature_creator, encoder, assembler, scaler, model])


paramGrid = ParamGridBuilder()\
    .addGrid(model.maxDepth, [5, 10])\
    .addGrid(model.maxIter, [10, 20, 30])\
    .addGrid(model.subsamplingRate, [0.5, 0.7, 1])\
    .build()


tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=BinaryClassificationEvaluator(metricName='areaUnderROC', labelCol='tx_fraud'),
    trainRatio=0.8,
    seed=42
)

mlflow.set_experiment("antifraud-experiment")

with mlflow.start_run() as run:
    tvs_model = tvs.fit(df)

    metrics = tvs_model.validationMetrics
    params = tvs_model.getEstimatorParamMaps()
    metrics_and_params = list(zip(metrics, params))
    metrics_and_params.sort(key=lambda x: x[0], reverse=True)
    best_metric, best_params = metrics_and_params[0]

    mlflow.log_params(best_params)
    mlflow.log_metric('roc_auc', best_metric)

    bestML = tvs_model.bestModel
    #mlflow.spark.log_model(bestML, 'GBTClassifier_model', code_paths=['create_features.py'])

    save_path = f's3a://{os.getenv("SAVE_MODEL_BUCKET")}/models/{run.info.run_id}.parquet'
    mlflow.set_tag("model_path", save_path)
    bestML.save(save_path)

spark.stop()