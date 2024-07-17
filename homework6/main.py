import os
import mlflow

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType
from pyspark.ml import PipelineModel
from scipy.stats import ttest_ind
from train_model import train_model, compute_bootstrap_metric


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
train_df, test_df = df.randomSplit(weights=[0.7, 0.3], seed=42)



experiment_name = "antifraud-experiment"
previous_runs = mlflow.search_runs(experiment_names=[experiment_name])
if len(previous_runs) != 0:
    best_run = previous_runs.sort_values(by='metrics.test_bootstrap_roc_auc', ascending=False).iloc[0]
    old_model_path = best_run['tags.model_path'] 
    old_model = PipelineModel.load(old_model_path)
    old_bootstrap_metric = compute_bootstrap_metric(old_model.transform(test_df))
else:
    old_bootstrap_metric = None



mlflow.set_experiment(experiment_name)


with mlflow.start_run() as run:
    trained = train_model(train_df)

    mlflow.log_params(trained['params'])
    mlflow.log_metric('val_roc_auc', trained['val_metric'])

    new_bootstrap_metric = compute_bootstrap_metric(trained['model'].transform(test_df))
    mlflow.log_metric('test_bootstrap_roc_auc', sum(new_bootstrap_metric) / len(new_bootstrap_metric))

    if old_bootstrap_metric:
        p_value = ttest_ind(old_bootstrap_metric, new_bootstrap_metric).pvalue
        mlflow.log_metric('p_value', p_value)


    save_path = f's3a://{os.getenv("SAVE_MODEL_BUCKET")}/models/{run.info.run_id}.parquet'
    mlflow.set_tag("model_path", save_path)
    trained['model'].save(save_path)


spark.stop()