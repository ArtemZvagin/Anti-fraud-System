from pyspark import ml
from pyspark.sql import DataFrame
from pyspark.sql.functions import hour
from pyspark.ml import Transformer
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

class FeatureCreator(Transformer, ml.util.DefaultParamsWritable, ml.util.DefaultParamsReadable):
    """
    A custom Transformer which create new features
    """
    
    def _transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('hour', hour(df.tx_datetime))
        df = df.withColumn('time_of_day', (df['hour'] / 6).cast('integer'))
        return df  
    

def train_model(df: DataFrame):
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


    tvs_model = tvs.fit(df)

    val_metrics = tvs_model.validationMetrics
    params = tvs_model.getEstimatorParamMaps()
    val_metrics_and_params = list(zip(val_metrics, params))
    val_metrics_and_params.sort(key=lambda x: x[0], reverse=True)
    best_val_metric, best_params = val_metrics_and_params[0]

    return {'val_metric': best_val_metric, 'params': best_params, 'model': tvs_model.bestModel}



def compute_bootstrap_metric(predictions: DataFrame, n_bootstraps: int=100) -> list:
    evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC').setLabelCol('tx_fraud')

    metric = []
    for _ in range(n_bootstraps):
        sampled = predictions.sample(withReplacement=True, fraction=1.0)
        metric.append(evaluator.evaluate(sampled))

    return metric