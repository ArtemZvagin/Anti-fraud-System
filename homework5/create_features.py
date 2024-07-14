from pyspark import ml
from pyspark.sql import DataFrame
from pyspark.sql.functions import hour
from pyspark.ml import Transformer

class FeatureCreator(Transformer, ml.util.DefaultParamsWritable, ml.util.DefaultParamsReadable):
    """
    A custom Transformer which create new features
    """
    
    def _transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('hour', hour(df.tx_datetime))
        df = df.withColumn('time_of_day', (df['hour'] / 6).cast('integer'))
        return df  