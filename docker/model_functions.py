import findspark

findspark.init()

from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel
import configs

spark = (
    SparkSession.builder.appName("API")
    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
    .config("spark.hadoop.fs.s3a.signing-algorithm", "")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", configs.ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", configs.SECRET_KEY)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_model():
    global model
    model = PipelineModel.load(configs.MODEL_PATH)


def dict_to_dataframe(row):
    row = Row(**row)
    df = spark.createDataFrame([row])
    return df


def get_prob(row):
    df = dict_to_dataframe(row)

    result_df = model.transform(df)
    prob = result_df.collect()[0].probability[1]
    return prob
