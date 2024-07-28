import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import configs


spark = (
    SparkSession
        .builder
        .appName("Kafka-Producer")
        .config("fs.s3a.endpoint", "storage.yandexcloud.net")
        .config("fs.s3a.signing-algorithm", "")
        .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("fs.s3a.access.key", configs.ACCESS_KEY)
        .config("fs.s3a.secret.key", configs.SECRET_KEY)
        .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')


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

df = spark.read.parquet(configs.DATA_PATH, schema=schema)
df_json = df.select(f.to_json(f.struct(*df.columns)).alias("value"))


while True:
    start = time.time()
    
    sampled_df = df_json.sample(withReplacement=False, fraction=0.1).limit(configs.MESSAGES_PER_SECOND)

    sampled_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{configs.IP}:9092,{configs.IP}:9093,{configs.IP}:9094") \
        .option("topic", configs.SOURCE_TOPIC) \
        .save()
    
    sleep_time = 1 - (time.time() - start)
    
    if sleep_time > 0:
        time.sleep(sleep_time)