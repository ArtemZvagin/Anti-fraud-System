import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.ml import PipelineModel
import configs

spark = (SparkSession
             .builder
             .appName('Kafka-Consumer')
             .getOrCreate())

spark.sparkContext.setLogLevel('WARN')

model = PipelineModel.load(configs.MODEL_PATH)


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


source = (spark
          .readStream
          .format('kafka')
          .option("kafka.bootstrap.servers", f"{configs.IP}:9092,{configs.IP}:9093,{configs.IP}:9094") \
          .option('subscribe', configs.SOURCE_TOPIC)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load())


df = (source.selectExpr('CAST(value AS STRING)', 'offset'))
df = (df.select(f.from_json('value', schema).alias('data'))).select("data.*")

result_df = model.transform(df)

json_df = result_df.select(f.to_json(f.struct(["transaction_id", "probability", "prediction"])).alias("value"))


query = (json_df
         .writeStream
         .format('kafka') \
         .option("kafka.bootstrap.servers", f"{configs.IP}:9092,{configs.IP}:9093,{configs.IP}:9094") \
         .option('topic', configs.RESULT_TOPIC) \
         .option('checkpointLocation', './checkpoint'))
         

query.start().awaitTermination()