from pyspark.sql import SparkSession

# create spark session
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingKafkaExample") \
    .getOrCreate()

# create kafka streaming
kafkaDF = (spark
  .readStream
  .option("kafka.bootstrap.servers", "localhost:9009")
  .option("subscribe", "en")
  .format("kafka")
  .load()
)

kafkaDF.show()
