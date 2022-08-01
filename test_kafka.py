from pyspark.sql import SparkSession

# create spark session
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingKafkaExample") \
    .getOrCreate()

# create kafka streaming
kafkaDF = (spark
  .readStream
  .option("kafka.bootstrap.servers", "your.server.name:port")
  .option("subscribe", "en")
  .format("kafka")
  .load()
)

kafkaDF.show()
