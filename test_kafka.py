from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9009") \
    .option("kafka.security.protocol", "SSL") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "topic1") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()

def func_call(df, batch_id):
    df.selectExpr("CAST(value AS STRING) as json")
    requests = df.rdd.map(lambda x: x.value).collect()
    df.show()
    logging.info(requests)
    
    
query = kafka_df.writeStream \
    .format(HiveWarehouseSession.STREAM_TO_STREAM) \
    .foreachBatch(func_call) \
    .option("checkpointLocation","file://F:/tmp/kafka/checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start().awaitTermination()
