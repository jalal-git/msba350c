from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

df = spark.readStream \
      .format("socket") \
      .option("host","localhost") \
      .option("port","9009") \
      .load()

query = (df.show().writeStream.outputMode("complete").format("console").queryName("counts").start())
query.awaitTermination() 
spark.stop()
