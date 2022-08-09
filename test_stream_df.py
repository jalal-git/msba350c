from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import json

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

#avg_open = df.agg({'open': 'mean'}).collect()


#wordsDF = df.select(explode(split(df("value")," "))).show()

process = df.selectExpr("split(value, ' ')[0] as open time", "split(value, ' ')[1] as open", "split(value, ' ')[2] as high", 
                       "split(value, ' ')[3] as low", "split(value, ' ')[4] as close", "split(value, ' ')[5] as volume",
                       "split(value, ' ')[6] as close time")

query = process \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
