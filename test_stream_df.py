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


process = df.selectExpr("split(value, ' ')[0] as col1", "split(value, ' ')[1] as col2", "split(value, ' ')[2] as col3"
                       "split(value, ' ')[3] as col1", "split(value, ' ')[4] as col2", "split(value, ' ')[5] as col3"
                       "split(value, ' ')[6] as col1", "split(value, ' ')[7] as col2", "split(value, ' ')[8] as col3"
                       "split(value, ' ')[9] as col1", "split(value, ' ')[10] as col2", "split(value, ' ')[11] as col3")

query = process \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
