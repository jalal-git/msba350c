from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType
import re
import json

def get_num(string):
    pattern = "\d+\.\d+" if '.' in string else "\d+"
    return float(re.findall(pattern, string)[0])

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9009) \
    .load()

#avg_open = df.agg({'open': 'mean'}).collect()


#wordsDF = df.select(explode(split(df("value")," "))).show()

df = df.selectExpr("split(value, ' ')[0] as open_time", "split(value, ' ')[1] as open", "split(value, ' ')[2] as high", 
                       "split(value, ' ')[3] as low", "split(value, ' ')[4] as close", "split(value, ' ')[5] as volume",
                       "split(value, ' ')[6] as close_time")

to_float = f.udf(lambda v: get_num(v), FloatType())

df = df.select([to_float(c).alias(c) for c in df.columns])

#df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d'))

query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
