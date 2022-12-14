from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import *
import re
import json
import datetime

def get_date(your_timestamp):
    return str(datetime.datetime.fromtimestamp(your_timestamp))


def get_num(string):
    pattern = "\d+\.\d+" if '.' in string else "\d+"
    return float(re.findall(pattern, string)[0]) if '.' in string else float(re.findall(pattern, string)[0])/1000

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



# get dataframe
df = df.selectExpr("split(value, ' ')[0] as open_time", "split(value, ' ')[1] as open", "split(value, ' ')[2] as high", 
                       "split(value, ' ')[3] as low", "split(value, ' ')[4] as close", "split(value, ' ')[5] as volume",
                       "split(value, ' ')[6] as close_time")

# clean data frame
to_float = f.udf(lambda v: get_num(v), FloatType())
df = df.select([to_float(c).alias(c) for c in df.columns])

# to timestamp
to_date = f.udf(lambda v: get_date(v), StringType())

# open time
df = df.withColumn('open_timestamp', to_date('open_time'))
df = df.withColumn('open_timestamp', df.open_timestamp.cast('timestamp'))

# close time
df = df.withColumn('close_timestamp', to_date('close_time'))
df = df.withColumn('close_timestamp', df.close_timestamp.cast('timestamp'))


# dropping columns
df = df.drop('open_time', 'close_time')

# #create window by casting timestamp to long (number of seconds)
windowedAvgSignalDF = \
  df \
    .groupBy(f.window("open_timestamp", "3 minutes", "1 minutes")) \
    .mean()


query = windowedAvgSignalDF \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
