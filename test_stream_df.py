from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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

df.createOrReplaceTempView("data")

sqlDF = spark.sql("""

select *,
  avg(close) OVER(ORDER BY "open time"
  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )
     as 2day_moving_averagee,

  avg(close) OVER(ORDER BY "open time"
  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW )
      as 30day_moving_average


from data""")

sqlDF.show()