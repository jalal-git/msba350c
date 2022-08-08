from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import time

# create spark configuration
conf = SparkConf()
conf.setAppName("StreamApp")

# Create a local StreamingContext with two working thread and batch interval of 1 second
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
# add checkpoint
ssc.checkpoint("checkpoint_App")

lines = ssc.socketTextStream("localhost",9009)
print("connected!")
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
print(pairs)
ssc.start()
ssc.awaitTermination()
ssc.stop()
