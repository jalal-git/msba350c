from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost",9009)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
print(pairs)
ssc.start()
ssc.awaitTermination()
ssc.stop()
