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
ssc = StreamingContext(sc, 10)
# add checkpoint
ssc.checkpoint("checkpoint_App")

lines = ssc.socketTextStream("localhost",9009)
print("connected!")
values = lines.flatMap(lambda line: line[0])


values.pprint()


print(values)
ssc.start()
ssc.awaitTermination()
ssc.stop()
