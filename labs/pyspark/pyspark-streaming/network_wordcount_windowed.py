r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: network_wordcount.py <hostname> <port> <checkpoint-dir>", file=sys.stderr)
        sys.exit(-1)
		
    sc = SparkContext(appName="PythonStreamingNetworkWordWindowedCount")
	
    ssc = StreamingContext(sc, 10)

    # Split each line into words
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # Checkpoint directory
    ssc.checkpoint(sys.argv[3])

    # Count each word in each batch
    words = lines.flatMap(lambda line: line.split(" "))
	
    pairs = words.map(lambda word: (word, 1))
	
    #Windowing Concept: Reduce last 30 seconds of data, every 10 seconds
    windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
	
    windowedWordCounts.pprint()

    ssc.start()
    ssc.awaitTermination()