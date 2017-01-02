

from pyspark import SparkConf
from pyspark import SparkContext

sc = SparkContext("local", "wordCount")

text_file = sc.textFile(inputFile) //Load our input data.
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(outputFile)


