

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


val conf = new SparkConf().setAppName("wordCount") // Create a Scala Spark Context.
val sc = new SparkContext(conf)

val input = sc.textFile(inputFile)// Load our input data.

val words = input.flatMap(line => line.split(" ")) // Split it up into words.

val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y} // Transform into pairs and count.

counts.saveAsTextFile(outputFile) // Save the word count back out to a text file, causing evaluation.



