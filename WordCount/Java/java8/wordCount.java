

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class JavaSpark {

    public static void main(String args[]) throws Exception {
        String inputFile = args[0];
        String outputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairRDD<String, Integer> counts = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile(outputFile);

    }
}


