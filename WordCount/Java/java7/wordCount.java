

JavaRDD<String> lines = sc.textFile("hdfs://log.txt");
 
// Map each line to multiple words
JavaRDD<String> words = lines.flatMap(
  new FlatMapFunction<String, String>() {
    public Iterable<String> call(String line) {
      return Arrays.asList(line.split(" "));
    }
});
 
// Turn the words into (word, 1) pairs
JavaPairRDD<String, Integer> ones = words.mapToPair(
  new PairFunction<String, String, Integer>() {
    public Tuple2<String, Integer> call(String w) {
      return new Tuple2<String, Integer>(w, 1);
    }
});
 
// Group up and add the pairs by key to produce counts
JavaPairRDD<String, Integer> counts = ones.reduceByKey(
  new Function2<Integer, Integer, Integer>() {
    public Integer call(Integer i1, Integer i2) {
      return i1 + i2;
    }
});
 
counts.saveAsTextFile("hdfs://counts.txt");
