import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RWPageRank {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        JavaRDD<String> linksRaw = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<Long, long[]> links = linksRaw
                .mapToPair(s -> new Tuple2<Long, long[]>(Long.parseLong(s.split(": ")[0]),
                        Arrays.stream(s.split(": ")[1].split(" ")).mapToLong(Long::parseLong)
                                .toArray()));
        JavaPairRDD<Long, String> titles = spark.read().textFile(args[1]).javaRDD().zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._2(), x._1())).cache();
        long numPages = titles.count();
        JavaPairRDD<Long, Double> ranks = links.mapValues(x -> 1.0 / numPages);
        for (int i = 0; i < 25; i++) {
            JavaPairRDD<Long, Double> tempRanks = links.join(ranks).values().flatMap(pair -> {
                ArrayList<Tuple2<Long, Double>> outs = new ArrayList<Tuple2<Long, Double>>();
                for (long link : pair._1()) {
                    outs.add(new Tuple2<Long, Double>(link, pair._2() / pair._1().length));
                }
                return outs.iterator();
            }).mapToPair(t -> new Tuple2<>(t._1(), t._2()));
            ranks = tempRanks.reduceByKey((x, y) -> x + y);
        }
        JavaPairRDD<String, Double> ranksSorted = ranks.join(titles).values()
                .mapToPair(x -> new Tuple2<>(x._1(), x._2()))
                .sortByKey(false)
                .mapToPair(x -> new Tuple2<>(x._2(), x._1()));
        ranksSorted.saveAsTextFile(args[2]);
    }
}
