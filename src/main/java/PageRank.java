import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class PageRank {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        JavaRDD<String> linksRaw = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<Integer, Iterable<Integer>> links = linksRaw.mapToPair(s -> {
            String[] parts = s.split(": ");
            Iterable<Integer> tos = Arrays.asList(
                    Arrays.stream(parts[1].split(" ")).mapToInt(Integer::parseInt).boxed().toArray(Integer[]::new));
            return new Tuple2<Integer, Iterable<Integer>>(Integer.parseInt(parts[0]), tos);
        });
        JavaPairRDD<Long, String> titles = spark.read().textFile(args[1]).javaRDD().zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._2(), x._1())).cache();
    }

}
