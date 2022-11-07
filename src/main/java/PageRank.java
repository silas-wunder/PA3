import java.util.Arrays;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class PageRank {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> linksRaw = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<Long, long[]> links = linksRaw
                .mapToPair(s -> new Tuple2<Long, long[]>(Long.parseLong(s.split(": ")[0]),
                        Arrays.stream(s.split(": ")[1].split(" ")).mapToLong(Long::parseLong).toArray()));
        JavaPairRDD<Long, String> titles = spark.read().textFile(args[1]).javaRDD().zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._2(), x._1())).cache();
        // TODO: I'm pretty sure this is wrong, should be changed
        RDD<MatrixEntry> entries = links.flatMap(l -> {
            ArrayList<MatrixEntry> es = new ArrayList<MatrixEntry>();
            for (long i : l._2()) {
                es.add(new MatrixEntry(l._1(), i, 1.0 / l._2().length));
            }
            return es.iterator();
        }).rdd();
        CoordinateMatrix linkMatrix = new CoordinateMatrix(entries, titles.count(), titles.count());
    }
}
