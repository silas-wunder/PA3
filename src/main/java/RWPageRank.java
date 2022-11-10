import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

public class RWPageRank {
  public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        SparkContext context = spark.sparkContext();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(context);
        JavaRDD<String> linksRaw = spark.read().textFile(args[0]).javaRDD();
        JavaPairRDD<Long, long[]> links = linksRaw
                .mapToPair(s -> new Tuple2<Long, long[]>(Long.parseLong(s.split(": ")[0]),
                        Arrays.stream(s.split(": ")[1].split(" ")).mapToLong(Long::parseLong).toArray()));
        JavaPairRDD<Long, String> titles = spark.read().textFile(args[1]).javaRDD().zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._2(), x._1())).cache();
        long numPages = titles.count();
    }
}
