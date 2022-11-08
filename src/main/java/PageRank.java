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
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.rdd.RDD;

public class PageRank {

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
        // TODO: maybe not wrong? someone should take a look at this tho
        RDD<MatrixEntry> linkEntries = links.flatMap(link -> {
            ArrayList<MatrixEntry> es = new ArrayList<MatrixEntry>();
            for (long i : link._2()) {
                es.add(new MatrixEntry(link._1(), i, 1.0 / link._2().length));
            }
            return es.iterator();
        }).rdd();
        CoordinateMatrix linkMatrix = new CoordinateMatrix(linkEntries, titles.count(), titles.count());
        JavaRDD<Double> ranks = links.map(link -> 1.0 / titles.count());
        DenseVector rankVector = new DenseVector(ArrayUtils.toPrimitive((Double[]) ranks.collect().toArray()));
        for (int i = 0; i < 25; i++) {
            rankVector = linkMatrix.toBlockMatrix().toLocalMatrix().multiply(rankVector);
        }
        JavaPairRDD<Long, Double> ranksSorted = sc.parallelize(Arrays.asList(ArrayUtils.toObject(rankVector.values())))
                .zipWithIndex().sortByKey().mapToPair(x -> new Tuple2<>(x._2(), x._1()));
        JavaRDD<Tuple2<Double, String>> ranksTitledSorted = ranksSorted.join(titles).values();
        ranksTitledSorted.saveAsTextFile(args[2]);
    }
}