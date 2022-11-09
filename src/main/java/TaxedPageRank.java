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

public class TaxedPageRank {

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
        JavaRDD<MatrixEntry> linkEntries = links.flatMap(link -> {
            ArrayList<MatrixEntry> es = new ArrayList<MatrixEntry>();
            for (long i : link._2()) {
                es.add(new MatrixEntry(link._1(), i, 1.0 / link._2().length));
            }
            return es.iterator();
        });
        SparseMatrix sparseLinkMatrix = new SparseMatrix((int) numPages, (int) numPages,
                Arrays.stream(linkEntries.map(x -> {
                    return (int) x.j();
                }).collect().toArray()).mapToInt(n -> Integer.parseInt(n.toString())).toArray(),
                Arrays.stream(linkEntries.map(x -> {
                    return (int) x.i();
                }).collect().toArray()).mapToInt(n -> Integer.parseInt(n.toString())).toArray(),
                Arrays.stream(linkEntries.map(x -> {
                    return (int) x.value();
                }).collect().toArray()).mapToDouble(n -> Double.parseDouble(n.toString())).toArray());
        JavaRDD<Double> ranks = links.map(link -> 1.0 / numPages);
        DenseVector rankVector = new DenseVector(
                Arrays.stream(ranks.collect().toArray()).mapToDouble(n -> Double.parseDouble(n.toString())).toArray());
        for (int i = 0; i < 25; i++) {
            double[] newRankVector = sparseLinkMatrix.multiply(rankVector).values();
            for (int j = 0; j < newRankVector.length; i++) {
                newRankVector[j] = (newRankVector[j] * 0.85) + (0.15 / numPages);
            }
            rankVector = new DenseVector(newRankVector);
        }
        JavaPairRDD<Long, Double> ranksSorted = sc.parallelize(Arrays.asList(ArrayUtils.toObject(rankVector.values())))
                .zipWithIndex().sortByKey().mapToPair(x -> new Tuple2<>(x._2(), x._1()));
        JavaRDD<Tuple2<Double, String>> ranksTitledSorted = ranksSorted.join(titles).values();
        ranksTitledSorted.saveAsTextFile(args[2]);
    }
}