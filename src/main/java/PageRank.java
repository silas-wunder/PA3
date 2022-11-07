import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class PageRank {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        JavaRDD<String> linksRaw = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> titles = spark.read().textFile(args[1]).javaRDD();

        // TODO: write mapping function to get link representation right
        JavaPairRDD<String, Iterable<String>> links = linksRaw.mapToPair(null);
    }

}
