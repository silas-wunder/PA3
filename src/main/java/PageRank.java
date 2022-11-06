import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class PageRank {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
        JavaRDD<String> links = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> titles = spark.read().textFile(args[1]).javaRDD();
    }

}
