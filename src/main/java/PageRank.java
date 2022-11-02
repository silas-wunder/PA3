import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class PageRank {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Page Rank").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.close();
    }

}
