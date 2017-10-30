import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Action {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        Dataset<String> logData = spark.read().textFile("/home/ab/Documents/job_listings_text_small/").cache();
        long numAs = logData.filter(s -> s.contains("good")).count();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Lines with good: " + numAs);
        spark.stop();
    }
}
