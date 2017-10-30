import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PureNaiveBayes {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        new PureNaiveBayes().Action();

    }

    private void Action() throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("ActionNAIVEBAYES");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Dataset<Row> rescaledData = getTFIDF();

        /*JavaRDD<Row> rows = rescaledData.toJavaRDD();

        JavaRDD<LabeledPoint> inputData = rows.map(f -> new LabeledPoint(getSkillCategory(f.getAs("label")), f.getAs("features")));

        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
        JavaRDD<LabeledPoint> training = tmp[0]; // training set
        JavaRDD<LabeledPoint> test = tmp[1]; // test set
        */
        NaiveBayes naiveBayes = new NaiveBayes();
        NaiveBayesModel model = naiveBayes.train(rescaledData);
        /*JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
        System.out.println("The overall accuracy of the model is " + accuracy);
        */
        jsc.stop();
    }

    private static Dataset<Row> getTFIDF() throws IOException {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        //Dataset<String> logData = spark.read().textFile("/home/ab/Documents/job_listings_text_small/").cache();

        File folder = new File("/home/ab/Documents/job_listings_text_small");
        List<Row> data = new ArrayList<>();
        for (File file : folder.listFiles()) {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = "";
            String doc = "";
            while ((line = bufferedReader.readLine()) != null) {
                doc += line;
            }
            if (bufferedReader != null) bufferedReader.close();
            Row row = RowFactory.create(file.getName(), doc);
            data.add(row);
        }
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.StringType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered");
        Dataset<Row> wordsData1 = remover.transform(wordsData);

        HashingTF hashingTF = new HashingTF()
                .setInputCol("filtered")
                .setOutputCol("rawFeatures");
        Dataset<Row> featurizedData = hashingTF.transform(wordsData1);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        return rescaledData;
    }

    private static Double getSkillCategory(String fileName) throws SQLException {
        Double category = 0.0; //for Non IT
        DB db = new DB();
        String listingID = null;
        String clusterType = null;
        try {
            listingID = fileName.split(".")[0].replaceAll("listing", "");
        } catch (Exception e) {
            System.out.println(fileName);
        }
        if (listingID != null) {
            String sql = "select distinct cluster_name from job_listing_clusters where id in (select listing_cluster.cluster_id from listing_cluster where listing_id = " + listingID + ");";
            ResultSet resultSet = db.runSql(sql);
            while (resultSet.next()) {
                clusterType = resultSet.getString(1);
            }
        }
        if (clusterType != null) {
            switch (clusterType) {
                case "IT Skills":
                    category = 1.0;
                    break;
                default:
                    category = 0.0;
                    break;
            }
        }
        return category;

    }
}
