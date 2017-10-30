import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ActionNAIVEBAYES {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        SparkConf sparkConf = new SparkConf().setAppName("ActionNAIVEBAYES");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Dataset<Row> rescaledData = getTFIDF();
        rescaledData.show(3, true);
        //rescaledData.foreach((ForeachFunction<Row>) row -> getfeatures(row));
    }

    private static void getfeatures(Row row) {
        System.out.println(row.toString());
        System.err.println(">>>>>>>>>>>>>>>>|||||||||||||||||<<<<<<<<<<<<<");
        //Seq<Object> objectSeq = row.toSeq();
        SparseVector features = row.getAs("features");
        double[] values = features.values();
        for (double value : values) {
            System.out.println(value);
        }
        System.err.println(">>>>>>>>>>>>>>>><<<<<<<<<<<<<");

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
}
