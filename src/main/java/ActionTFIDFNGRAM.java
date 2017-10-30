import org.apache.spark.ml.feature.*;
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

public class ActionTFIDFNGRAM {
    public static void main(String[] args) throws FileNotFoundException, IOException {
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
        NGram ngramTransformer = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");
        Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordsData);
        ngramDataFrame.select("ngrams").show(20);



        int numFeatures = 20;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);
        Dataset<Row> featurizedData = hashingTF.transform(ngramDataFrame);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        rescaledData.select("label", "features").show();
        spark.stop();
    }
}
