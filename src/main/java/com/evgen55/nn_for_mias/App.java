package com.evgen55.nn_for_mias;

import com.evgen55.nn_for_mias.data.MiasLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static com.evgen55.nn_for_mias.data.LabelsMaker.BACKGROUND_TISSUE_MAPPING_SIZE;

/**
 * Entry point to operate with MIAS dataset
 * <p>
 * -Xmx16G
 *
 * @author <a href="mailto:i.dolende@gmail.com">Evgenii Lartcev</a>
 */
public class App {

    private static final String PATH_TO_MIAS_DATASET = "load from http://peipa.essex.ac.uk/info/mias.html to folder on local file system or HDFS";

    public static void main(String[] args) throws IOException {
        final SparkConf sparkConf = new SparkConf(true)
                .set("spark.driver.maxResultSize", "2500M");

        final SparkSession sparkSession = SparkSession.builder()
                .appName("mias")
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate();

        final MiasLoader miasLoader = new MiasLoader(sparkSession);

        final Dataset<Row> rowDataset = miasLoader
                .load(PATH_TO_MIAS_DATASET);

        final Dataset<Row>[] datasets = rowDataset.randomSplit(new double[]{0.6, 0.4}, 1234L);
        final Dataset<Row> train = datasets[0];
        final Dataset<Row> test = datasets[1];

        final int[] layers = new int[]{1024 * 1024, 100, BACKGROUND_TISSUE_MAPPING_SIZE};
        final MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier("Evgen55")
                .setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(5);

        final MultilayerPerceptronClassificationModel model = trainer.fit(train);

        //evaluate
        final Dataset<Row> result = model.transform(test);
        final Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        predictionAndLabels.show();
        final MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels));
    }
}
