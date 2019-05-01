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

public class App {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf(true)
                .set("spark.driver.maxResultSize", "2500M");
        SparkSession sparkSession = SparkSession.builder()
                .appName("mias")
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate();

        MiasLoader miasLoader = new MiasLoader(sparkSession);

        Dataset<Row> rowDataset = miasLoader
                .load("/home/evgen/Development/1_Under_VCS/github/4_NN_ML/data_for_trainings/all-mias");

        Dataset<Row>[] datasets = rowDataset.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = datasets[0];
        Dataset<Row> test = datasets[1];

        final int[] layers = new int[]{1024 * 1024, 100, 1}; // TODO: 01.05.19 labels
//        final int[] layers = new int[]{4, 5, 9, 3, 4};
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
