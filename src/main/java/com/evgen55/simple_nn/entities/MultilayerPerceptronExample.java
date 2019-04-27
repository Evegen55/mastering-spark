package com.evgen55.simple_nn.entities;

import com.evgen55.simple_nn.entities.LabeledBehaviour;
import com.evgen55.simple_nn.entities.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class MultilayerPerceptronExample {

    public static void main(String[] args) throws InterruptedException {

        final SparkSession sparkSession = SparkSession.builder()
                .appName("")
                .master("local[*]")
                .getOrCreate();

        final String pathToTrainDataset = "src/main/resources/game_controller/train.csv";
        final Dataset<Row> dataFrameTrain = sparkSession.createDataFrame(getLabeledFeaturesFromCsv(sparkSession, pathToTrainDataset), LabeledBehaviour.class);
        dataFrameTrain.show();

        final int[] layers = new int[]{4, 3, 4}; //as in the book
//        final int[] layers = new int[]{4, 5, 9, 3, 4};
        final MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier("Evgen55")
                .setLayers(layers)
                .setBlockSize(3)
                .setSeed(1234L)
                .setMaxIter(5);

        final MultilayerPerceptronClassificationModel model = trainer.fit(dataFrameTrain);

        evaluate(sparkSession, model);

        final double predict = model.predict(Vectors.dense(new double[]{2, 0, 1, 3}));
        System.out.println(Utils.getMappingFrom((int) predict)); //3, hide

        final double predict1 = model.predict(Vectors.dense(new double[]{0, 0, 0, 0}));
        System.out.println(Utils.getMappingFrom((int) predict1)); //0, attack

        final double predict2 = model.predict(Vectors.dense(new double[]{2, 0, 1, 3}));
        System.out.println(Utils.getMappingFrom((int) predict2)); //3, hide

        Thread.sleep(100000000); //to see SparkUI
    }

    private static void evaluate(final SparkSession sparkSession, final MultilayerPerceptronClassificationModel model) {
        final String pathToTestDataset = "src/main/resources/game_controller/test.csv";
        final Dataset<Row> dataFrameTest = sparkSession.createDataFrame(getLabeledFeaturesFromCsv(sparkSession, pathToTestDataset), LabeledBehaviour.class);
        final Dataset<Row> result = model.transform(dataFrameTest);
        final Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        predictionAndLabels.show();
        final MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels));
    }

    private static JavaRDD<LabeledBehaviour> getLabeledFeaturesFromCsv(final SparkSession sparkSession, final String path) {
        //it's a trick to reuse array for each mapping operation for each newly created object
        //it's a valid behaviour only because all calculations are LAZY
        //if calculations is NOT LAZY  - it will behave in another way
        double[] abilities = new double[4];
        return sparkSession.sqlContext().read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path)
                .map((MapFunction<Row, LabeledBehaviour>) row -> {
//                    double[] abilities = new double[4]; //BE REALLY CAREFULLY - see #testArrayReusage()
                    abilities[0] = (Integer) row.get(0);
                    abilities[1] = (Integer) row.get(1);
                    abilities[2] = (Integer) row.get(2);
                    abilities[3] = (Integer) row.get(3);
                    return new LabeledBehaviour(Vectors.dense(abilities), Utils.getMappingFrom(row.getAs("behaviour")));
                }, Encoders.bean(LabeledBehaviour.class))
                .toJavaRDD();
    }

    private static void testArrayReusage() {
        double[] abilities = new double[4];
        List<double[]> doubleList = new ArrayList<>();
        abilities[0] = 1;
        doubleList.add(abilities);
        abilities[0] = 2;
        doubleList.add(abilities);
        System.out.println();
    }
}
