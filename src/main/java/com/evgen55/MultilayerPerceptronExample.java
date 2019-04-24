package com.evgen55;

import com.evgen55.entities.LabeledBehaviour;
import com.evgen55.entities.Utils;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
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
        final MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier("My MultilayerPerceptronClassifier")
                .setLayers(layers)
                .setBlockSize(3)
                .setSeed(1234L)
                .setMaxIter(10);

        final MultilayerPerceptronClassificationModel model = trainer.fit(dataFrameTrain);

        evaluate(sparkSession, model);

        final double predict = model.predict(Vectors.dense(new double[]{2,0,1,3}));
        System.out.println(Utils.getMappingFrom((int) predict)); //3, hide

        final double predict1 = model.predict(Vectors.dense(new double[]{0,0,0,0}));
        System.out.println(Utils.getMappingFrom((int) predict1)); //0, attack

        final double predict2 = model.predict(Vectors.dense(new double[]{2,0,1,3}));
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
        double[] abilities = new double[4];
        return sparkSession.sqlContext().read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path)
                .map((MapFunction<Row, LabeledBehaviour>) row -> {
                    abilities[0] = (Integer) row.get(0);
                    abilities[1] = (Integer) row.get(1);
                    abilities[2] = (Integer) row.get(2);
                    abilities[3] = (Integer) row.get(3);
                    return new LabeledBehaviour(Vectors.dense(abilities), Utils.getMappingFrom(row.getAs("behaviour")));
                }, Encoders.bean(LabeledBehaviour.class))
                .toJavaRDD();
    }
}
