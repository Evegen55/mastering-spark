package com.evgen55;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;

@Slf4j
public class MultylayerPerceptronExample {


    public static void main(String[] args) {

        MultilayerPerceptronClassificationModel model;

        final int[] layers = new int[]{4, 3, 4};

        MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier("My MultilayerPerceptronClassifier")
                .setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(10);

    }
}
