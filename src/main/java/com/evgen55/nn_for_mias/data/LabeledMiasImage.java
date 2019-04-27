package com.evgen55.nn_for_mias.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LabeledMiasImage implements Serializable {

    private Vector features; //abilities
    private int label; //behaviour

    public LabeledMiasImage(final double[] singleDimArray, final int label) {
        features = Vectors.dense(singleDimArray);
        this.label = label;
    }
}