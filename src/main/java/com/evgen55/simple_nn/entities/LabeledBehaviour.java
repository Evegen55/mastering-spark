package com.evgen55.simple_nn.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.ml.linalg.Vector;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LabeledBehaviour implements Serializable {

    private Vector features; //abilities
    private int label; //behaviour

}


