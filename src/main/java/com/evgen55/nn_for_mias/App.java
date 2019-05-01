package com.evgen55.nn_for_mias;

import com.evgen55.nn_for_mias.data.MiasLoader;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("mias")
                .master("local[*]")
                .getOrCreate();

        MiasLoader miasLoader = new MiasLoader(sparkSession);

        miasLoader.load("/home/evgen/Development/1_Under_VCS/github/4_NN_ML/data_for_trainings/all-mias");
    }
}
