package com.evgen55.nn_for_mias.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MiasLoaderTest {

    private SparkSession sparkSession;

    @Before
    public void tests() {
        sparkSession = SparkSession.builder()
                .appName("mias")
                .master("local[*]")
                .getOrCreate();
    }

    @Test
    public void getLabeledMiasImage() throws IOException, InterruptedException {
        MiasLoader miasLoader = new MiasLoader(sparkSession);
        final String inputImagePath = "src/main/resources/dataset_mias/mdb001.pgm";
        miasLoader.getLabeledMiasImage(inputImagePath);
        Thread.sleep(1000000);
    }

    @Test
    public void readFromData() throws IOException, InterruptedException {
        MiasLoader miasLoader = new MiasLoader(sparkSession);
        String pathToMias = "/home/evgen/Development/1_Under_VCS/github/4_NN_ML/data_for_trainings/3-mias";
        Dataset<Row> rowDataset = miasLoader.readFromData(pathToMias);
        rowDataset.show();
        rowDataset.count();
        Thread.sleep(1000000);
    }

    @Test
    public void testLoad() throws IOException, InterruptedException {
        MiasLoader miasLoader = new MiasLoader(sparkSession);
        String pathToMias = "/home/evgen/Development/1_Under_VCS/github/4_NN_ML/data_for_trainings/3-mias";
        Dataset<Row> rowDataset = miasLoader.load(pathToMias);
        rowDataset.show();
    }
}