package com.evgen55.nn_for_mias.data;

import com.evgen55.nn_for_mias.data.etl.MiasLoader;
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
    public void testLoad() throws IOException {
        MiasLoader miasLoader = new MiasLoader(sparkSession);
        String pathToMias = "";
        Dataset<Row> rowDataset = miasLoader.load(pathToMias, false);
        rowDataset.show();
    }
}