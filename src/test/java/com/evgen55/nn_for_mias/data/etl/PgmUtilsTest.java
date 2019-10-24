package com.evgen55.nn_for_mias.data.etl;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.evgen55.nn_for_mias.data.etl.PgmUtils.IMAGE_HIGH;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.IMAGE_WIDTH;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.MAX_GREY_VAL;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.calculateHistogram;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.calculateHistogramStreamApi;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.writeImageFromArray;

public class PgmUtilsTest {

    @Test
    public void test_writeImageFromArray() throws IOException {
        double[] imgArr = IntStream.generate(() -> ThreadLocalRandom.current().nextInt(255))
                .limit((long) IMAGE_WIDTH * IMAGE_HIGH)
                .mapToDouble(i -> i)
                .toArray();

        writeImageFromArray(imgArr, IMAGE_WIDTH, IMAGE_HIGH, MAX_GREY_VAL, new File("src/test/resources/white_noise.pgm"));

    }

    @Test
    public void test_calculateHistogram() {
        double[] imgArr = IntStream.generate(() -> ThreadLocalRandom.current().nextInt(255))
                .limit((long) IMAGE_WIDTH * IMAGE_HIGH)
                .mapToDouble(i -> i)
                .toArray();

        int[] hist_1 = calculateHistogram(imgArr);
        int[] hist_2 = calculateHistogramStreamApi(imgArr);

        Assert.assertArrayEquals(hist_1, hist_2);
    }


}