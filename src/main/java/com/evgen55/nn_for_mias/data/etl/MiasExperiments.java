package com.evgen55.nn_for_mias.data.etl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.evgen55.nn_for_mias.data.etl.MiasTransformer.getLastZerosMinLengthForSinglePgmPicture;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.MAX_GREY_VAL;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.nextString;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.readAsTwoDimArrayIntsFromFile;

public class MiasExperiments {


    public void testImageWrite() {
        final String inputImagePath = "c:/Users/lartsev/Downloads/mias-small/mdb043.pgm";
        final String pathToWriteTempPgmPicture = "exported.pgm";
        int widthLimiter = 0;

        //read raw image
        try (final FileInputStream inImage = new FileInputStream(inputImagePath);
             final BufferedInputStream inImageStream = new BufferedInputStream(inImage)) {

            System.out.println("magic is " + nextString(inImageStream));
            final int width = Integer.parseInt(nextString(inImageStream));
            final int height = Integer.parseInt(nextString(inImageStream));
            final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
            System.out.println("read image " + inputImagePath);

            final int[][] readWithoutLimiter = readAsTwoDimArrayIntsFromFile(inImageStream, width, height, 0, MAX_GREY_VAL);
            testImageInners(readWithoutLimiter);

            System.out.println();
            widthLimiter = getLastZerosMinLengthForSinglePgmPicture(width, readWithoutLimiter);
            System.out.println("widthLimiterFromright = " + widthLimiter);


            PgmUtils.writeImageFromArray(readWithoutLimiter, MAX_GREY_VAL, new File(pathToWriteTempPgmPicture));

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();

        //read raw image with widthLimiter
        try (final FileInputStream inImage = new FileInputStream(inputImagePath);
             final BufferedInputStream inImageStream = new BufferedInputStream(inImage)) {

            System.out.println("magic is " + nextString(inImageStream));
            final int width = Integer.parseInt(nextString(inImageStream));
            final int height = Integer.parseInt(nextString(inImageStream));
            final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
            System.out.println("read image " + inputImagePath);
            System.out.println("widthLimiterFromright = " + widthLimiter);

            final int[][] readWithLimiter = readAsTwoDimArrayIntsFromFile(inImageStream, width, height, widthLimiter, MAX_GREY_VAL);
            testImageInners(readWithLimiter);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();

        //read edited image
        try (final FileInputStream inImage = new FileInputStream(pathToWriteTempPgmPicture);
             final BufferedInputStream inImageStream = new BufferedInputStream(inImage)) {

            System.out.println("magic is " + nextString(inImageStream));
            final int width = Integer.parseInt(nextString(inImageStream));
            final int height = Integer.parseInt(nextString(inImageStream));
            final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
            System.out.println("read image " + pathToWriteTempPgmPicture + " with " + width + " x " + height + " with the maximum gray value " + maxGreyValue + ".");

            final int[][] imageAsArray = readAsTwoDimArrayIntsFromFile(inImageStream, width, height, 0, MAX_GREY_VAL); //0 because it was written with limiter already
            testImageInners(imageAsArray);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void testImageInners(final int[][] imageAsArray) {
        Arrays.stream(imageAsArray[0]).forEach(i -> System.out.print(i + ","));
        System.out.println();
        Arrays.stream(imageAsArray[1]).forEach(i -> System.out.print(i + ","));
        System.out.println();
        Arrays.stream(imageAsArray[986]).forEach(i -> System.out.print(i + ","));
        System.out.println();
        Arrays.stream(imageAsArray[987]).forEach(i -> System.out.print(i + ","));
        System.out.println();
        Arrays.stream(imageAsArray[988]).forEach(i -> System.out.print(i + ","));
        System.out.println();
        Arrays.stream(imageAsArray[5]).forEach(i -> System.out.print(i + ","));
    }
}
