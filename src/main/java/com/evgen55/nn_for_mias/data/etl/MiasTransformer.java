package com.evgen55.nn_for_mias.data.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.evgen55.nn_for_mias.data.etl.PgmUtils.MAX_GREY_VAL;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.PGM_SUFFIX;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.nextString;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.readAsTwoDimArrayIntsFromFile;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.readAsTwoDimArrayIntsMirrorFromFile;

public class MiasTransformer {

    private final SparkSession sparkSession;
    private final Configuration hadoopConfiguration;
    private final FileSystem fileSystem;
    private final String pathToMiasDataSet;

    public MiasTransformer(final SparkSession sparkSession, final String pathToMiasDataSet) throws IOException {
        this.sparkSession = sparkSession;
        hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();
        this.pathToMiasDataSet = pathToMiasDataSet;
        fileSystem = FileSystem.get(hadoopConfiguration);
    }

    /**
     * read image for odd image - define minimum edge from right to cut - read with right limiter for even image -
     * mirror - then define define minimum edge from right to cut - read with right limiter
     * <p>
     * temporary write into dedicated folder
     *
     * @return
     */
    public Path cleanAndTransformDataset(final boolean cutImage) throws IOException {
        final Path hdfsPath = new Path(pathToMiasDataSet); //folder with mias images and descriptions
        final String folderWithETL = pathToMiasDataSet + Path.SEPARATOR + "etl";
        final Path hdfsPathToEtl = new Path(folderWithETL);
        fileSystem.delete(hdfsPathToEtl, true);
        fileSystem.mkdirs(hdfsPathToEtl);

        fileSystem.copyFromLocalFile(new Path(pathToMiasDataSet + org.apache.hadoop.fs.Path.SEPARATOR + "Info.txt"),
                new Path(folderWithETL + org.apache.hadoop.fs.Path.SEPARATOR + "Info.txt"));

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(hdfsPath, true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            final LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            final Path locatedFileStatusPath = locatedFileStatus.getPath();
            final String locatedFileStatusName = locatedFileStatusPath.getName();
            if (locatedFileStatusName.endsWith(PGM_SUFFIX)) {
                final String inputImagePath = locatedFileStatusPath.toUri().getPath();
                try (final FileInputStream inImage = new FileInputStream(inputImagePath);
                     final BufferedInputStream inImageStream = new BufferedInputStream(inImage)) {

                    //skipping necessary bytes
                    System.out.println("magic is " + nextString(inImageStream));
                    final int width = Integer.parseInt(nextString(inImageStream));
                    final int height = Integer.parseInt(nextString(inImageStream));
                    final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
                    System.out.println("read image " + inputImagePath);

                    if (maxGreyValue <= MAX_GREY_VAL) {
                        System.out.println("Reading data represented as 1 byte, see http://netpbm.sourceforge.net/doc/pgm.html");
                        int[][] readWithoutLimiter;
                        int[][] result;
                        final Integer pictureNumber = Integer.valueOf(locatedFileStatusName
                                .replace("mdb", "")
                                .replace(PGM_SUFFIX, ""));
                        if (pictureNumber % 2 == 0) {
                            readWithoutLimiter = readAsTwoDimArrayIntsMirrorFromFile(inImageStream, width, height, MAX_GREY_VAL);
                        } else {
                            readWithoutLimiter = readAsTwoDimArrayIntsFromFile(inImageStream, width, height, 0, MAX_GREY_VAL);
                        }
                        if (cutImage) {
                            final int widthLimiter = getLastZerosMinLengthForSinglePgmPicture(width, readWithoutLimiter);
                            final int[][] readWithLimiter = transformWithDelimiter(readWithoutLimiter, widthLimiter);
                            result = readWithLimiter;
                        } else {
                            result = readWithoutLimiter;
                        }

                        //temporary action ?
                        PgmUtils.writeImageFromArray(result, MAX_GREY_VAL,
                                new File(folderWithETL + org.apache.hadoop.fs.Path.SEPARATOR + locatedFileStatusName));
                    } else {
                        System.out.println("Read data represented as 2 bytes, see http://netpbm.sourceforge.net/doc/pgm.html");
                        throw new UnsupportedOperationException();
                    }
                }
            }
        }
        return hdfsPathToEtl;
    }

    private static int[][] transformWithDelimiter(final int[][] readWithoutLimiter, final int widthLimiterFromRight) {
        final int widthCorrected = readWithoutLimiter[0].length - widthLimiterFromRight;
        final int height = readWithoutLimiter.length;
        System.out.println("read image with width " + widthCorrected + " x " + height + " high");
        final int[][] image = new int[height][widthCorrected];
        for (int i = 0; i < height; ++i) {
            for (int j = 0; j < widthCorrected; ++j) {
                image[i][j] = readWithoutLimiter[i][j];
            }
        }
        return image;
    }

    /**
     * To calculate widthLimiterFromLeft for a single picture to use in next methods
     *
     * @param width
     * @param ints
     * @return
     */
    public static int getLastZerosMinLengthForSinglePgmPicture(final int width, final int[][] ints) {
        int lastZeroCountMinimum = width;
        for (int k = 1; k < ints.length / 3; k++) { //noise reducing
            final int lastZeroCount = getLastZeroCount(ints[k]);
            if ((lastZeroCount < lastZeroCountMinimum) && lastZeroCount > 0) {
                System.out.println(" ================================ right limiter is changed from " + lastZeroCountMinimum + " to " + lastZeroCount + " on line " + k);
                lastZeroCountMinimum = lastZeroCount;
            }
        }
        return lastZeroCountMinimum;
    }

    private static int getLastZeroCount(final int[] ints) {
        int result = 0;
        for (int i = ints.length - 1; i > 0; i--) {
            if (ints[i] > 3) break; //noise reducing
            result += 1;
        }
        return result;
    }

}
