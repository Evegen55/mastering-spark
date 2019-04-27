package com.evgen55.nn_for_mias.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class MiasLoader {

    private static String PGM_SUFFIX = ".pgm";

    /**
     * Magic number representing the binary PGM file type.
     */
    private static final String MAGIC = "P5";
    /**
     * Character indicating a comment.
     */
    private static final char COMMENT = '#';

    private final SparkSession sparkSession;
    private final Configuration hadoopConfiguration;
    private final FileSystem fileSystem;

    public MiasLoader(final SparkSession sparkSession) throws IOException {
        hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();
        this.sparkSession = sparkSession;
        fileSystem = FileSystem.get(hadoopConfiguration);
    }

    public Dataset<Row> readFromData(final String pathToMiasDataSet) throws IOException {

        final List<LabeledMiasImage> labeledMiasImages = new ArrayList<>();
        final Path hdfsPath = new Path(pathToMiasDataSet); //folder with mias images and descriptions
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(hdfsPath, true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            final LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            final Path locatedFileStatusPath = locatedFileStatus.getPath();
            final String locatedFileStatusName = locatedFileStatusPath.getName();
            if (locatedFileStatusName.endsWith(PGM_SUFFIX)) {
                labeledMiasImages.add(getLabeledMiasImage(locatedFileStatusPath.toUri().getPath()));
            }
        }
        return sparkSession.createDataFrame(labeledMiasImages, LabeledMiasImage.class);

    }

    protected LabeledMiasImage getLabeledMiasImage(final String inputImagePath) {
        LabeledMiasImage labeledMiasImage = null;
        try (final FileInputStream inImage = new FileInputStream(inputImagePath);
             final BufferedInputStream inImageStream = new BufferedInputStream(inImage)) {
            try {
                System.out.println("Available " + inImageStream.available() + " bytes");
                if (MAGIC.equals(nextString(inImageStream))) {
                    final int width = Integer.parseInt(nextString(inImageStream));
                    final int height = Integer.parseInt(nextString(inImageStream));
                    final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
                    System.out.println("read image " + width + " x " + height + " with the maximum gray value " + maxGreyValue + ".");

                    final int recurrentImageBufferSize = width * height;
                    if (maxGreyValue <= 255) {
                        System.out.println("Reading data represented as 1 byte, see http://netpbm.sourceforge.net/doc/pgm.html");
//                        readAsTwoDimArray(inImageStream, width, height, maxGreyValue);
                        double[] singleDimArray = readAsSingleDimArray(inImageStream, maxGreyValue, recurrentImageBufferSize);
                        return new LabeledMiasImage(singleDimArray, 0); // TODO: 27.04.19 Label from dataset
                    } else {
                        System.out.println("Read data represented as 2 bytes, see http://netpbm.sourceforge.net/doc/pgm.html");
                        // TODO: 27.04.19
                    }
                    System.out.println("Available " + inImageStream.available() + " bytes");
                } else {
                    throw new IOException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return labeledMiasImage;
    }

    private static double[] readAsSingleDimArray(final BufferedInputStream inImageStream,
                                                 final int maxGreyValue, final int recurrentImageBufferSize) throws IOException {
        final double[] image = new double[recurrentImageBufferSize];
        for (int i = 0; i < recurrentImageBufferSize; ++i) {
            final int p = inImageStream.read();
            if (p == -1)
                throw new IOException("Reached end-of-file prematurely.");
            else if (p > maxGreyValue)
                throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
            image[i] = p;
        }
        return image;
    }

    private static double[][] readAsTwoDimArray(final BufferedInputStream inImageStream,
                                                final int width, final int height, final int maxGreyValue) throws IOException {
        final double[][] image = new double[width][height];
        for (int i = 0; i < width; ++i) {
            for (int j = 0; j < height; ++j) {
                final int p = inImageStream.read();
                if (p == -1)
                    throw new IOException("Reached end-of-file prematurely.");
                else if (p > maxGreyValue)
                    throw new IOException("Pixel value " + p + " outside of range [0, " + maxGreyValue + "].");
                image[i][j] = p;
            }
        }
        return image;
    }

    private static String nextString(final InputStream stream) throws IOException {
        final List<Byte> bytes = new ArrayList<>();
        while (true) {
            final int b = stream.read();
            if (b != -1) {
                final char c = (char) b;
                if (c == COMMENT) {
                    skipComment(stream);
                } else if (!Character.isWhitespace(c)) {
                    bytes.add((byte) b);
                } else if (bytes.size() > 0) {
                    break;
                }
            } else {
                break;
            }

        }
        final byte[] bytesArray = new byte[bytes.size()];
        for (int i = 0; i < bytesArray.length; ++i) {
            bytesArray[i] = bytes.get(i);
        }

        return new String(bytesArray);
    }

    private static int skipComment(final InputStream stream) throws IOException {
        int skipPos = 0;
        int d;
        do {
            d = stream.read();
            skipPos += d;
        } while (d != -1 && d != '\n' && d != '\r');
        return skipPos;
    }
}
