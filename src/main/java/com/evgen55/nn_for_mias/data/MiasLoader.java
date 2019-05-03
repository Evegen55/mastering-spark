package com.evgen55.nn_for_mias.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.evgen55.nn_for_mias.data.LabelsMaker.getLabelsFromDescription;

/**
 * Class is responsible to form all necessary data structures to train NN
 *
 * @author <a href="mailto:i.dolende@gmail.com">Evgenii Lartcev</a>
 */
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
    private final JavaSparkContext javaSparkContext;
    private final Configuration hadoopConfiguration;
    private final FileSystem fileSystem;

    public MiasLoader(final SparkSession sparkSession) throws IOException {
        this.sparkSession = sparkSession;
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();
        fileSystem = FileSystem.get(hadoopConfiguration);
    }

    public Dataset<Row> load(final String pathToMiasDataSet) {
        System.out.println("Creating data frame from MIAS images . . .");
        final Path hdfsPath = new Path(pathToMiasDataSet); //folder with mias images and descriptions
        final Map<String, MiasLabel> labelsFromDescription = getLabelsFromDescription(javaSparkContext, hdfsPath);
        JavaRDD<LabeledPoint> labeledPointJavaRDD = getLabeledPointJavaRDD(hdfsPath, labelsFromDescription);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(labeledPointJavaRDD, LabeledPoint.class);
        System.out.println("Creating data frame from MIAS images is done");
        return dataFrame;
    }

    private JavaRDD<LabeledPoint> getLabeledPointJavaRDD(final Path pathToMiasDataSet, final Map<String, MiasLabel> labelsFromDescription) {
        final JavaPairRDD<String, PortableDataStream> binaryFiles = javaSparkContext.binaryFiles(pathToMiasDataSet.toUri().getPath());
        return binaryFiles
                .map(fileNameAndDataTuple -> {
                    LabeledPoint labeledPointMiasImage = null;
                    String pathToFile = fileNameAndDataTuple._1();
                    final Path hdfsPathToFile = new Path(pathToFile);
                    final String fileName = hdfsPathToFile.getName();
                    if (fileName.endsWith(PGM_SUFFIX)) { // TODO: 03.05.19 there are two projections for each case, it needs to be reverted to train!
                        try (final BufferedInputStream inImageStream = new BufferedInputStream(fileNameAndDataTuple._2().open())) {
                            System.out.println("Available " + inImageStream.available() + " bytes");
                            if (MAGIC.equals(nextString(inImageStream))) {
                                final int width = Integer.parseInt(nextString(inImageStream));
                                final int height = Integer.parseInt(nextString(inImageStream));
                                final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
                                System.out.println("read image " + fileName + " " + width + " x " + height + " with the maximum gray value " + maxGreyValue + ".");

                                final int recurrentImageBufferSize = width * height;
                                if (maxGreyValue <= 255) {
                                    System.out.println("Reading data represented as 1 byte, see http://netpbm.sourceforge.net/doc/pgm.html");
//                                    readAsTwoDimArray(inImageStream, width, height, maxGreyValue);
                                    double[] singleDimArray = readAsSingleDimArray(inImageStream, maxGreyValue, recurrentImageBufferSize);
                                    final MiasLabel miasLabel = labelsFromDescription.get(fileName.replace(PGM_SUFFIX, ""));
                                    labeledPointMiasImage = new LabeledPoint(miasLabel.backgroundTissueClass, Vectors.dense(singleDimArray));
                                } else {
                                    System.out.println("Read data represented as 2 bytes, see http://netpbm.sourceforge.net/doc/pgm.html");
                                    // TODO: 27.04.19
                                }
                                System.out.println("Available " + inImageStream.available() + " bytes");
                            }
                        }
                    }
                    return labeledPointMiasImage;
                })
                .filter(Objects::nonNull);
    }

    @Deprecated
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

    @Deprecated
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
            image[i] = p; //upscale from 1 byte to 8 - CRAZY JAVA
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
