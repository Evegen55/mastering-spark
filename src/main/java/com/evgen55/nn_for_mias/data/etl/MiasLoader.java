package com.evgen55.nn_for_mias.data.etl;

import com.databricks.sparkdl.ImageUtils$;
import com.evgen55.nn_for_mias.data.MiasLabel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.evgen55.nn_for_mias.data.LabelsMaker.getLabelsFromDescription;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.IMAGE_HIGH;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.IMAGE_WIDTH;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.MAGIC;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.MAX_GREY_VAL;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.PGM_SUFFIX;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.nextString;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.readAsSingleDimArrayFromFile;
import static com.evgen55.nn_for_mias.data.etl.PgmUtils.readAsSingleDimArrayMirrorFromFile;
import static java.awt.image.BufferedImage.TYPE_BYTE_GRAY;

/**
 * Class is responsible to form all necessary data structures to train NN
 *
 * @author <a href="mailto:i.dolende@gmail.com">Evgenii Lartcev</a>
 */
public class MiasLoader {

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

    /**
     * Method to load from unprepared dataset with direct mirroring
     *
     * @param pathToMiasDataSet
     * @param mirrorDirect
     * @return
     */
    public Dataset<Row> load(final String pathToMiasDataSet, final boolean mirrorDirect) {
        System.out.println("Creating data frame from MIAS images . . .");
        final Path hdfsPath = new Path(pathToMiasDataSet); //folder with mias images and descriptions
        return getRowDatasetFromHdfs(hdfsPath, mirrorDirect);
    }

    /**
     * Method to load from cleaned and transformed dataset, which prepares mirrored pictures at even numbers
     *
     * @param pathToMiasDataSet
     * @return
     */
    public Dataset<Row> load(final Path pathToMiasDataSet) {
        System.out.println("Creating data frame from MIAS images . . .");
        return getRowDatasetFromHdfs(pathToMiasDataSet, false);
    }

    /**
     *
     * @param hdfsPath
     * @param mirrorDirect
     * @return dataset of rows, each of which represents a single-dimension array made of picture, and an appropriate label.
     *
     * Label represents a unique class to be able to classify images in simple NN algorithms
     *
     * see fields com.evgen55.nn_for_mias.data.MiasLabel for possible meaning of label
     *
     * +--------------------+-----+
     * |            features|label|
     * +--------------------+-----+
     * |[0.0,0.0,0.0,0.0,...|  2.0|
     * |[0.0,0.0,0.0,0.0,...|  1.0|
     * |[0.0,0.0,0.0,0.0,...|  1.0|
     * +--------------------+-----+
     */
    private Dataset<Row> getRowDatasetFromHdfs(final Path hdfsPath, final boolean mirrorDirect) {
        final Map<String, MiasLabel> labelsFromDescription = getLabelsFromDescription(javaSparkContext, hdfsPath);
        JavaRDD<LabeledPoint> labeledPointsJavaRDD = getLabeledPointJavaRDD(hdfsPath, labelsFromDescription, mirrorDirect);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(labeledPointsJavaRDD, LabeledPoint.class);
        System.out.println("Creating data frame from MIAS images is done");
        return dataFrame;
    }

    private JavaRDD<LabeledPoint> getLabeledPointJavaRDD(final Path pathToMiasDataSet,
                                                         final Map<String, MiasLabel> labelsFromDescription,
                                                         final boolean mirrorDirect) {
        final JavaPairRDD<String, PortableDataStream> binaryFiles = javaSparkContext.binaryFiles(pathToMiasDataSet.toUri().getPath());
        return binaryFiles
                .map(fileNameAndDataTuple -> {
                    LabeledPoint labeledPointMiasImage = null;
                    String pathToFile = fileNameAndDataTuple._1();
                    final Path hdfsPathToFile = new Path(pathToFile);
                    final String fileName = hdfsPathToFile.getName();
                    if (fileName.endsWith(PGM_SUFFIX)) {
                        try (final BufferedInputStream inImageStream = new BufferedInputStream(fileNameAndDataTuple._2().open())) {
                            System.out.println("Available " + inImageStream.available() + " bytes");
                            if (MAGIC.equals(nextString(inImageStream))) {
                                final int width = Integer.parseInt(nextString(inImageStream));
                                final int height = Integer.parseInt(nextString(inImageStream));
                                final int maxGreyValue = Integer.parseInt(nextString(inImageStream));
                                if (mirrorDirect) { //do not allow load images with diferrent size - because mias transformer will cut images
                                    if (width != IMAGE_WIDTH || height != IMAGE_HIGH) {
                                        throw new RuntimeException("=========IMAGE SIZE HAS BEEN CHANGED to " + width + "\t" + height);
                                    }
                                }
                                System.out.println("read image " + fileName + " " + width + " x " + height +
                                        " with the maximum gray value " + maxGreyValue + ".");

                                final int recurrentImageBufferSize = width * height;
                                if (maxGreyValue <= MAX_GREY_VAL) {
                                    System.out.println("Reading data represented as 1 byte, see http://netpbm.sourceforge.net/doc/pgm.html");
                                    final Integer pictureNumber = Integer.valueOf(fileName
                                            .replace("mdb", "")
                                            .replace(PGM_SUFFIX, ""));
                                    double[] singleDimArray;
                                    if (mirrorDirect && pictureNumber % 2 == 0) {
                                        singleDimArray = readAsSingleDimArrayMirrorFromFile(inImageStream, width, height, maxGreyValue, recurrentImageBufferSize);
                                        //for test only
//                                        PgmUtils.writeImageFromArray(singleDimArray, width, height, MAX_GREY_VAL, new File("from_single_dim_mirrored_" + pictureNumber + ".pgm"));
                                    } else {
                                        singleDimArray = readAsSingleDimArrayFromFile(inImageStream, maxGreyValue, recurrentImageBufferSize);
                                    }
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

    public Dataset<Row> getRowDatasetWithDatabrickAPI() {
        final Row spImageFromBufferedImage = ImageUtils$.MODULE$.spImageFromBufferedImage(
                new BufferedImage(IMAGE_WIDTH, IMAGE_HIGH, TYPE_BYTE_GRAY),
                "/home/evgen/Development/1_Under_VCS/github/4_NN_ML/data_for_trainings/3-mias/mdb001.pgm");

        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        final JavaRDD<Row> parallelize = javaSparkContext.parallelize(List.of(spImageFromBufferedImage));

        return sparkSession.createDataFrame(parallelize, Row.class);
    }


}
