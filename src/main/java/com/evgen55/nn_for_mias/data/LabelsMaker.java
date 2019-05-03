package com.evgen55.nn_for_mias.data;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class which holds labeling info from description to dataset
 *
 * @author <a href="mailto:i.dolende@gmail.com">Evgenii Lartcev</a>
 */
public class LabelsMaker {

    public static final int BACKGROUND_TISSUE_MAPPING_SIZE = 3;
    private static final Map<String, Integer> backgroundTissueMapping = new HashMap<>(BACKGROUND_TISSUE_MAPPING_SIZE);

    public static final int CLASS_OF_ABNORMALITY_MAPPING_SIZE = 7;
    private static final Map<String, Integer> classOfAbnormalityMapping = new HashMap<>(CLASS_OF_ABNORMALITY_MAPPING_SIZE);

    public static final int SEVERITY_OF_ABNORMALITY_MAPPING_SIZE = 2;
    private static final Map<String, Integer> severityOfAbnormalityMapping = new HashMap<>(SEVERITY_OF_ABNORMALITY_MAPPING_SIZE);

    static {
        backgroundTissueMapping.put("F", 0);
        backgroundTissueMapping.put("G", 1);
        backgroundTissueMapping.put("D", 2);

        classOfAbnormalityMapping.put("CALC", 0);
        classOfAbnormalityMapping.put("CIRC", 1);
        classOfAbnormalityMapping.put("SPIC", 2);
        classOfAbnormalityMapping.put("MISC", 3);
        classOfAbnormalityMapping.put("ARCH", 4);
        classOfAbnormalityMapping.put("ASYM", 5);
        classOfAbnormalityMapping.put("NORM", 6);

        severityOfAbnormalityMapping.put("B", 0);
        severityOfAbnormalityMapping.put("M", 1);
    }

    public static int getMappingForBackgroundTissue(final String labelName) {
        return backgroundTissueMapping.get(labelName);
    }

    public static int getMappingForclassOfAbnormality(final String labelName) {
        return classOfAbnormalityMapping.get(labelName);
    }

    public static int getMappingForSeverityOfAbnormality(final String labelName) {
        return severityOfAbnormalityMapping.get(labelName);
    }

    /**
     * mdb001 G CIRC B 535 425 197
     * <p>
     * 0 if not presented or in NOTE 3 case (cluster of calcifications)
     *
     * @param rawDescription
     * @return
     */
    public static MiasLabel createMiasLabel(final String[] rawDescription) {
        int backgroundTissueClass = getMappingForBackgroundTissue(rawDescription[1]);
//        int classOfAbnormalityClass = getMappingForclassOfAbnormality(rawDescription[2]);
//        int severityOfAbnormalityClass = rawDescription.length > 3 ? getMappingForSeverityOfAbnormality(rawDescription[3]) : 0;
//
//        int[] coordinatesOfCentreAbnormality = new int[]{};
//        if (rawDescription.length > 5) {
//            if (!rawDescription[4].contains("NOTE")) { // NOTE 3
//                coordinatesOfCentreAbnormality = new int[]{Integer.parseInt(rawDescription[4]), Integer.parseInt(rawDescription[5])};
//            }
//        }
//
//        int radiusAbnormality = rawDescription.length > 6 ? Integer.parseInt(rawDescription[6]) : 0;
        return MiasLabel.builder()
                .backgroundTissueClass(backgroundTissueClass)
                .build();
    }

    public static Map<String, MiasLabel> getLabelsFromDescription(final JavaSparkContext javaSparkContext, final Path hdfsPathToRoot) {
        final JavaRDD<String> logFileWithCoordinates = javaSparkContext
                .textFile(hdfsPathToRoot.toUri().getPath() + org.apache.hadoop.fs.Path.SEPARATOR + "Info.txt");
        return logFileWithCoordinates
                .filter(line -> line.startsWith("mdb"))
                .map(line -> line.split(" "))
                .collect().stream()
                .collect(Collectors.toMap(desc -> desc[0], LabelsMaker::createMiasLabel, (oldValue, newValue) -> oldValue));
    }

}

