package com.evgen55.simple_nn.entities;

import java.util.HashMap;
import java.util.Map;

public class Utils {

    private Utils() {
    }

    private static final Map<String, Integer> labelsMapping = new HashMap<>(4);

    static {
        labelsMapping.put("attack", 0);
        labelsMapping.put("run", 1);
        labelsMapping.put("dodge", 2); //уворачиваться
        labelsMapping.put("hide", 3);
    }

    public static int getMappingFrom(final String labelName) {
        return labelsMapping.get(labelName);
    }

    public static String getMappingFrom(final int labelNumber) {
        return labelsMapping.entrySet().stream()
                .filter(stringIntegerEntry -> stringIntegerEntry.getValue() == labelNumber)
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getKey();
    }
}
