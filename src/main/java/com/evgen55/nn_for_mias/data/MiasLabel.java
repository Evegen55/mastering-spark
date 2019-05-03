package com.evgen55.nn_for_mias.data;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class MiasLabel implements Serializable {
    public int backgroundTissueClass;
//    public int classOfAbnormalityClass;
//    public int severityOfAbnormalityClass;
//    public int[] coordinatesOfCentreAbnormality;
//    public int radiusAbnormality;
}
