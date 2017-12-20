package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidInFilter extends DruidFilterBase {

    private String type = DruidCompareOp.TYPE_IN.getCompareOp();
    private String dimension;
    private String[] values;

    @JsonCreator
    public DruidInFilter(@JsonProperty("dimension") String dimension,
                            @JsonProperty("values") String[] values) {
        this.dimension = dimension;
        this.values = values;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }
}
