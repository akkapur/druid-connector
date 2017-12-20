package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidSelectorFilter extends DruidFilterBase {

    private String type = DruidCompareOp.TYPE_SELECTOR.getCompareOp();
    private String dimension;
    private String value;

    @JsonCreator
    public DruidSelectorFilter(@JsonProperty("dimension") String dimension,
                               @JsonProperty("value") String value) {
        this.dimension = dimension;
        this.value = value;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
