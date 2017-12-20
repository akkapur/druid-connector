package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidRegexFilter extends DruidFilterBase {
    private String type = DruidCompareOp.TYPE_REGEX.getCompareOp();
    private String dimension;
    private String pattern;

    @JsonCreator
    public DruidRegexFilter(@JsonProperty("dimension") String dimension,
                               @JsonProperty("pattern") String pattern) {
        this.dimension = dimension;
        this.pattern = pattern;
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

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}
