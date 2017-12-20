package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

public class DruidBoundFilter {

    private String type = DruidCompareOp.TYPE_BOUND.getCompareOp();
    private String dimension;
    private String lower;
    private String upper;
    private Boolean alphaNumeric = false;
    private Boolean lowerStrict =  false;
    private Boolean upperStrict = false;

    @JsonCreator
    public DruidBoundFilter(@JsonProperty("dimension") String dimension,
                               @JsonProperty("lower") String lower,
                            @JsonProperty("upper") String upper) {
        this.dimension = dimension;
        this.lower = lower;
        this.upper= upper;
    }

    @JsonCreator
    public DruidBoundFilter(@JsonProperty("dimension") String dimension,
                            @JsonProperty("lower") String lower,
                            @JsonProperty("upper") String upper,
                            @JsonProperty("alphaNumeric") Boolean alphaNumeric,
                            @JsonProperty("lowerStrict") Boolean lowerStrict,
                            @JsonProperty("upperStrict") Boolean upperStrict) {
        this.dimension = dimension;
        this.lower = lower;
        this.upper= upper;
        this.alphaNumeric = alphaNumeric;
        this.lowerStrict = lowerStrict;
        this.upperStrict = upperStrict;
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

    public String getLower() {
        return lower;
    }

    public void setLower(String lower) {
        this.lower = lower;
    }

    public String getUpper() {
        return upper;
    }

    public void setUpper(String upper) {
        this.upper = upper;
    }

    public Boolean getAlphaNumeric() {
        return alphaNumeric;
    }

    public void setAlphaNumeric(Boolean alphaNumeric) {
        this.alphaNumeric = alphaNumeric;
    }

    public Boolean getLowerStrict() {
        return lowerStrict;
    }

    public void setLowerStrict(Boolean lowerStrict) {
        this.lowerStrict = lowerStrict;
    }

    public Boolean getUpperStrict() {
        return upperStrict;
    }

    public void setUpperStrict(Boolean upperStrict) {
        this.upperStrict = upperStrict;
    }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> filter = new HashMap<>();
        filter.put("type", this.type);
        filter.put("dimension", this.dimension);
        if (StringUtils.isNotBlank(this.lower))
            filter.put("lower", this.lower);
        if (StringUtils.isNotBlank(this.upper))
            filter.put("upper", this.upper);

        filter.put("lowerStrict", this.lowerStrict);
        filter.put("upperStrict", this.upperStrict);
        filter.put("alphaNumeric", this.alphaNumeric);

        return objectMapper.writeValueAsString(filter);
    }
}
