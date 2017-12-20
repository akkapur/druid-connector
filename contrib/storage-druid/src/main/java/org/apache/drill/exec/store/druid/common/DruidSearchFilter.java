package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.xpath.operations.Bool;

import java.util.HashMap;

public class DruidSearchFilter {

    private String type = DruidCompareOp.TYPE_SEARCH.getCompareOp();
    private String dimension;
    private Boolean caseSensitive;
    private String value;

    public DruidSearchFilter(String dimension, Boolean caseSensitive, String value) {
        this.dimension = dimension;
        this.caseSensitive = caseSensitive;
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

    public Boolean getCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> filter = new HashMap<>();
        filter.put("type", this.type);
        filter.put("dimension", this.dimension);

        HashMap<String, Object> query = new HashMap<>();
        query.put("type", DruidCompareOp.TYPE_SEARCH_CONTAINS.getCompareOp());
        query.put("value", this.value);
        query.put("caseSensitive", this.caseSensitive);
        filter.put("query", query);

        return objectMapper.writeValueAsString(filter);
    }
}
