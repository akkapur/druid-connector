package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

public class DruidNotFilter extends DruidFilterBase {

    private String type = DruidCompareOp.NOT.getCompareOp();
    private String field;

    @JsonCreator
    public DruidNotFilter(@JsonProperty("field") String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String toJson() {
        String json = "{ \"field\": " +
                this.field +
                ", \"type\":" + "\"" + this.type + "\"" + "}";
        return json;
    }
}
