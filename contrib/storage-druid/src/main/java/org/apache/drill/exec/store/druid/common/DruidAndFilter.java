package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DruidAndFilter extends DruidFilterBase {

    private String type = DruidCompareOp.AND.getCompareOp();
    private List<String> fields = new ArrayList<String>();

    public DruidAndFilter(@JsonProperty("fields") List<String> fields) {
        this.fields = fields;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public void addField(String field) {
        this.fields.add(field);
    }

    @Override
    public String toJson() {
        String json = "{ \"fields\": [" +
                StringUtils.join(this.fields, ",") +
                "], \"type\":" + "\"" + this.type + "\"" + "}";
        return json;
    }
}
