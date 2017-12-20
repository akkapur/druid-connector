package org.apache.drill.exec.store.druid;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidScanSpec {

    private String dataSourceName;
    private String filters;

    @JsonCreator
    public DruidScanSpec(@JsonProperty("dataSourceName") String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public DruidScanSpec(String dataSourceName, String filters) {
        this.dataSourceName = dataSourceName;
        this.filters = filters;
    }

    public String getDataSourceName() {
        return this.dataSourceName;
    }

    public String getFilters() {
        return this.filters;
    }

    @Override
    public String toString() {
        return "DruidScanSpec [dataSourceName=" + dataSourceName + ", filters=" + filters + "]";
    }
}
