package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidIntervalFilter extends DruidFilterBase {

    private String eventInterval;

    @JsonCreator
    public DruidIntervalFilter(@JsonProperty("eventInterval") String eventInterval) {
        this.eventInterval = eventInterval;
    }

    public String getEventInterval() {
        return eventInterval;
    }

    public void setEventInterval(String eventInterval) {
        this.eventInterval = eventInterval;
    }
}
