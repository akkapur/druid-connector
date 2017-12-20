package org.apache.drill.exec.store.druid.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class PagingSpec {

    private ObjectMapper mapper = new ObjectMapper();
    private ObjectNode pagingIdentifiers = mapper.createObjectNode();
    private int threshold = 1000;
    private Boolean fromNext = true;

    public PagingSpec(ObjectNode pagingIdentifiers, int threshold) {
        this.pagingIdentifiers = pagingIdentifiers;
        this.threshold = threshold;
    }

    public PagingSpec(ObjectNode pagingIdentifiers) {
        if (pagingIdentifiers != null) {
            this.pagingIdentifiers = pagingIdentifiers;
        }
    }

    public PagingSpec() {
    }

    public ObjectNode getPagingIdentifiers() {
        return pagingIdentifiers;
    }

    public void setPagingIdentifiers(ObjectNode pagingIdentifiers) {
        this.pagingIdentifiers = pagingIdentifiers;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public Boolean getFromNext() {
        return fromNext;
    }

    public void setFromNext(Boolean fromNext) {
        this.fromNext = fromNext;
    }
}
