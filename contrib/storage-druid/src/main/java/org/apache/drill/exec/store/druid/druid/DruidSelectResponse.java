package org.apache.drill.exec.store.druid.druid;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DruidSelectResponse {

    private ArrayList<ObjectNode> events = new ArrayList<>();
    private ArrayList<PagingIdentifier> pagingIdentifiers = new ArrayList<>();


    public ArrayList<ObjectNode> getEvents() {
        return events;
    }

    public void setEvents(ArrayList<ObjectNode> events) {
        this.events = events;
    }

    public ArrayList<PagingIdentifier> getPagingIdentifiers() {
        return pagingIdentifiers;
    }

    public void setPagingIdentifiers(ArrayList<PagingIdentifier> pagingIdentifiers) {
        this.pagingIdentifiers = pagingIdentifiers;
    }
}
