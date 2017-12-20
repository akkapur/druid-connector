package org.apache.drill.exec.store.druid.druid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class SelectQuery {

    public static String IntervalDimensionName = "eventInterval";
    private static String ISO8601DateStringFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static String NixStartTime = "1970-01-01T00:00:00.000Z";
    private String queryType = "select";
    private String dataSource;
    private boolean descending = false;
    private ArrayList<String> dimensions = new ArrayList<>();
    private ArrayList<String> metrics = new ArrayList<>();
    private String granularity = "all";
    private List<String> intervals = new ArrayList<>();
    private PagingSpec pagingSpec = new PagingSpec(null);
    private String filter;

    public SelectQuery(String dataSource, List<String> intervals) {
        this.dataSource = dataSource;
        this.intervals = intervals;
    }

    public SelectQuery(String dataSource) {
        this.dataSource = dataSource;

        //Note - Interval is always this by default because there is no way to provide an interval via SQL
        DateTime now = new DateTime();
        DateTime zulu = now.toDateTime( DateTimeZone.UTC );
        String interval = NixStartTime + "/" + zulu;
        this.intervals.add(interval);
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isDescending() {
        return descending;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    public ArrayList<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = (ArrayList<String>) dimensions;
    }

    public ArrayList<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(ArrayList<String> metrics) {
        this.metrics = metrics;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
    }

    public List<String> getIntervals() {
        return intervals;
    }

    public void setIntervals(ArrayList<String> intervals) {
        this.intervals = intervals;
    }

    public PagingSpec getPagingSpec() {
        return pagingSpec;
    }

    public void setPagingSpec(PagingSpec pagingSpec) {
        this.pagingSpec = pagingSpec;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String toJson() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> query = new HashMap<>();
        query.put("queryType", queryType);
        query.put("dataSource", dataSource);
        query.put("descending", descending);
        query.put("dimensions", getDimensionsAsSpec());
        query.put("metrics", metrics);
        query.put("granularity", granularity);
        query.put("pagingSpec", pagingSpec);
        if (StringUtils.isNotBlank(this.filter))
        {
            //We are doing this monkey dance so that we can pull out interval fields
            //instead of parsing the json to find and remove the eventInterval nodes,
            //we just string replace the node with empty string.
            List<JsonNode> intervalNodes = parseFilterForInterval(objectMapper.readTree(this.filter));
            for (JsonNode intervalNode : intervalNodes) {
                String interval = intervalNode.asText();
                String intervalSubString1 = ",{\"eventInterval\":\"" + interval + "\"}";
                String intervalSubString2 = "{\"eventInterval\":\"" + interval + "\"},";
                this.filter = this.filter.replace(intervalSubString1, "");
                this.filter = this.filter.replace(intervalSubString2, "");
            }

            //make sure we still have a filter after string replace.
            if (StringUtils.isNotBlank(this.filter)) {
                JsonNode filterJson = objectMapper.readTree(this.filter);
                if (filterJson != null)
                {
                    query.put("filter", filterJson);
                }
            }
        }

        query.put("intervals", intervals); //should always be set after parsing the filter for interval.
        return objectMapper.writeValueAsString(query);
    }

    private ArrayNode getDimensionsAsSpec() {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode dimensionsAsSpec = objectMapper.createArrayNode();
        for(int i=0;i<this.dimensions.size();i++) {
            if (Objects.equals(dimensions.get(i), "__time")) {
                ObjectNode timeDimSpec = objectMapper.createObjectNode();
                timeDimSpec.put("type", "extraction");
                timeDimSpec.set("extractionFn", getTimeExtractionFunction());
                timeDimSpec.put("dimension", "__time");
                timeDimSpec.put("outputName", "__time");
                dimensionsAsSpec.add(timeDimSpec);
            } else {
                dimensionsAsSpec.add(this.dimensions.get(i));
            }
        }
        return dimensionsAsSpec;
    }

    private ObjectNode getTimeExtractionFunction() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode function = objectMapper.createObjectNode();
        function.put("format", ISO8601DateStringFormat);
        function.put("type", "timeFormat");
        return function;
    }

    private List<JsonNode> parseFilterForInterval(JsonNode filterNode)
    {
        //if the filter is on the special Interval Dimension, then use it for the interval.

        List<JsonNode> intervalNodes = filterNode.findValues(IntervalDimensionName);
        if (!intervalNodes.isEmpty()) {
            JsonNode firstIntervalNode = intervalNodes.get(0);
            if (firstIntervalNode != null) {
                String interval = firstIntervalNode.asText();
                this.intervals.clear();
                this.intervals.add(interval);
            }
        }

        return intervalNodes;
    }
}
