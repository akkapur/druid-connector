package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.exec.store.druid.druid.DruidSelectResponse;
import org.apache.drill.exec.store.druid.druid.PagingIdentifier;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import static org.apache.http.protocol.HTTP.CONTENT_TYPE;

public class DruidQueryClient {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidQueryClient.class);

    private static final String QUERY_BASE_URI = "/druid/v2";
    private static final String DEFAULT_ENCODING = "UTF-8";

    private String brokerURI;
    private String druidURL;

    public DruidQueryClient(String brokerURI) {
        this.brokerURI = brokerURI;
        druidURL = this.brokerURI + QUERY_BASE_URI;
        logger.debug("Initialized DruidQueryClient with druidURL - " + this.druidURL);
    }

    public DruidSelectResponse ExecuteQuery(String query) throws IOException {

        logger.debug("Executing Query - " + query);

        ArrayList<ObjectNode> events = new ArrayList<>();
        ObjectNode pagingIdentifiersNode = null;

        HttpClient client = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(druidURL);
        httppost.addHeader(CONTENT_TYPE, "application/json");
        HttpEntity entity = new ByteArrayEntity(query.getBytes(DEFAULT_ENCODING));
        httppost.setEntity(entity);

        HttpResponse response = client.execute(httppost);
        String data = EntityUtils.toString(response.getEntity());
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode responses = mapper.readValue(data, ArrayNode.class);

        if (responses.size() > 0) {
            ObjectNode firstNode = (ObjectNode) responses.get(0);
            ObjectNode resultNode = (ObjectNode) firstNode.get("result");
            pagingIdentifiersNode = (ObjectNode) resultNode.get("pagingIdentifiers");
            ArrayNode eventsNode = (ArrayNode) resultNode.get("events");
            for(int i =0;i < eventsNode.size(); i++) {
                ObjectNode eventNode = (ObjectNode) eventsNode.get(i).get("event");
                events.add(eventNode);
            }
        }

        DruidSelectResponse druidSelectResponse = new DruidSelectResponse();

        ArrayList<PagingIdentifier> pagingIdentifierList = new ArrayList<>();
        if (pagingIdentifiersNode != null) {
            for (Iterator<Map.Entry<String, JsonNode>> iterator = pagingIdentifiersNode.fields(); iterator.hasNext();) {
                Map.Entry<String, JsonNode> currentNode = iterator.next();
                if (currentNode != null) {
                    String segmentName = currentNode.getKey();
                    int segmentOffset = currentNode.getValue().asInt();
                    PagingIdentifier pagingIdentifier = new PagingIdentifier(segmentName, segmentOffset);
                    pagingIdentifierList.add(pagingIdentifier);
                }
            }
        }

        if (!pagingIdentifierList.isEmpty())
            druidSelectResponse.setPagingIdentifiers(pagingIdentifierList);

        druidSelectResponse.setEvents(events);

        return druidSelectResponse;
    }
}
