package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class DruidAdminClient {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidAdminClient.class);

    private static final String DATASOURCES_BASE_URI = "/druid/coordinator/v1/datasources";
    private static final String DEFAULT_ENCODING = "UTF-8";

    private String coordinatorURI;

    public DruidAdminClient(String coordinatorURI) {
        this.coordinatorURI = coordinatorURI;
    }

    public Set<String> GetDataSources() throws IOException {

        logger.debug("GetDataSources Called.");

        String dataSourcesURI = this.coordinatorURI + DATASOURCES_BASE_URI;

        HttpClient client = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(dataSourcesURI);
        httpget.addHeader(CONTENT_TYPE, APPLICATION_JSON);

        HttpResponse response = client.execute(httpget);
        String responseJson = EntityUtils.toString(response.getEntity(), DEFAULT_ENCODING);
        return new ObjectMapper().readValue(responseJson, new TypeReference<HashSet<String>>(){});
    }
}
