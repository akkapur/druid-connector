package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.bson.BsonRecordReader;
import org.apache.drill.exec.store.druid.common.DruidAndFilter;
import org.apache.drill.exec.store.druid.common.DruidUtils;
import org.apache.drill.exec.store.druid.druid.DruidSelectResponse;
import org.apache.drill.exec.store.druid.druid.PagingIdentifier;
import org.apache.drill.exec.store.druid.druid.PagingSpec;
import org.apache.drill.exec.store.druid.druid.SelectQuery;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.bson.Document;

import java.io.IOException;
import java.util.*;

public class DruidRecordReader extends AbstractRecordReader {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidRecordReader.class);
    private DruidStoragePlugin plugin;
    private final DruidSubScan.DruidSubScanSpec scanSpec;
    private List<String> dimensions;
    private String filters;
    private ArrayList<PagingIdentifier> pagingIdentifiers = new ArrayList<>();

    private JsonReader jsonReader;
    private VectorContainerWriter writer;

    private OutputMutator output;
    private OperatorContext context;
    private final FragmentContext fragmentContext;

    private ObjectMapper objectMapper = new ObjectMapper();

    public DruidRecordReader(DruidSubScan.DruidSubScanSpec subScanSpec, List<SchemaPath> projectedColumns,
                             FragmentContext context, DruidStoragePlugin plugin) {
        dimensions = new ArrayList<String>();
        setColumns(projectedColumns);
        this.plugin = plugin;
        scanSpec = subScanSpec;
        fragmentContext = context;
        this.filters = subScanSpec.getFilter();
    }

    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
        Set<SchemaPath> transformed = Sets.newLinkedHashSet();
        if (!isStarQuery()) {
            for (SchemaPath column : projectedColumns) {
                String fieldName = column.getRootSegment().getPath();
                transformed.add(column);
                this.dimensions.add(fieldName);
            }
        } else {
            transformed.add(AbstractRecordReader.STAR_COLUMN);
        }
        return transformed;
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        this.context = context;
        this.output = output;
        this.writer = new VectorContainerWriter(output);

        this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()),
                true, false, false);
        logger.debug(" Intialized JsonRecordReader. ");
    }

    @Override
    public int next() {

        writer.allocate();
        writer.reset();
        SelectQuery selectQuery = new SelectQuery(scanSpec.dataSourceName);
        selectQuery.setDimensions(this.dimensions);
        selectQuery.setFilter(this.filters);

        ObjectNode paging = objectMapper.createObjectNode();
        if (this.pagingIdentifiers != null && !this.pagingIdentifiers.isEmpty()) {

            for (PagingIdentifier pagingIdentifier : this.pagingIdentifiers)
            {
                paging.put(pagingIdentifier.getSegmentName(), pagingIdentifier.getSegmentOffset());
            }
        }

        PagingSpec pagingSpec = new PagingSpec(paging);
        selectQuery.setPagingSpec(pagingSpec);

        DruidQueryClient druidQueryClient = plugin.getDruidQueryClient();

        try {
            String query = selectQuery.toJson();
            DruidSelectResponse druidSelectResponse = druidQueryClient.ExecuteQuery(query);
            ArrayList<PagingIdentifier> newPagingIdentifiers = druidSelectResponse.getPagingIdentifiers();

            ArrayList<String> newPagingIdentifierNames = new ArrayList<>();
            for (PagingIdentifier pagingIdentifier : newPagingIdentifiers)
            {
                newPagingIdentifierNames.add(pagingIdentifier.getSegmentName());
            }

            for (PagingIdentifier pagingIdentifier : this.pagingIdentifiers)
            {
                if (!newPagingIdentifierNames.contains(pagingIdentifier.getSegmentName()))
                    newPagingIdentifiers.add(new PagingIdentifier(pagingIdentifier.getSegmentName(), pagingIdentifier.getSegmentOffset() + 1));
            }

            //update the paging identifiers
            this.pagingIdentifiers = newPagingIdentifiers;

            int docCount = 0;
            for (ObjectNode eventNode : druidSelectResponse.getEvents()) {
                writer.setPosition(docCount);
                jsonReader.setSource(eventNode);
                try {
                    jsonReader.write(writer);
                } catch (IOException e) {
                    String msg = "Failure while reading document. - Parser was at record: " + eventNode.toString();
                    logger.error(msg, e);
                    throw new DrillRuntimeException(msg, e);
                }
                docCount++;
            }

            writer.setValueCount(docCount);
            return docCount;
        } catch (IOException e) {
            String msg = "Failure while reading documents";
            logger.error(msg, e);
            throw new DrillRuntimeException(msg, e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
