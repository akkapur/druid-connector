package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.bson.Document;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A Class containing information to read a single druid data source.
 */
@JsonTypeName("druid-datasource-scan")
public class DruidSubScan extends AbstractBase implements SubScan {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidSubScan.class);

    @JsonProperty
    public final DruidStoragePluginConfig druidStoragePluginConfig;

    @JsonIgnore
    private final DruidStoragePlugin druidStoragePlugin;

    private final List<DruidSubScanSpec> dataSourceScanSpecList;
    private final List<SchemaPath> columns;

    @JsonCreator
    public DruidSubScan(@JacksonInject StoragePluginRegistry registry,
                       @JsonProperty("druidStoragePluginConfig") StoragePluginConfig druidStoragePluginConfig,
                       @JsonProperty("datasourceScanSpecList") LinkedList<DruidSubScanSpec> datasourceScanSpecList,
                       @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
        super("someuser");
        druidStoragePlugin = (DruidStoragePlugin) registry.getPlugin(druidStoragePluginConfig);
        this.dataSourceScanSpecList = datasourceScanSpecList;
        this.druidStoragePluginConfig = (DruidStoragePluginConfig) druidStoragePluginConfig;
        this.columns = columns;
    }

    public DruidSubScan(DruidStoragePlugin plugin, DruidStoragePluginConfig config,
                       List<DruidSubScanSpec> dataSourceInfoList, List<SchemaPath> columns) {
        super("someuser");
        druidStoragePlugin = plugin;
        druidStoragePluginConfig = config;
        this.dataSourceScanSpecList = dataSourceInfoList;
        this.columns = columns;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    public List<DruidSubScanSpec> getDataSourceScanSpecList() {
        return dataSourceScanSpecList;
    }

    @JsonIgnore
    public DruidStoragePluginConfig getStorageConfig() {
        return druidStoragePluginConfig;
    }

    public List<SchemaPath> getColumns() {
        return columns;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }

    @JsonIgnore
    public DruidStoragePlugin getStorageEngine(){
        return druidStoragePlugin;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new DruidSubScan(druidStoragePlugin, druidStoragePluginConfig, dataSourceScanSpecList, columns);
    }

    @Override
    public int getOperatorType() {
        return 1009;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Iterators.emptyIterator();
    }

    public static class DruidSubScanSpec {

        protected String dataSourceName;
        protected String filter;

        @JsonCreator
        public DruidSubScanSpec(@JsonProperty("dataSourceName") String dataSourceName,
                                @JsonProperty("filters") String filters) {
            this.dataSourceName = dataSourceName;
            this.filter = filters;
        }

        DruidSubScanSpec() {

        }

        public String getDataSourceName() {
            return dataSourceName;
        }

        public String getFilter() {return this.filter;}
    }
}
