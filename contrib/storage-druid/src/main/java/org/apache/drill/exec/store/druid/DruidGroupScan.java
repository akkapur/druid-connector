package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.ServerAddress;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.*;

import java.io.IOException;
import java.util.*;

@JsonTypeName("druid-scan")
public class DruidGroupScan extends AbstractGroupScan {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidGroupScan.class);
    private static final long DEFAULT_TABLET_SIZE = 1000;

    private DruidStoragePluginConfig storagePluginConfig;
    private List<SchemaPath> columns;
    private DruidScanSpec scanSpec;
    private DruidStoragePlugin storagePlugin;
    private boolean filterPushedDown = false;
    private List<DruidWork> druidWorkList = Lists.newArrayList();
    private ListMultimap<Integer,DruidWork> assignments;
    private List<EndpointAffinity> affinities;
    private String objectName;

    @JsonCreator
    public DruidGroupScan(@JsonProperty("scanSpec") DruidScanSpec scanSpec,
                         @JsonProperty("storagePluginConfig") DruidStoragePluginConfig storagePluginConfig,
                         @JsonProperty("columns") List<SchemaPath> columns,
                         @JacksonInject StoragePluginRegistry pluginRegistry)
            throws IOException, ExecutionSetupException {
        this((DruidStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
        int columnSize = (columns == null) ? 0 : columns.size();
    }

    public DruidGroupScan(DruidStoragePlugin storagePlugin, DruidScanSpec scanSpec,
                         List<SchemaPath> columns) {
        super("someuser");
        objectName = UUID.randomUUID().toString();
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.scanSpec = scanSpec;
        this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
        init();
    }

    /**
     * Private constructor, used for cloning.
     * @param that The DruidGroupScan to clone
     */
    private DruidGroupScan(DruidGroupScan that) {
        super(that);
        this.columns = that.columns;
        this.scanSpec = that.scanSpec;
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.filterPushedDown = that.filterPushedDown;
        this.druidWorkList = that.druidWorkList;
        this.assignments = that.assignments;
        this.objectName = that.objectName;
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        DruidGroupScan newScan = new DruidGroupScan(this);
        newScan.columns = columns;
        return newScan;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        if (affinities == null) {
            affinities = AffinityCreator.getAffinityMap(druidWorkList);
        }
        return affinities;
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }

    @JsonIgnore
    public void setFilterPushedDown(boolean filterPushedDown) {
        this.filterPushedDown = filterPushedDown;
    }

    private void init()
    {
        logger.debug("Adding Druid Work for Table - " + getTableName() + " Filter - " + getScanSpec().getFilters());

        DruidWork druidWork =
                new DruidWork(
                        new DruidSubScan.DruidSubScanSpec(
                                getTableName(),
                                getScanSpec().getFilters()
                        )
                );
        druidWorkList.add(druidWork);
    }

    private static class DruidWork implements CompleteWork {

        private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();

        private DruidSubScan.DruidSubScanSpec druidSubScanSpec;

        public DruidWork(DruidSubScan.DruidSubScanSpec druidSubScanSpec) {
            this.druidSubScanSpec = druidSubScanSpec;
        }

        public DruidSubScan.DruidSubScanSpec getDruidSubScanSpec() {
            return druidSubScanSpec;
        }

        @Override
        public long getTotalBytes() {
            return DEFAULT_TABLET_SIZE;
        }

        @Override
        public EndpointByteMap getByteMap() {
            return byteMap;
        }

        @Override
        public int compareTo(CompleteWork o) {
            return 0;
        }
    }

    //TODO - MAY GET MORE PRECISE COUNT FROM DRUID ITSELF.
    public ScanStats getScanStats() {
        long recordCount = 100000;
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        assignments = AssignmentCreator.getMappings(endpoints, druidWorkList);
    }

    @Override
    public DruidSubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {

        List<DruidWork> workList = assignments.get(minorFragmentId);

        List<DruidSubScan.DruidSubScanSpec> scanSpecList = Lists.newArrayList();
        for (DruidWork druidWork : workList) {
            scanSpecList
                    .add(
                            new DruidSubScan.DruidSubScanSpec(
                                    druidWork.getDruidSubScanSpec().getDataSourceName(),
                                    druidWork.getDruidSubScanSpec().getFilter()
                            )
                    );
        }

        return new DruidSubScan(storagePlugin, storagePluginConfig, scanSpecList, this.columns);
    }

    @JsonIgnore
    public String getTableName() {
        return getScanSpec().getDataSourceName();
    }

    @Override
    public int getMaxParallelizationWidth() {
        return druidWorkList.size();
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @JsonProperty("druidScanSpec")
    public DruidScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonProperty("storage")
    public DruidStoragePluginConfig getStorageConfig() {
        return storagePluginConfig;
    }

    @JsonIgnore
    public DruidStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new DruidGroupScan(this);
    }
}
