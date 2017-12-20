package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.druid.schema.DruidSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class DruidStoragePlugin extends AbstractStoragePlugin {

    static final Logger logger = LoggerFactory
            .getLogger(DruidStoragePlugin.class);

    private final DrillbitContext context;
    private final DruidStoragePluginConfig pluginConfig;
    private final DruidAdminClient druidAdminClient;
    private final DruidQueryClient druidQueryClient;
    private final DruidSchemaFactory schemaFactory;

    public DruidStoragePlugin(DruidStoragePluginConfig pluginConfig, DrillbitContext context, String name) throws IOException,
            ExecutionSetupException {
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.druidAdminClient = new DruidAdminClient(pluginConfig.GetCoordinatorURI());
        this.druidQueryClient = new DruidQueryClient(pluginConfig.GetBrokerURI());
        this.schemaFactory = new DruidSchemaFactory(this, name);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        DruidScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<DruidScanSpec>() {});
        return new DruidGroupScan(this, scanSpec, null);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(DruidPushDownFilterForScan.INSTANCE);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public boolean supportsWrite() {
        return false;
    }

    @Override
    public DruidStoragePluginConfig getConfig() {
        return pluginConfig;
    }

    public DrillbitContext getContext() {
        return this.context;
    }

    public DruidAdminClient getAdminClient() {
        return this.druidAdminClient;
    }

    public DruidQueryClient getDruidQueryClient() {
        return this.druidQueryClient;
    }
}
