package org.apache.drill.exec.store.druid.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang.StringUtils;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.druid.DruidScanSpec;
import org.apache.drill.exec.store.druid.DruidStoragePlugin;
import org.apache.drill.exec.store.druid.DruidStoragePluginConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DruidSchemaFactory implements SchemaFactory {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DruidSchemaFactory.class);

    final String schemaName;
    final DruidStoragePlugin plugin;

    public DruidSchemaFactory(DruidStoragePlugin plugin, String schemaName)
    {
        this.schemaName = schemaName;
        this.plugin = plugin;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        DruidDataSources schema = new DruidDataSources(schemaName);
        SchemaPlus hPlus = parent.add(schemaName, schema);
        schema.setHolder(hPlus);
    }

    public class DruidDataSources extends AbstractSchema {

        private final Set<String> tableNames;
        private final Map<String, DrillTable> drillTables = Maps.newHashMap();

        public DruidDataSources(String name) {
            super(ImmutableList.<String>of(), name);
            this.tableNames = this.getTableNames();
        }

        public void setHolder(SchemaPlus plusOfThis) {
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            return null;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Collections.emptySet();
        }

        @Override
        public Table getTable(String tableName) {

            if (!tableNames.contains(tableName)) { // table does not exist
                return null;
            }

            try {

                if (! drillTables.containsKey(tableName)) {
                    DruidScanSpec scanSpec = new DruidScanSpec(tableName);
                    DynamicDrillTable dynamicDrillTable = new DynamicDrillTable(plugin, schemaName, null, scanSpec);
                    drillTables.put(tableName, dynamicDrillTable);
                }

                return drillTables.get(tableName);
            } catch (Exception e) {
                logger.warn("Failure while retrieving druid table {}", tableName, e);
                return null;
            }
        }

        @Override
        public Set<String> getTableNames() {
            try {
                Set<String> dataSources = plugin.getAdminClient().GetDataSources();
                logger.debug("Found Druid DataSources - " + StringUtils.join(dataSources, ","));
                return dataSources;
            } catch (Exception e) {
                logger.error("Failure while loading druid datasources for database '{}'.", schemaName, e);
                return Collections.emptySet();
            }
        }

        @Override
        public String getTypeName() {
            return DruidStoragePluginConfig.NAME;
        }
    }
}