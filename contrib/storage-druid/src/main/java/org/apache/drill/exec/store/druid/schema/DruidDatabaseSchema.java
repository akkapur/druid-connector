/*
package org.apache.drill.exec.store.druid.schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.druid.DruidStoragePluginConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidDatabaseSchema extends AbstractSchema {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(DruidDatabaseSchema.class);

    private final DruidSchemaFactory.DruidSchema druidSchema;
    private final Set<String> tableNames;

    private final Map<String, DrillTable> drillTables = Maps.newHashMap();

    public DruidDatabaseSchema(Set<String> tableList, DruidSchemaFactory.DruidSchema druidSchema,
                               String name) {
        super(druidSchema.getSchemaPath(), name);
        this.druidSchema = druidSchema;
        this.tableNames = Sets.newHashSet(tableList);
    }

    @Override
    public Table getTable(String tableName) {
        if (!tableNames.contains(tableName)) { // table does not exist
            return null;
        }

        if (! drillTables.containsKey(tableName)) {
            drillTables.put(tableName, druidSchema.getDrillTable(tableName));
        }

        return drillTables.get(tableName);
    }

    @Override
    public Set<String> getTableNames() {
        return tableNames;
    }

    @Override
    public String getTypeName() {
        return DruidStoragePluginConfig.NAME;
    }
}
*/
