package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidPushDownFilterForScan extends StoragePluginOptimizerRule {

    private static final Logger logger = LoggerFactory
            .getLogger(DruidPushDownFilterForScan.class);

    public static final StoragePluginOptimizerRule INSTANCE = new DruidPushDownFilterForScan();

    private DruidPushDownFilterForScan() {
        super(
                RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
                "DruidPushDownFilterForScan");
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        final ScanPrel scan = (ScanPrel) relOptRuleCall.rel(1);
        final FilterPrel filter = (FilterPrel) relOptRuleCall.rel(0);
        final RexNode condition = filter.getCondition();

        DruidGroupScan groupScan = (DruidGroupScan) scan.getGroupScan();
        if (groupScan.isFilterPushedDown()) {
            return;
        }

        LogicalExpression conditionExp =
                DrillOptiq.toDrill(
                        new DrillParseContext(PrelUtil.getPlannerSettings(relOptRuleCall.getPlanner())),
                        scan,
                        condition);

        DruidFilterBuilder druidFilterBuilder =
                new DruidFilterBuilder(groupScan, conditionExp);

        DruidScanSpec newScanSpec = null;
        try {
            newScanSpec = druidFilterBuilder.parseTree();
        } catch (JsonProcessingException e) {
            logger.error("Error in onMatch. Exception - " + e.getMessage());
        }
        if (newScanSpec == null) {
            return; // no filter pushdown so nothing to apply.
        }

        DruidGroupScan newGroupsScan = new DruidGroupScan(groupScan.getStoragePlugin(),
                newScanSpec, groupScan.getColumns());
        newGroupsScan.setFilterPushedDown(true);

        final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(),
                newGroupsScan, scan.getRowType());
        if (druidFilterBuilder.isAllExpressionsConverted()) {
      /*
       * Since we could convert the entire filter condition expression into an
       * Druid filter, we can eliminate the filter operator altogether.
       */
            relOptRuleCall.transformTo(newScanPrel);
        } else {
            relOptRuleCall.transformTo(filter.copy(filter.getTraitSet(),
                    ImmutableList.of((RelNode) newScanPrel)));
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = (ScanPrel) call.rel(1);
        if (scan.getGroupScan() instanceof DruidGroupScan) {
            return super.matches(call);
        }
        return false;
    }
}
