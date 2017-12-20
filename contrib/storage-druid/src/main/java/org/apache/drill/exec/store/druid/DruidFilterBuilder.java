package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.druid.common.*;
import org.apache.drill.exec.store.druid.druid.SelectQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DruidFilterBuilder extends
        AbstractExprVisitor<DruidScanSpec, Void, RuntimeException> {

    static final Logger logger = LoggerFactory
            .getLogger(DruidFilterBuilder.class);

    final DruidGroupScan groupScan;
    final LogicalExpression le;
    private boolean allExpressionsConverted = true;

    public DruidFilterBuilder(DruidGroupScan groupScan,
                              LogicalExpression conditionExp) {
        this.groupScan = groupScan;
        this.le = conditionExp;
    }

    public DruidScanSpec parseTree() throws JsonProcessingException {
        logger.debug("DruidScanSpec parseTree() called.");

        DruidScanSpec parsedSpec = le.accept(this, null);
        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getScanSpec(),
                    parsedSpec);
        }
        return parsedSpec;
    }

    private DruidScanSpec mergeScanSpecs(String functionName,
                                         DruidScanSpec leftScanSpec,
                                         DruidScanSpec rightScanSpec) throws JsonProcessingException {

        logger.debug("mergeScanSpecs called for functionName - " + functionName);

        String newFilter = null;

        switch (functionName) {
            case "booleanAnd":
                if (StringUtils.isNotBlank(leftScanSpec.getFilters())
                        && StringUtils.isNotBlank(rightScanSpec.getFilters())) {
                    newFilter =
                            DruidUtils
                                    .andFilterAtIndex(
                                            leftScanSpec.getFilters(),
                                            rightScanSpec.getFilters()
                                    );
                } else if (leftScanSpec.getFilters() != null) {
                    newFilter = leftScanSpec.getFilters();
                } else {
                    newFilter = rightScanSpec.getFilters();
                }
                break;
            case "booleanOr":
                newFilter =
                        DruidUtils
                                .orFilterAtIndex(
                                        leftScanSpec.getFilters(),
                                        rightScanSpec.getFilters()
                                );
        }

        return new DruidScanSpec(groupScan.getScanSpec().getDataSourceName(), newFilter);
    }

    public boolean isAllExpressionsConverted() {
        return allExpressionsConverted;
    }

    @Override
    public DruidScanSpec visitUnknown(LogicalExpression e, Void value)
            throws RuntimeException {
        allExpressionsConverted = false;
        return null;
    }

    @Override
    public DruidScanSpec visitBooleanOperator(BooleanOperator op, Void value) {
        List<LogicalExpression> args = op.args;
        DruidScanSpec nodeScanSpec = null;
        String functionName = op.getName();

        logger.debug("visitBooleanOperator Called. FunctionName - " + functionName);

        for (int i = 0; i < args.size(); ++i) {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    if (nodeScanSpec == null) {
                        nodeScanSpec = args.get(i).accept(this, null);
                    } else {
                        DruidScanSpec scanSpec = args.get(i).accept(this, null);
                        if (scanSpec != null) {
                            try {
                                nodeScanSpec = mergeScanSpecs(functionName, nodeScanSpec, scanSpec);
                            } catch (JsonProcessingException e) {
                                logger.error("Error in visitBooleanOperator. Exception - " + e.getMessage());
                            }
                        } else {
                            allExpressionsConverted = false;
                        }
                    }
                    break;
            }
        }
        return nodeScanSpec;
    }

    @Override
    public DruidScanSpec visitFunctionCall(FunctionCall call, Void value)
            throws RuntimeException {
        DruidScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        logger.debug("visitFunctionCall Called. FunctionName - " + functionName);

        if (DruidCompareFunctionProcessor.isCompareFunction(functionName)) {
            DruidCompareFunctionProcessor processor = DruidCompareFunctionProcessor
                    .process(call);
            if (processor.isSuccess()) {
                try {
                    nodeScanSpec = createDruidScanSpec(processor.getFunctionName(),
                            processor.getPath(), processor.getValue());
                } catch (Exception e) {
                    logger.error(" Failed to create Filter ", e);
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    DruidScanSpec leftScanSpec = args.get(0).accept(this, null);
                    DruidScanSpec rightScanSpec = args.get(1).accept(this, null);
                    if (leftScanSpec != null && rightScanSpec != null) {
                        try {
                            nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec,
                                    rightScanSpec);
                        } catch (JsonProcessingException e) {
                            logger.error("Error in visitFunctionCall. Exception - " + e.getMessage());
                            throw new RuntimeException(e.getMessage(), e);
                        }
                    } else {
                        allExpressionsConverted = false;
                        if ("booleanAnd".equals(functionName)) {
                            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
                        }
                    }
                    break;
            }
        }

        if (nodeScanSpec == null) {
            allExpressionsConverted = false;
        }

        return nodeScanSpec;
    }

    private DruidScanSpec createDruidScanSpec(String functionName,
                                              SchemaPath field,
                                              Object fieldValue) throws ClassNotFoundException,
            IOException {
        // extract the field name

        String fieldName = field.getAsUnescapedPath();
        String filter;

        logger.debug("createDruidScanSpec called. FunctionName - "
                + functionName + ", field - " + fieldName + ", fieldValue - " + fieldValue);

        switch (functionName) {
            case "equal":
            {
                if (fieldName.equalsIgnoreCase(SelectQuery.IntervalDimensionName)) {
                    DruidIntervalFilter druidIntervalFilter = new DruidIntervalFilter((String)fieldValue);
                    filter = druidIntervalFilter.toJson();
                    break;
                } else {
                    DruidSelectorFilter druidSelectorFilter = new DruidSelectorFilter(fieldName, (String) fieldValue);
                    filter = druidSelectorFilter.toJson();
                    break;
                }
            }
            case "not_equal":
            {
                DruidSelectorFilter druidSelectorFilter = new DruidSelectorFilter(fieldName, (String) fieldValue);
                String selectorFilter = druidSelectorFilter.toJson();
                DruidNotFilter druidNotFilter = new DruidNotFilter(selectorFilter);
                filter = druidNotFilter.toJson();
                break;
            }
            case "greater_than_or_equal_to":
            {
                Boolean isAlphaNumeric = (fieldValue instanceof Number);
                DruidBoundFilter druidBoundFilter = new DruidBoundFilter(fieldName, (String) fieldValue, null);
                druidBoundFilter.setAlphaNumeric(isAlphaNumeric);
                filter = druidBoundFilter.toJson();
                break;
            }
            case "greater_than":
            {
                Boolean isAlphaNumeric = (fieldValue instanceof Number);
                DruidBoundFilter druidBoundFilter = new DruidBoundFilter(fieldName, (String) fieldValue, null);
                druidBoundFilter.setAlphaNumeric(isAlphaNumeric);
                druidBoundFilter.setLowerStrict(true);
                filter = druidBoundFilter.toJson();
                break;
            }
            case "less_than_or_equal_to":
            {
                Boolean isAlphaNumeric = (fieldValue instanceof Number);
                DruidBoundFilter druidBoundFilter = new DruidBoundFilter(fieldName, null, (String) fieldValue);
                druidBoundFilter.setAlphaNumeric(isAlphaNumeric);
                filter = druidBoundFilter.toJson();
                break;
            }
            case "less_than":
            {
                Boolean isAlphaNumeric = (fieldValue instanceof Number);
                DruidBoundFilter druidBoundFilter = new DruidBoundFilter(fieldName, null, (String) fieldValue);
                druidBoundFilter.setAlphaNumeric(isAlphaNumeric);
                druidBoundFilter.setUpperStrict(true);
                filter = druidBoundFilter.toJson();
                break;
            }
            case "isnull":
            case "isNull":
            case "is null":
            {
                DruidSelectorFilter druidSelectorFilter = new DruidSelectorFilter(fieldName, null);
                filter = druidSelectorFilter.toJson();
                break;
            }
            case "isnotnull":
            case "isNotNull":
            case "is not null":
            {
                DruidSelectorFilter druidSelectorFilter = new DruidSelectorFilter(fieldName, null);
                String selectorFilter = druidSelectorFilter.toJson();
                DruidNotFilter druidNotFilter = new DruidNotFilter(selectorFilter);
                filter = druidNotFilter.toJson();
                break;
            }
            case "like":
            {
				String prefix = "$regex$_";
				if(((String)fieldValue).startsWith(prefix) )
				{
					DruidRegexFilter druidRegexFilter = new DruidRegexFilter(fieldName, ((String) fieldValue).substring(prefix.length()));
					filter = druidRegexFilter.toJson();
				}
				else
				{
					DruidSearchFilter druidSearchFilter = new DruidSearchFilter(fieldName, false, (String) fieldValue);
					filter = druidSearchFilter.toJson();
				}
                break;
            }
            default:
                String message = "No support for functionName-" + functionName;
                logger.error(message);
                throw new UnsupportedOperationException(message);
        }

        String dataSource = groupScan.getScanSpec().getDataSourceName();
        DruidScanSpec scanSpec = new DruidScanSpec(dataSource, filter);
        logger.debug("Created new DruidScanSpec with filter - " + filter + " for DataSource - " + dataSource);
        return scanSpec;
    }
}
