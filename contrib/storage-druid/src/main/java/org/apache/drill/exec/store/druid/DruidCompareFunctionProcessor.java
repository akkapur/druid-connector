package org.apache.drill.exec.store.druid;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

public class DruidCompareFunctionProcessor extends
        AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {

    private Object value;
    private boolean success;
    private boolean isEqualityFn;
    private SchemaPath path;
    private String functionName;

    public static boolean isCompareFunction(String functionName) {
        return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
    }

    public static DruidCompareFunctionProcessor process(FunctionCall call) {
        String functionName = call.getName();
        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg = call.args.size() == 2 ? call.args.get(1)
                : null;
        DruidCompareFunctionProcessor evaluator = new DruidCompareFunctionProcessor(
                functionName);

        if (valueArg != null) { // binary function
            if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
                LogicalExpression swapArg = valueArg;
                valueArg = nameArg;
                nameArg = swapArg;
                evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP
                        .get(functionName);
            }
            evaluator.success = nameArg.accept(evaluator, valueArg);
        } else if (call.args.get(0) instanceof SchemaPath) {
            evaluator.success = true;
            evaluator.path = (SchemaPath) nameArg;
        }

        return evaluator;
    }

    public DruidCompareFunctionProcessor(String functionName) {
        this.success = false;
        this.functionName = functionName;
        this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP
                .containsKey(functionName)
                && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(
                functionName);
    }

    public Object getValue() {
        return value;
    }

    public boolean isSuccess() {
        return success;
    }

    public SchemaPath getPath() {
        return path;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Boolean visitCastExpression(CastExpression e,
                                       LogicalExpression valueArg) throws RuntimeException {
        if (e.getInput() instanceof CastExpression
                || e.getInput() instanceof SchemaPath) {
            return e.getInput().accept(this, valueArg);
        }
        return false;
    }

    @Override
    public Boolean visitConvertExpression(ConvertExpression e,
                                          LogicalExpression valueArg) throws RuntimeException {
        if (e.getConvertFunction() == ConvertExpression.CONVERT_FROM
                && e.getInput() instanceof SchemaPath) {
            String encodingType = e.getEncodingType();
            switch (encodingType) {
                case "INT_BE":
                case "INT":
                case "UINT_BE":
                case "UINT":
                case "UINT4_BE":
                case "UINT4":
                    if (valueArg instanceof ValueExpressions.IntExpression
                            && (isEqualityFn || encodingType.startsWith("U"))) {
                        this.value = ((ValueExpressions.IntExpression) valueArg).getInt();
                    }
                    break;
                case "BIGINT_BE":
                case "BIGINT":
                case "UINT8_BE":
                case "UINT8":
                    if (valueArg instanceof ValueExpressions.LongExpression
                            && (isEqualityFn || encodingType.startsWith("U"))) {
                        this.value = ((ValueExpressions.LongExpression) valueArg).getLong();
                    }
                    break;
                case "FLOAT":
                    if (valueArg instanceof ValueExpressions.FloatExpression && isEqualityFn) {
                        this.value = ((ValueExpressions.FloatExpression) valueArg).getFloat();
                    }
                    break;
                case "DOUBLE":
                    if (valueArg instanceof ValueExpressions.DoubleExpression && isEqualityFn) {
                        this.value = ((ValueExpressions.DoubleExpression) valueArg).getDouble();
                    }
                    break;
                case "TIME_EPOCH":
                case "TIME_EPOCH_BE":
                    if (valueArg instanceof ValueExpressions.TimeExpression) {
                        this.value = ((ValueExpressions.TimeExpression) valueArg).getTime();
                    }
                    break;
                case "DATE_EPOCH":
                case "DATE_EPOCH_BE":
                    if (valueArg instanceof ValueExpressions.DateExpression) {
                        this.value = ((ValueExpressions.DateExpression) valueArg).getDate();
                    }
                    break;
                case "BOOLEAN_BYTE":
                    if (valueArg instanceof ValueExpressions.BooleanExpression) {
                        this.value = ((ValueExpressions.BooleanExpression) valueArg).getBoolean();
                    }
                    break;
                case "UTF8":
                    // let visitSchemaPath() handle this.
                    return e.getInput().accept(this, valueArg);
            }

            if (value != null) {
                this.path = (SchemaPath) e.getInput();
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
            throws RuntimeException {
        return false;
    }

    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
            throws RuntimeException {
        if (valueArg instanceof ValueExpressions.QuotedString) {
            this.value = ((ValueExpressions.QuotedString) valueArg).value;
            this.path = path;
            return true;
        }

        if (valueArg instanceof ValueExpressions.IntExpression) {
            this.value = ((ValueExpressions.IntExpression) valueArg).getInt();
            this.path = path;
            return true;
        }

        if (valueArg instanceof ValueExpressions.LongExpression) {
            this.value = ((ValueExpressions.LongExpression) valueArg).getLong();
            this.path = path;
            return true;
        }

        if (valueArg instanceof ValueExpressions.FloatExpression) {
            this.value = ((ValueExpressions.FloatExpression) valueArg).getFloat();
            this.path = path;
            return true;
        }

        if (valueArg instanceof ValueExpressions.DoubleExpression) {
            this.value = ((ValueExpressions.DoubleExpression) valueArg).getDouble();
            this.path = path;
            return true;
        }

        if (valueArg instanceof ValueExpressions.BooleanExpression) {
            this.value = ((ValueExpressions.BooleanExpression) valueArg).getBoolean();
            this.path = path;
            return true;
        }

        return false;
    }

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
    static {
        ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet
                .builder();
        VALUE_EXPRESSION_CLASSES = builder.add(ValueExpressions.BooleanExpression.class)
                .add(ValueExpressions.DateExpression.class).add(ValueExpressions.DoubleExpression.class)
                .add(ValueExpressions.FloatExpression.class).add(ValueExpressions.IntExpression.class)
                .add(ValueExpressions.LongExpression.class).add(ValueExpressions.QuotedString.class)
                .add(ValueExpressions.TimeExpression.class).build();
    }

    private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
    static {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
                // unary functions
                .put("isnotnull", "isnotnull")
                .put("isNotNull", "isNotNull")
                .put("is not null", "is not null")
                .put("isnull", "isnull")
                .put("isNull", "isNull")
                .put("is null", "is null")
                // binary functions
                .put("equal", "equal").put("not_equal", "not_equal")
                .put("greater_than_or_equal_to", "less_than_or_equal_to")
                .put("greater_than", "less_than")
                .put("less_than_or_equal_to", "greater_than_or_equal_to")
                .put("less_than", "greater_than")
                .put("like", "like")
                .put("LIKE", "like").build();
    }
}
