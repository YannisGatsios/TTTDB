package com.database.db.api;

import java.util.EnumMap;

import com.database.db.api.Condition.Conditions;

public final class ConditionUtils {

    public static RangeBounds extractRange(EnumMap<Conditions, Object> conditionList) {
        Object start = null;
        Object end = null;

        if (conditionList.containsKey(Conditions.IS_BIGGER))
            start = conditionList.get(Conditions.IS_BIGGER);
        if (conditionList.containsKey(Conditions.IS_SMALLER))
            end = conditionList.get(Conditions.IS_SMALLER);
        if (conditionList.containsKey(Conditions.IS_BIGGER_OR_EQUAL))
            start = conditionList.get(Conditions.IS_BIGGER_OR_EQUAL);
        if (conditionList.containsKey(Conditions.IS_SMALLER_OR_EQUAL))
            end = conditionList.get(Conditions.IS_SMALLER_OR_EQUAL);

        if (start == null && end == null && conditionList.containsKey(Conditions.IS_EQUAL)) {
            Object equal = conditionList.get(Conditions.IS_EQUAL);
            start = equal;
            end = equal;
        }

        return new RangeBounds(start, end);
    }

    public record RangeBounds(Object start, Object end) {}
}
