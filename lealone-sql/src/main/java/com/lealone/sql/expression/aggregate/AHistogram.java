/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.aggregate;

import java.util.Arrays;
import java.util.Comparator;

import com.lealone.db.Constants;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.ValueHashMap;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.query.Select;

public class AHistogram extends BuiltInAggregate {

    public AHistogram(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        dataType = Value.ARRAY;
        scale = 0;
        precision = displaySize = Integer.MAX_VALUE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataHistogram();
    }

    @Override
    public String getSQL() {
        return getSQL("HISTOGRAM");
    }

    // 会忽略distinct
    // 计算每个值出现的次数
    public class AggregateDataHistogram extends AggregateData {

        private long count;
        private ValueHashMap<AggregateDataHistogram> distinctValues;

        @Override
        public void add(ServerSession session, Value v) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            AggregateDataHistogram a = distinctValues.get(v);
            if (a == null) {
                if (distinctValues.size() < Constants.SELECTIVITY_DISTINCT_COUNT) {
                    a = new AggregateDataHistogram();
                    distinctValues.put(v, a);
                }
            }
            if (a != null) {
                a.count++;
            }
        }

        @Override
        Value getValue(ServerSession session) {
            ValueArray[] values = new ValueArray[distinctValues.size()];
            int i = 0;
            for (Value dv : distinctValues.keys()) {
                AggregateDataHistogram d = distinctValues.get(dv);
                values[i] = ValueArray.get(new Value[] { dv, ValueLong.get(d.count) });
                i++;
            }
            final CompareMode compareMode = session.getDatabase().getCompareMode();
            Arrays.sort(values, new Comparator<ValueArray>() {
                @Override
                public int compare(ValueArray v1, ValueArray v2) {
                    Value a1 = v1.getList()[0];
                    Value a2 = v2.getList()[0];
                    return a1.compareTo(a2, compareMode);
                }
            });
            Value v = ValueArray.get(values);
            return v.convertTo(dataType);
        }
    }
}
