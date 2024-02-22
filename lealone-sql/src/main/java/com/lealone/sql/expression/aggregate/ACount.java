/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.aggregate;

import com.lealone.db.session.ServerSession;
import com.lealone.db.util.ValueHashMap;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.query.Select;

// COUNT(x)
public class ACount extends BuiltInAggregate {

    public ACount(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    @Override
    public Expression optimize(ServerSession session) {
        super.optimize(session);
        dataType = Value.LONG;
        scale = 0;
        precision = ValueLong.PRECISION;
        displaySize = ValueLong.DISPLAY_SIZE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataCount();
    }

    @Override
    public String getSQL() {
        return getSQL("COUNT");
    }

    public class AggregateDataCount extends AggregateData {

        private long count;
        private ValueHashMap<AggregateDataCount> distinctValues;

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public ValueHashMap<AggregateDataCount> getDistinctValues() {
            return distinctValues;
        }

        public void setDistinctValues(ValueHashMap<AggregateDataCount> distinctValues) {
            this.distinctValues = distinctValues;
        }

        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public void add(ServerSession session, Value v) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            count++;
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
            }
        }

        @Override
        Value getValue(ServerSession session) {
            if (distinct) {
                if (distinctValues != null) {
                    count = distinctValues.size();
                } else {
                    count = 0;
                }
            }
            return ValueLong.get(count);
        }
    }
}
