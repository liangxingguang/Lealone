/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.condition;

import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.optimizer.TableFilter;

/**
 * A NOT condition.
 */
public class ConditionNot extends Condition {

    private Expression condition;

    public ConditionNot(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return condition;
    }

    @Override
    public Value getValue(ServerSession session) {
        Value v = condition.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return v;
        }
        return v.convertTo(Value.BOOLEAN).negate();
    }

    @Override
    public Expression optimize(ServerSession session) {
        Expression e2 = condition.getNotIfPossible(session);
        if (e2 != null) {
            return e2.optimize(session);
        }
        Expression expr = condition.optimize(session);
        if (expr.isConstant()) {
            Value v = expr.getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueExpression.getNull();
            }
            return ValueExpression.get(v.convertTo(Value.BOOLEAN).negate());
        }
        condition = expr;
        return this;
    }

    @Override
    public String getSQL() {
        return "(NOT " + condition.getSQL() + ")";
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (outerJoin) {
            // can not optimize:
            // select * from test t1 left join test t2 on t1.id = t2.id where
            // not t2.id is not null
            // to
            // select * from test t1 left join test t2 on t1.id = t2.id and
            // t2.id is not null
            return;
        }
        super.addFilterConditions(filter, outerJoin);
    }

    @Override
    public int getCost() {
        return condition.getCost();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitConditionNot(this);
    }
}
