/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import java.util.ArrayList;

import com.lealone.sql.expression.Alias;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.ExpressionList;
import com.lealone.sql.expression.Operation;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.Rownum;
import com.lealone.sql.expression.SelectOrderBy;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.Variable;
import com.lealone.sql.expression.Wildcard;
import com.lealone.sql.expression.aggregate.AGroupConcat;
import com.lealone.sql.expression.aggregate.Aggregate;
import com.lealone.sql.expression.aggregate.JavaAggregate;
import com.lealone.sql.expression.condition.CompareLike;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.condition.ConditionAndOr;
import com.lealone.sql.expression.condition.ConditionExists;
import com.lealone.sql.expression.condition.ConditionIn;
import com.lealone.sql.expression.condition.ConditionInConstantSet;
import com.lealone.sql.expression.condition.ConditionInSelect;
import com.lealone.sql.expression.condition.ConditionNot;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.expression.function.JavaFunction;
import com.lealone.sql.expression.function.TableFunction;
import com.lealone.sql.expression.subquery.SubQuery;
import com.lealone.sql.query.Query;
import com.lealone.sql.query.Select;
import com.lealone.sql.query.SelectUnion;

public class VoidExpressionVisitor extends ExpressionVisitorBase<Void> {

    @Override
    public Void visitExpression(Expression e) {
        return null;
    }

    @Override
    public Void visitAlias(Alias e) {
        return e.getNonAliasExpression().accept(this);
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        return null;
    }

    @Override
    public Void visitExpressionList(ExpressionList e) {
        for (Expression e2 : e.getList()) {
            e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitOperation(Operation e) {
        e.getLeft().accept(this);
        if (e.getRight() != null)
            e.getRight().accept(this);
        return null;
    }

    @Override
    public Void visitParameter(Parameter e) {
        return null;
    }

    @Override
    public Void visitRownum(Rownum e) {
        return null;
    }

    @Override
    public Void visitSequenceValue(SequenceValue e) {
        return null;
    }

    @Override
    public Void visitSubQuery(SubQuery e) {
        visitQuery(e.getQuery());
        return null;
    }

    protected Void visitQuery(Query query) {
        query.accept(this);
        return null;
    }

    @Override
    public Void visitValueExpression(ValueExpression e) {
        return null;
    }

    @Override
    public Void visitVariable(Variable e) {
        return null;
    }

    @Override
    public Void visitWildcard(Wildcard e) {
        return null;
    }

    @Override
    public Void visitCompareLike(CompareLike e) {
        e.getLeft().accept(this);
        e.getRight().accept(this);
        if (e.getEscape() != null)
            e.getEscape().accept(this);
        return null;
    }

    @Override
    public Void visitComparison(Comparison e) {
        e.getLeft().accept(this);
        if (e.getRight() != null)
            e.getRight().accept(this);
        return null;
    }

    @Override
    public Void visitConditionAndOr(ConditionAndOr e) {
        e.getLeft().accept(this);
        e.getRight().accept(this);
        return null;
    }

    @Override
    public Void visitConditionExists(ConditionExists e) {
        visitQuery(e.getQuery());
        return null;
    }

    @Override
    public Void visitConditionIn(ConditionIn e) {
        e.getLeft().accept(this);
        for (Expression e2 : e.getValueList()) {
            e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitConditionInConstantSet(ConditionInConstantSet e) {
        e.getLeft().accept(this);
        return null;
    }

    @Override
    public Void visitConditionInSelect(ConditionInSelect e) {
        e.getLeft().accept(this);
        visitQuery(e.getQuery());
        return null;
    }

    @Override
    public Void visitConditionNot(ConditionNot e) {
        e.getCondition().accept(this);
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        if (e.getOn() != null)
            e.getOn().accept(this);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        if (e.getOn() != null)
            e.getOn().accept(this);
        if (e.getGroupConcatSeparator() != null)
            e.getGroupConcatSeparator().accept(this);
        if (e.getGroupConcatOrderList() != null) {
            for (SelectOrderBy o : e.getGroupConcatOrderList()) {
                o.expression.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        for (Expression e2 : e.getArgs()) {
            if (e2 != null)
                e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitFunction(Function e) {
        for (Expression e2 : e.getArgs()) {
            if (e2 != null)
                e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitJavaFunction(JavaFunction e) {
        for (Expression e2 : e.getArgs()) {
            if (e2 != null)
                e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitTableFunction(TableFunction e) {
        for (Expression e2 : e.getArgs()) {
            if (e2 != null)
                e2.accept(this);
        }
        return null;
    }

    @Override
    public Void visitSelect(Select s) {
        ExpressionVisitor<Void> v2 = incrementQueryLevel(1);
        ArrayList<Expression> expressions = s.getExpressions();
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i);
            e.accept(v2);
        }
        if (s.getCondition() != null) {
            s.getCondition().accept(v2);
        }
        if (s.getHaving() != null) {
            s.getHaving().accept(v2);
        }
        return null;
    }

    @Override
    public Void visitSelectUnion(SelectUnion su) {
        su.getLeft().accept(this);
        su.getRight().accept(this);
        return null;
    }
}
