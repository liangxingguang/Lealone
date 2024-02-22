/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StringUtils;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.result.ResultTarget;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.sql.ISelectUnion;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.executor.YieldableBase;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.optimizer.ColumnResolver;
import com.lealone.sql.optimizer.TableFilter;

/**
 * Represents a union SELECT statement.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SelectUnion extends Query implements ISelectUnion {

    final int unionType;
    final Query left;
    final Query right;

    public SelectUnion(ServerSession session, int unionType, Query left, Query right) {
        super(session);
        this.unionType = unionType;
        this.left = left;
        this.right = right;
    }

    @Override
    public int getType() {
        return SQLStatement.SELECT;
    }

    @Override
    public int getUnionType() {
        return unionType;
    }

    @Override
    public Query getLeft() {
        return left;
    }

    @Override
    public Query getRight() {
        return right;
    }

    @Override
    public Future<Result> getMetaData() {
        int columnCount = left.getColumnCount();
        LocalResult result = new LocalResult(session, expressionArray, columnCount);
        result.done();
        return Future.succeededFuture(result);
    }

    @Override
    public LocalResult getEmptyResult() {
        int columnCount = left.getColumnCount();
        return new LocalResult(session, expressionArray, columnCount);
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        checkInit = true;
        left.init();
        right.init();
        int len = left.getColumnCount();
        if (len != right.getColumnCount()) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        ArrayList<Expression> le = left.getExpressions();
        // set the expressions to get the right column count and names,
        // but can't validate at this time
        expressions = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            expressions.add(l);
        }
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return this;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        isPrepared = true;
        left.prepare();
        right.prepare();
        int len = left.getColumnCount();
        // set the correct expressions now
        expressions = new ArrayList<>(len);
        ArrayList<Expression> le = left.getExpressions();
        ArrayList<Expression> re = right.getExpressions();
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            Expression r = re.get(i);
            int type = Value.getHigherOrder(l.getType(), r.getType());
            long prec = Math.max(l.getPrecision(), r.getPrecision());
            int scale = Math.max(l.getScale(), r.getScale());
            int displaySize = Math.max(l.getDisplaySize(), r.getDisplaySize());
            Column col = new Column(l.getAlias(), type, prec, scale, displaySize);
            Expression e = new ExpressionColumn(session.getDatabase(), col);
            expressions.add(e);
        }
        if (orderList != null) {
            initOrder(session, expressions, null, orderList, getColumnCount(), true, null);
            sort = prepareOrder(session, orderList, expressions.size());
            orderList = null;
        }
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
        return this;
    }

    @Override
    public double getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = left.getTables();
        set.addAll(right.getTables());
        return set;
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        left.setForUpdate(forUpdate);
        right.setForUpdate(forUpdate);
        isForUpdate = forUpdate;
    }

    @Override
    public int getColumnCount() {
        return left.getColumnCount();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
    }

    @Override
    public String getPlanSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getPlanSQL()).append(')');
        switch (unionType) {
        case UNION_ALL:
            buff.append("\nUNION ALL\n");
            break;
        case UNION:
            buff.append("\nUNION\n");
            break;
        case INTERSECT:
            buff.append("\nINTERSECT\n");
            break;
        case EXCEPT:
            buff.append("\nEXCEPT\n");
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        buff.append('(').append(right.getPlanSQL()).append(')');
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, exprList.length));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append("\nOFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitSelectUnion(this);
    }

    @Override
    public void fireBeforeSelectTriggers() {
        left.fireBeforeSelectTriggers();
        right.fireBeforeSelectTriggers();
    }

    @Override
    public List<TableFilter> getFilters() {
        List<TableFilter> filters = left.getFilters();
        filters.addAll(right.getFilters());
        return filters;
    }

    @Override
    public List<TableFilter> getTopFilters() {
        List<TableFilter> filters = left.getTopFilters();
        filters.addAll(right.getTopFilters());
        return filters;
    }

    @Override
    public boolean allowGlobalConditions() {
        return left.allowGlobalConditions() && right.allowGlobalConditions();
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        switch (unionType) {
        case UNION_ALL:
        case UNION:
        case INTERSECT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            right.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        case EXCEPT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
    }

    @Override
    public int getPriority() {
        return Math.min(left.getPriority(), right.getPriority());
    }

    @Override
    public Result query(int maxRows, ResultTarget target) {
        YieldableSelectUnion yieldable = new YieldableSelectUnion(this, maxRows, false, null, target);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableBase<Result> createYieldableQuery(int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler, ResultTarget target) {
        return new YieldableSelectUnion(this, maxRows, scrollable, asyncHandler, target);
    }
}
