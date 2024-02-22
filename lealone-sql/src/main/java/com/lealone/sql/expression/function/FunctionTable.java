/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.function;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableType;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueResultSet;
import com.lealone.sql.expression.Expression;

/**
 * A table backed by a system or user-defined function that returns a result set.
 * 
 * @author H2 Group
 * @author zhh
 */
public class FunctionTable extends Table {

    private final Function function;
    private final Expression functionExpr;
    private LocalResult cachedResult;
    private Value cachedValue;

    public FunctionTable(Schema schema, ServerSession session, Function function) {
        super(schema, 0, function.getName(), false, true);
        this.function = function;
        functionExpr = function.optimize(session);
        int type = function.getType();
        if (type != Value.RESULT_SET) {
            throw DbException.get(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ValueResultSet template = function.getValueForColumnList(session, function.getArgs());
        if (template == null) {
            throw DbException.get(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ResultSet rs = template.getResultSet();
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            Column[] cols = new Column[columnCount];
            for (int i = 0; i < columnCount; i++) {
                cols[i] = new Column(meta.getColumnName(i + 1),
                        DataType.getValueTypeFromResultSet(meta, i + 1), meta.getPrecision(i + 1),
                        meta.getScale(i + 1), meta.getColumnDisplaySize(i + 1));
            }
            setColumns(cols);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public TableType getTableType() {
        return TableType.FUNCTION_TABLE;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return new FunctionIndex(this, IndexColumn.wrap(columns));
    }

    @Override
    public long getMaxDataModificationId() {
        return database.getModificationDataId();
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

    @Override
    public boolean canReference() {
        return false;
    }

    @Override
    public boolean canDrop() {
        throw DbException.getInternalError();
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return Long.MAX_VALUE;
    }

    @Override
    public long getRowCountApproximation() {
        return Long.MAX_VALUE;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getSQL() {
        return function.getSQL();
    }

    /**
     * Read the result from the function. This method buffers the result in a
     * temporary file.
     *
     * @param session the session
     * @return the result
     */
    public Result getResult(ServerSession session) {
        ValueResultSet v = getValueResultSet(session);
        if (v == null) {
            return null;
        }
        if (cachedResult != null && cachedValue == v) {
            cachedResult.reset();
            return cachedResult;
        }
        ResultSet rs = v.getResultSet();
        LocalResult result = LocalResult.read(session, Expression.getExpressionColumns(session, rs), rs,
                0);
        if (function.isDeterministic()) {
            cachedResult = result;
            cachedValue = v;
        }
        return result;
    }

    /**
     * Read the result set from the function. This method doesn't cache.
     *
     * @param session the session
     * @return the result set
     */
    public ResultSet getResultSet(ServerSession session) {
        ValueResultSet v = getValueResultSet(session);
        return v == null ? null : v.getResultSet();
    }

    private ValueResultSet getValueResultSet(ServerSession session) {
        Value v = functionExpr.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return null;
        }
        return (ValueResultSet) v;
    }

    boolean isBufferResultSetToLocalTemp() {
        return function.isBufferResultSetToLocalTemp();
    }
}
