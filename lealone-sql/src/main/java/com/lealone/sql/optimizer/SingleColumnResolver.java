/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.optimizer;

import com.lealone.db.session.Session;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.sql.IExpression;
import com.lealone.sql.query.Select;

/**
 * The single column resolver is like a table with exactly one row.
 * It is used to parse a simple one-column check constraint.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SingleColumnResolver extends ColumnResolverBase {

    private final Column column;
    private Value value;

    public SingleColumnResolver(Column column) {
        this.column = column;
    }

    @Override
    public Value getValue(Column col) {
        return value;
    }

    @Override
    public Column[] getColumns() {
        return new Column[] { column };
    }

    @Override
    public TableFilter getTableFilter() {
        return null;
    }

    @Override
    public Select getSelect() {
        return null;
    }

    @Override
    public Value getExpressionValue(Session session, IExpression e, Object data) {
        value = (Value) data;
        return e.getValue(session);
    }
}
