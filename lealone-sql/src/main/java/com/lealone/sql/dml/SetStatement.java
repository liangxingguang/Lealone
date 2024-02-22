/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.ValueInt;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ValueExpression;

/**
 * This class represents the statement
 * SET
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class SetStatement extends ManipulationStatement {

    protected Expression expression;
    protected String stringValue;
    protected String[] stringValueList;

    public SetStatement(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.SET;
    }

    protected abstract String getSettingName();

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    protected int getIntValue() {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    protected String getStringValue() {
        if (stringValue != null)
            return stringValue;
        else if (stringValueList != null)
            return StringUtils.arrayCombine(stringValueList, ',');
        else if (expression != null)
            return expression.optimize(session).getValue(session).getString();
        else
            return "";
    }

    protected int getAndValidateIntValue() {
        return getAndValidateIntValue(0);
    }

    protected int getAndValidateIntValue(int lessThan) {
        int value = getIntValue();
        if (value < lessThan) {
            throw DbException.getInvalidValueException(getSettingName(), value);
        }
        return value;
    }

    protected boolean getAndValidateBooleanValue() {
        int value = getIntValue();
        if (value < 0 || value > 1) {
            throw DbException.getInvalidValueException(getSettingName(), value);
        }
        return value == 1;
    }

    protected void databaseChanged(Database db) {
        // the meta data information has changed
        db.getNextModificationDataId();
        // query caches might be affected as well, for example
        // when changing the compatibility mode
        db.getNextModificationMetaId();
    }
}
