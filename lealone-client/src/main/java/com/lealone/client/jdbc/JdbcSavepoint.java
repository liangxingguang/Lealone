/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.client.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceObject;
import com.lealone.common.trace.TraceObjectType;
import com.lealone.common.util.StringUtils;
import com.lealone.db.api.ErrorCode;

/**
 * A savepoint is a point inside a transaction to where a transaction can be
 * rolled back. The tasks that where done before the savepoint are not rolled
 * back in this case.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcSavepoint extends TraceObject implements Savepoint {

    private static final String SYSTEM_SAVEPOINT_PREFIX = "SYSTEM_SAVEPOINT_";

    private final int savepointId;
    private final String name;
    private JdbcConnection conn;

    JdbcSavepoint(JdbcConnection conn, int savepointId, String name, Trace trace, int id) {
        this.conn = conn;
        this.savepointId = savepointId;
        this.name = name;
        this.trace = conn.getTrace(TraceObjectType.SAVEPOINT, id);
    }

    /**
     * Release this savepoint. This method only set the connection to null and
     * does not execute a statement.
     */
    void release() {
        this.conn = null;
    }

    /**
     * Get the savepoint name for this name or id.
     * If the name is null, the id is used.
     *
     * @param name the name (may be null)
     * @param id the id
     * @return the savepoint name
     */
    static String getName(String name, int id) {
        if (name != null) {
            return StringUtils.quoteJavaString(name);
        }
        return SYSTEM_SAVEPOINT_PREFIX + id;
    }

    /**
     * Roll back to this savepoint.
     */
    void rollback() throws SQLException {
        checkValid();
        conn.createStatement().executeUpdateAsync("ROLLBACK TO SAVEPOINT " + getName(name, savepointId));
    }

    private void checkValid() {
        if (conn == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, getName(name, savepointId));
        }
    }

    /**
     * Get the generated id of this savepoint.
     * @return the id
     */
    @Override
    public int getSavepointId() throws SQLException {
        try {
            debugCodeCall("getSavepointId");
            checkValid();
            if (name != null) {
                throw DbException.get(ErrorCode.SAVEPOINT_IS_NAMED);
            }
            return savepointId;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the name of this savepoint.
     * @return the name
     */
    @Override
    public String getSavepointName() throws SQLException {
        try {
            debugCodeCall("getSavepointName");
            checkValid();
            if (name == null) {
                throw DbException.get(ErrorCode.SAVEPOINT_IS_UNNAMED);
            }
            return name;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": id=" + savepointId + " name=" + name;
    }
}
