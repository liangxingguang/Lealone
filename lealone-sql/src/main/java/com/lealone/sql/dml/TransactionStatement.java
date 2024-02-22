/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * Represents a transactional statement.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TransactionStatement extends ManipulationStatement {

    private final int type;
    private String savepointName;

    public TransactionStatement(ServerSession session, int type) {
        super(session);
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public boolean isTransactionStatement() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setSavepointName(String name) {
        if (INTERNAL_SAVEPOINT.equals(name))
            throw DbException.getUnsupportedException("Savepoint name cannot use " + INTERNAL_SAVEPOINT);
        this.savepointName = name;
    }

    public void setTransactionName(String string) {
        // 2PC语句已经废弃
    }

    @Override
    public int update() {
        switch (type) {
        case SQLStatement.SET_AUTOCOMMIT_TRUE:
            session.setAutoCommit(true);
            break;
        case SQLStatement.SET_AUTOCOMMIT_FALSE:
            session.setAutoCommit(false);
            break;
        case SQLStatement.BEGIN:
            session.begin();
            break;
        case SQLStatement.COMMIT:
            // 在session.stopCurrentCommand中处理
            // session.asyncCommit(null);
            break;
        case SQLStatement.ROLLBACK:
            session.rollback();
            break;
        case SQLStatement.SAVEPOINT:
            session.addSavepoint(savepointName);
            break;
        case SQLStatement.ROLLBACK_TO_SAVEPOINT:
            session.rollbackToSavepoint(savepointName);
            break;
        case SQLStatement.CHECKPOINT:
            session.getUser().checkAdmin();
            session.getDatabase().checkpoint();
            break;
        // 以下三个是跟2PC相关的语句，已经废弃
        case SQLStatement.PREPARE_COMMIT:
        case SQLStatement.COMMIT_TRANSACTION:
        case SQLStatement.ROLLBACK_TRANSACTION:
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }
}
