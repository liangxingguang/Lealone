/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.admin;

import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * SHUTDOWN DATABASE [ IMMEDIATELY ]
 */
public class ShutdownDatabase extends AdminStatement {

    private final Database db;
    private final boolean immediately;

    public ShutdownDatabase(ServerSession session, Database db, boolean immediately) {
        super(session);
        this.db = db;
        this.immediately = immediately;
    }

    @Override
    public int getType() {
        return SQLStatement.SHUTDOWN_DATABASE;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "shutdown database");
        // 如果是LealoneDatabase什么都不做
        if (LealoneDatabase.isMe(db.getName()))
            return 0;
        DbObjectLock lock = LealoneDatabase.getInstance().tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;
        db.markClosed();
        if (immediately) {
            db.shutdownImmediately();
        } else if (db.getSessionCount() == 0) {
            db.closeIfNeeded();
        }
        return 0;
    }
}
