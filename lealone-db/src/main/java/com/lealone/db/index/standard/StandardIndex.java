/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.db.index.IndexBase;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.StandardTable;

public abstract class StandardIndex extends IndexBase {

    protected StandardIndex(StandardTable table, int id, String name, IndexType indexType,
            IndexColumn[] indexColumns) {
        super(table, id, name, indexType, indexColumns);
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }
}
