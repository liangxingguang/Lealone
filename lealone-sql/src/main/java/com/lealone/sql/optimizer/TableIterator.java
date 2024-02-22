/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.optimizer;

import com.lealone.db.index.Cursor;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;

public class TableIterator {

    private final ServerSession session;
    private final TableFilter tableFilter;
    private final Table table;
    private Row oldRow;
    private Cursor cursor;

    public TableIterator(ServerSession session, TableFilter tableFilter) {
        this.session = session;
        this.tableFilter = tableFilter;
        this.table = tableFilter.getTable();
    }

    public void setCursor(Cursor cursor) {
        this.cursor = cursor;
    }

    public void start() {
        tableFilter.startQuery(session);
        reset();
    }

    public void reset() {
        tableFilter.reset();
    }

    public boolean next() {
        if (oldRow != null) {
            Row r = oldRow;
            oldRow = null;
            // 当发生行锁时可以直接用tableFilter的当前值重试
            if (tableFilter.rebuildSearchRow(session, r) != null)
                return true;
        }
        if (cursor == null) {
            return tableFilter.next();
        } else {
            return cursor.next();
        }
    }

    public Row getRow() {
        if (cursor == null) {
            return tableFilter.get();
        } else {
            SearchRow found = cursor.getSearchRow();
            return tableFilter.getTable().getRow(session, found.getKey());
        }
    }

    public int tryLockRow(int[] lockColumns) {
        Row oldRow = getRow();
        if (oldRow == null) { // 已经删除了
            return -1;
        }
        int ret = table.tryLockRow(session, oldRow, lockColumns);
        if (ret < 0) { // 已经删除了
            return -1;
        } else if (ret == 0) { // 被其他事务锁住了
            this.oldRow = oldRow;
            return 0;
        }
        if (table.isRowChanged(oldRow)) {
            this.oldRow = oldRow;
            return -1;
        }
        return 1;
    }

    public void onLockedException() {
        oldRow = getRow();
    }
}
