/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.value.Value;

// 最普通的查询
class QFlat extends QOperator {

    QFlat(Select select) {
        super(select);
    }

    @Override
    public void run() {
        while (next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !tryLockRow()) {
                    return; // 锁记录失败
                }
                Value[] row = createRow();
                result.addRow(row);
                rowCount++;
                if (canBreakLoop()) {
                    break;
                }
            }
            if (yield)
                return;
        }
        loopEnd = true;
    }
}
