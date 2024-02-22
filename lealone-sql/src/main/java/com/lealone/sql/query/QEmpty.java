/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

// 比如limit子句为0时
class QEmpty extends QOperator {

    QEmpty(Select select) {
        super(select);
    }

    @Override
    public void run() {
        loopEnd = true;
    }
}
