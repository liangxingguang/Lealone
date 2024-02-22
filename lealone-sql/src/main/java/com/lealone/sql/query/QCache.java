/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.result.LocalResult;

// 直接基于缓存中的结果集
class QCache extends QOperator {

    QCache(Select select, LocalResult result) {
        super(select);
        this.result = result;
        localResult = result;
    }

    @Override
    public void start() {
        // 什么都不做
    }

    @Override
    public void run() {
        loopEnd = true;
    }

    @Override
    public void stop() {
        // 忽略limit和offset
        handleLocalResult();
    }
}
