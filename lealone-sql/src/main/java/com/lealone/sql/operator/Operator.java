/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.operator;

import com.lealone.db.result.LocalResult;

public interface Operator {

    void start();

    void run();

    void stop();

    boolean isStopped();

    LocalResult getLocalResult();

    default void copyStatus(Operator old) {
    }

    default void onLockedException() {
    }
}
