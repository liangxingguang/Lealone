/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

public enum PageStorageMode {
    // 定义的先后顺序不能随便改动，其他代码依赖 ordinal
    ROW_STORAGE,
    COLUMN_STORAGE;
}
