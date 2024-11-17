/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataType;

public class RowPage extends RowStorageLeafPage {

    // 内存占用32+48+56=136字节
    public static final int PAGE_MEMORY = 136;

    public RowPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    protected int getPageType() {
        return 1;
    }

    @Override
    protected int getEmptyPageMemory() {
        return PAGE_MEMORY;
    }

    @Override
    protected Object[] getValues() {
        return keys;
    }

    @Override
    public Object getSplitKey(int index) {
        return map.getKeyType().getSplitKey(getKey(index));
    }

    @Override
    protected int getKeyMemory(Object old) {
        // 也用值的类型来计算
        return map.getValueType().getMemory(old);
    }

    @Override
    protected void readValues(ByteBuffer buff, int keyLength) {
        map.getValueType().read(buff, keys, keyLength);
        setPageListener(map.getValueType(), keys);
    }

    @Override
    protected boolean writeValues(DataBuffer buff, int keyLength) {
        StorageDataType type = map.getValueType();
        boolean isLockable = type.isLockable();
        boolean isLocked = false;
        for (int i = 0; i < keyLength; i++) {
            type.write(buff, keys[i]);
            if (isLockable && !isLocked)
                isLocked = isLocked(keys[i]);
        }
        return isLocked;
    }
}
