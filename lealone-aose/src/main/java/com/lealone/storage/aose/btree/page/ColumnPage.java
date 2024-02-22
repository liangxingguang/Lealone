/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.type.StorageDataType;

public class ColumnPage extends Page {

    private final AtomicInteger memory = new AtomicInteger(0);
    private ByteBuffer buff;

    ColumnPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public int getMemory() {
        return memory.get();
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);

        readCheckValue(buff, chunkId, offset, pageLength);
        buff.get(); // page type;
        int compressType = buff.get();

        // 解压完之后就结束了，因为还不知道具体的行，所以延迟对列进行反序列化
        this.buff = expandPage(buff, compressType, start, pageLength);
    }

    // 在read方法中已经把buff读出来了，这里只是把字段从buff中解析出来
    void readColumn(Object[] values, int columnIndex) {
        int memory = 0;
        ByteBuffer buff = this.buff.slice(); // 要支持多线程同时读，所以直接用slice
        StorageDataType valueType = map.getValueType();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.readColumn(buff, values[row], columnIndex);
            memory += valueType.getMemory(values[row], columnIndex);
        }
        if (this.memory.compareAndSet(0, memory)) {
            // buff内存大小在getOrReadPage中加了，这里只加列占用的内存大小
            map.getBTreeStorage().getBTreeGC().addUsedMemory(memory);
        }
    }

    long write(Chunk chunk, DataBuffer buff, Object[] values, int columnIndex) {
        PageInfo pInfoOld = getRef().getPageInfo();
        beforeWrite(pInfoOld);
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_COLUMN;
        buff.putInt(0); // 回填pageLength

        StorageDataType valueType = map.getValueType();
        int checkPos = buff.position();
        buff.putShort((short) 0);
        buff.put((byte) type);
        int compressTypePos = buff.position();
        int compressType = 0;
        buff.put((byte) compressType); // 调用compressPage时会回填
        int compressStart = buff.position();
        for (int row = 0, rowCount = values.length; row < rowCount; row++) {
            valueType.writeColumn(buff, values[row], columnIndex);
        }
        compressPage(buff, compressStart, compressType, compressTypePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);
        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type);
    }
}
