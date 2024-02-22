/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.type;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;

public class CharacterType extends StorageDataTypeBase {

    @Override
    public int getType() {
        return TYPE_CHAR;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        Character a = (Character) aObj;
        Character b = (Character) bObj;
        return a.compareTo(b);
    }

    @Override
    public int getMemory(Object obj) {
        return 16;
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        buff.put((byte) TYPE_CHAR);
        buff.putChar(((Character) obj).charValue());
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        return Character.valueOf(buff.getChar());
    }
}
