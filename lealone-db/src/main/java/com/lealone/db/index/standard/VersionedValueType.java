/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;
import com.lealone.db.table.Column.EnumColumn;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;

public class VersionedValueType extends ValueDataType {

    final int columnCount;
    final EnumColumn[] enumColumns;

    public VersionedValueType(DataHandler handler, CompareMode compareMode, int[] sortTypes,
            int columnCount) {
        this(handler, compareMode, sortTypes, columnCount, null);
    }

    public VersionedValueType(DataHandler handler, CompareMode compareMode, int[] sortTypes,
            int columnCount, EnumColumn[] enumColumns) {
        super(handler, compareMode, sortTypes);
        this.columnCount = columnCount;
        this.enumColumns = enumColumns;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        if (aObj == null) {
            return -1;
        } else if (bObj == null) {
            return 1;
        }
        VersionedValue a = (VersionedValue) aObj;
        VersionedValue b = (VersionedValue) bObj;
        long comp = a.version - b.version;
        if (comp == 0) {
            return compareValues(a.columns, b.columns);
        }
        return Long.signum(comp);
    }

    @Override
    public int getMemory(Object obj) {
        VersionedValue v = (VersionedValue) obj;
        int memory = 4 + 4;
        if (v == null)
            return memory;
        Value[] columns = v.columns;
        for (int i = 0, len = columns.length; i < len; i++) {
            Value c = columns[i];
            if (c == null)
                memory += 4;
            else
                memory += c.getMemory();
        }
        return memory;
    }

    @Override
    public Object read(ByteBuffer buff) {
        int vertion = DataUtils.readVarInt(buff);
        ValueArray a = (ValueArray) DataBuffer.readValue(buff);
        if (enumColumns != null)
            setEnumColumns(a);
        return new VersionedValue(vertion, a.getList());
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.version);
        buff.writeValue(ValueArray.get(v.columns));
    }

    @Override
    public void writeMeta(DataBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.version);
    }

    @Override
    public Object readMeta(ByteBuffer buff, int columnCount) {
        int vertion = DataUtils.readVarInt(buff);
        Value[] columns = new Value[columnCount];
        return new VersionedValue(vertion, columns);
    }

    @Override
    public void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length)
            buff.writeValue(columns[columnIndex]);
    }

    @Override
    public void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length) {
            Value value = DataBuffer.readValue(buff);
            columns[columnIndex] = value;
            if (enumColumns != null)
                setEnumColumn(value, columnIndex);
        }
    }

    @Override
    public void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        if (columnIndexes != null) {
            VersionedValue oldValue = (VersionedValue) oldObj;
            VersionedValue newValue = (VersionedValue) newObj;
            Value[] oldColumns = oldValue.columns;
            Value[] newColumns = newValue.columns;
            for (int i : columnIndexes) {
                oldColumns[i] = newColumns[i];
            }
        }
    }

    @Override
    public ValueArray getColumns(Object obj) {
        return ValueArray.get(((VersionedValue) obj).columns);
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getMemory(Object obj, int columnIndex) {
        VersionedValue v = (VersionedValue) obj;
        Value[] columns = v.columns;
        if (columnIndex >= 0 && columnIndex < columns.length) {
            return columns[columnIndex].getMemory();
        } else {
            return 0;
        }
    }

    private void setEnumColumn(Value value, int columnIndex) {
        if (enumColumns[columnIndex] != null)
            enumColumns[columnIndex].setLabel(value);
    }

    private void setEnumColumns(ValueArray a) {
        for (int i = 0, len = a.getList().length; i < len; i++) {
            setEnumColumn(a.getValue(i), i);
        }
    }
}
