/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.lealone.db.DataBuffer;
import com.lealone.storage.type.StorageDataTypeBase;

/**
 * Implementation of the BOOLEAN data type.
 */
public class ValueBoolean extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 1;

    /**
     * The maximum display size of a boolean.
     * Example: FALSE
     */
    public static final int DISPLAY_SIZE = 5;

    /**
     * Of type Object so that Tomcat doesn't set it to null.
     */
    public static final ValueBoolean TRUE = new ValueBoolean(true);
    public static final ValueBoolean FALSE = new ValueBoolean(false);

    private final boolean value;

    private ValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.BOOLEAN;
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public Value negate() {
        return value ? FALSE : TRUE;
    }

    @Override
    public boolean getBoolean() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        boolean v2 = ((ValueBoolean) o).value;
        boolean v = value;
        return (v == v2) ? 0 : (v ? 1 : -1);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return value ? 1 : 0;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBoolean(parameterIndex, value);
    }

    /**
     * Get the boolean value for the given boolean.
     *
     * @param b the boolean
     * @return the value
     */
    public static ValueBoolean get(boolean b) {
        return b ? TRUE : FALSE;
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        // there are only ever two instances, so the instance must match
        return this == other;
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return TYPE_BOOLEAN;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Boolean a = (Boolean) aObj;
            Boolean b = (Boolean) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 16;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            write0(buff, ((Boolean) obj).booleanValue());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            write0(buff, v.getBoolean());
        }

        private void write0(DataBuffer buff, boolean v) {
            buff.put((byte) (v ? TAG_BOOLEAN_TRUE : TYPE_BOOLEAN));
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            if (tag == TYPE_BOOLEAN)
                return ValueBoolean.get(false);
            else
                return ValueBoolean.get(true);
        }
    };
}
