/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.MathUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.api.ErrorCode;

/**
 * Implementation of the SMALLINT data type.
 */
public class ValueShort extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 5;

    /**
     * The maximum display size of a short.
     * Example: -32768
     */
    static final int DISPLAY_SIZE = 6;

    private final short value;

    private ValueShort(short value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value + other.value);
    }

    private static ValueShort checkRange(int x) {
        if (x < Short.MIN_VALUE || x > Short.MAX_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Integer.toString(x));
        }
        return ValueShort.get((short) x);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(int) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value - other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value * other.value);
    }

    @Override
    public Value divide(Value v) {
        ValueShort other = (ValueShort) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueShort.get((short) (value / other.value));
    }

    @Override
    public Value modulus(Value v) {
        ValueShort other = (ValueShort) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueShort.get((short) (value % other.value));
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getType() {
        return Value.SHORT;
    }

    @Override
    public short getShort() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueShort v = (ValueShort) o;
        return MathUtils.compareInt(value, v.value);
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public Object getObject() {
        return Short.valueOf(value);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setShort(parameterIndex, value);
    }

    /**
     * Get or create a short value for the given short.
     *
     * @param i the short
     * @return the value
     */
    public static ValueShort get(short i) {
        return (ValueShort) Value.cache(new ValueShort(i));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueShort && value == ((ValueShort) other).value;
    }

    @Override
    public int getMemory() {
        return 16;
    }

    public static final ValueDataTypeBase type = new ValueDataTypeBase() {

        @Override
        public int getType() {
            return SHORT;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Short a = (Short) aObj;
            Short b = (Short) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 16;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            buff.put((byte) Value.SHORT).putShort(((Short) obj).shortValue());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            buff.put((byte) Value.SHORT).putShort(v.getShort());
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return ValueShort.get(buff.getShort());
        }
    };
}
