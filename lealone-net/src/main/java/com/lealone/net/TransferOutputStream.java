/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.Session;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueList;
import com.lealone.db.value.ValueLob;
import com.lealone.db.value.ValueMap;
import com.lealone.db.value.ValueResultSet;
import com.lealone.db.value.ValueSet;
import com.lealone.db.value.ValueTime;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.db.value.ValueUuid;
import com.lealone.server.protocol.PacketType;

/**
 * The transfer class is used to send Value objects.
 * It is used on both the client side, and on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TransferOutputStream implements NetOutputStream {

    private static final int BUFFER_SIZE = 4 * 1024;
    static final int LOB_MAGIC = 0x1234;

    public static final byte REQUEST = 1;
    public static final byte RESPONSE = 2;

    private final Session session;
    private final DataOutputStream out;
    private final ResettableBufferOutputStream resettableOutputStream;

    public TransferOutputStream(Session session, WritableChannel writableChannel,
            DataBufferFactory dataBufferFactory) {
        this.session = session;
        resettableOutputStream = new ResettableBufferOutputStream(writableChannel, BUFFER_SIZE,
                dataBufferFactory);
        out = new DataOutputStream(resettableOutputStream);
    }

    public int getDataOutputStreamSize() {
        return out.size();
    }

    public void setPayloadSize(int payloadStartPos, int size) {
        resettableOutputStream.setPayloadSize(payloadStartPos, size);
    }

    public TransferOutputStream writeRequestHeader(int packetId, int packetType) throws IOException {
        writeByte(REQUEST).writeInt(packetId).writeInt(packetType).writeInt(session.getId());
        return this;
    }

    public TransferOutputStream writeRequestHeader(int packetId, PacketType packetType)
            throws IOException {
        writeByte(REQUEST).writeInt(packetId).writeInt(packetType.value).writeInt(session.getId());
        return this;
    }

    public TransferOutputStream writeRequestHeaderWithoutSessionId(int packetId, int packetType)
            throws IOException {
        writeByte(REQUEST).writeInt(packetId).writeInt(packetType);
        return this;
    }

    public TransferOutputStream writeResponseHeader(int packetId, int status) throws IOException {
        writeByte(RESPONSE).writeInt(packetId).writeInt(status);
        return this;
    }

    public DataOutputStream getDataOutputStream() {
        return out;
    }

    /**
     * 当输出流写到一半时碰到某种异常了(可能是内部代码实现bug)，比如产生了NPE，
     * 就会转到错误处理，生成一个新的错误协议包，但是前面产生的不完整的内容没有正常结束，
     * 这会导致客户端无法正常解析数据，所以这里允许在生成错误协议包之前清除之前的内容，
     * 如果之前的协议包不完整，但是已经发出去一半了，这里的方案也无能为力。 
     */
    public void reset() throws IOException {
        resettableOutputStream.reset();
    }

    public Session getSession() {
        return session;
    }

    @Override
    public void setSSL(boolean ssl) {
        // this.ssl = ssl;
    }

    /**
     * Write pending changes.
     */
    public void flush() throws IOException {
        if (session != null) // 一些场景允许为null
            session.checkClosed();
        resettableOutputStream.flush();
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeBoolean(boolean x) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     */
    private TransferOutputStream writeByte(byte x) throws IOException {
        out.writeByte(x);
        return this;
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeInt(int x) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeLong(long x) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Write a double.
     *
     * @param i the value
     * @return itself
     */
    private TransferOutputStream writeDouble(double i) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param i the value
     * @return itself
     */
    private TransferOutputStream writeFloat(float i) throws IOException {
        out.writeFloat(i);
        return this;
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeString(String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            int len = s.length();
            out.writeInt(len);
            for (int i = 0; i < len; i++) {
                out.writeChar(s.charAt(i));
            }
        }
        return this;
    }

    /**
     * Write a byte buffer.
     *
     * @param data the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeByteBuffer(ByteBuffer data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            if (data.hasArray()) {
                writeBytes(data.array(), data.arrayOffset(), data.limit());
            } else {
                byte[] bytes = new byte[data.limit()];
                data.get(bytes);
                writeBytes(bytes);
            }
        }
        return this;
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     */
    @Override
    public TransferOutputStream writeBytes(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            out.write(data);
        }
        return this;
    }

    /**
     * Write a number of bytes.
     *
     * @param buff the value
     * @param off the offset
     * @param len the length
     * @return itself
     */
    public TransferOutputStream writeBytes(byte[] buff, int off, int len) throws IOException {
        writeInt(len);
        out.write(buff, off, len);
        return this;
    }

    /**
     * Write a value.
     *
     * @param v the value
     */
    @Override
    public void writeValue(Value v) throws IOException {
        int type = v.getType();
        writeInt(type);
        switch (type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            writeBytes(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh());
            writeLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            writeBoolean(v.getBoolean());
            break;
        case Value.BYTE:
            writeByte(v.getByte());
            break;
        case Value.DATE:
            writeLong(((ValueDate) v).getDateValue());
            break;
        case Value.TIME:
            writeLong(((ValueTime) v).getNanos());
            break;
        case Value.TIMESTAMP: {
            ValueTimestamp ts = (ValueTimestamp) v;
            writeLong(ts.getDateValue());
            writeLong(ts.getNanos());
            break;
        }
        case Value.DECIMAL:
            writeString(v.getString());
            break;
        case Value.DOUBLE:
            writeDouble(v.getDouble());
            break;
        case Value.FLOAT:
            writeFloat(v.getFloat());
            break;
        case Value.SHORT:
            writeInt(v.getShort());
            break;
        case Value.INT:
            writeInt(v.getInt());
            break;
        case Value.LONG:
            writeLong(v.getLong());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            writeString(v.getString());
            break;
        case Value.BLOB:
        case Value.CLOB: {
            ValueLob lob = (ValueLob) v;
            if (lob.isStored()) {
                writeLong(-1);
                writeInt(lob.getTableId());
                writeLong(lob.getLobId());
                writeBytes(calculateLobMac(session, lob));
                writeLong(lob.getPrecision());
                break;
            }
            long length = v.getPrecision();
            if (length < 0) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length);
            if (type == Value.BLOB) {
                long written = IOUtils.copyAndCloseInput(v.getInputStream(), out);
                if (written != length) {
                    throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                            "length:" + length + " written:" + written);
                }
            } else {
                Reader reader = v.getReader();
                DataBuffer.copyString(reader, out);
            }
            writeInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            Class<?> componentType = va.getComponentType();
            if (componentType == Object.class) {
                writeInt(len);
            } else {
                writeInt(-(len + 1));
                writeString(componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.RESULT_SET: {
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                writeInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    writeString(meta.getColumnName(i + 1));
                    writeInt(meta.getColumnType(i + 1));
                    writeInt(meta.getPrecision(i + 1));
                    writeInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    writeBoolean(true);
                    for (int i = 0; i < columnCount; i++) {
                        int t = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                        Value val = DataType.readValue(session, rs, i + 1, t);
                        writeValue(val);
                    }
                }
                writeBoolean(false);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
            break;
        }
        case Value.SET: {
            ValueSet vs = (ValueSet) v;
            Set<Value> set = vs.getSet();
            int size = set.size();
            Class<?> componentType = vs.getComponentType();
            if (componentType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(componentType.getName());
            }
            for (Value value : set) {
                writeValue(value);
            }
            break;
        }
        case Value.LIST: {
            ValueList vl = (ValueList) v;
            List<Value> list = vl.getList();
            int size = list.size();
            Class<?> componentType = vl.getComponentType();
            if (componentType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(componentType.getName());
            }
            for (Value value : list) {
                writeValue(value);
            }
            break;
        }
        case Value.MAP: {
            ValueMap vm = (ValueMap) v;
            Map<Value, Value> map = vm.getMap();
            int size = map.size();
            Class<?> kType = vm.getKeyType();
            Class<?> vType = vm.getValueType();
            if (kType == Object.class && vType == Object.class) {
                writeInt(size);
            } else {
                writeInt(-(size + 1));
                writeString(kType.getName());
                writeString(vType.getName());
            }
            for (Entry<Value, Value> e : map.entrySet()) {
                writeValue(e.getKey());
                writeValue(e.getValue());
            }
            break;
        }
        case Value.ENUM: {
            writeString(v.getString());
            writeInt(v.getInt());
            break;
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Verify the HMAC.
     *
     * @param hmacData the message authentication code
     * @param lobId the lobId
     * @throws DbException if the HMAC does not match
     */
    public static int verifyLobMac(Session session, byte[] hmacData, long lobId) {
        long hmac = Utils.readLong(hmacData, 0);
        if ((lobId >> 32) != ((int) hmac)) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "Invalid lob hmac; possibly the connection was re-opened internally");
        }
        return (int) (hmac >> 32);
    }

    private static byte[] calculateLobMac(Session session, ValueLob lob) {
        int tableId = lob.getTableId();
        long lobId = lob.getLobId();
        if (lob.isUseTableLobStorage())
            tableId = -tableId;
        long hmac = (lobId >> 32) + (((long) tableId) << 32);
        byte[] hmacData = new byte[8];
        Utils.writeLong(hmacData, 0, hmac);
        return hmacData;
    }

    private static class ResettableBufferOutputStream extends NetBufferOutputStream {

        ResettableBufferOutputStream(WritableChannel writableChannel, int initialSizeHint,
                DataBufferFactory dataBufferFactory) {
            super(writableChannel, initialSizeHint, dataBufferFactory);
        }

        @Override
        public void flush() throws IOException {
            writePacketLength();
            buffer.flip();
            writableChannel.write(buffer);
        }

        @Override
        protected void reset() {
            super.reset();
            // 协议包头占4个字节，最后flush时再回填
            buffer.appendInt(0);
        }

        // 按java.io.DataInputStream.readInt()的格式写
        private void writePacketLength() {
            int v = buffer.length() - 4;
            buffer.setByte(0, (byte) ((v >>> 24) & 0xFF));
            buffer.setByte(1, (byte) ((v >>> 16) & 0xFF));
            buffer.setByte(2, (byte) ((v >>> 8) & 0xFF));
            buffer.setByte(3, (byte) (v & 0xFF));
        }

        public void setPayloadSize(int payloadStartPos, int size) {
            payloadStartPos += 4;
            int v = size;
            buffer.setByte(payloadStartPos, (byte) ((v >>> 24) & 0xFF));
            buffer.setByte(payloadStartPos + 1, (byte) ((v >>> 16) & 0xFF));
            buffer.setByte(payloadStartPos + 2, (byte) ((v >>> 8) & 0xFF));
            buffer.setByte(payloadStartPos + 3, (byte) (v & 0xFF));
        }
    }
}
