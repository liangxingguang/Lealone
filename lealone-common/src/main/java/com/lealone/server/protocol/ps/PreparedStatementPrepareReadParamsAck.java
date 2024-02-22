/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.ps;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.CommandParameter;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.value.Value;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class PreparedStatementPrepareReadParamsAck implements AckPacket {

    public final boolean isQuery;
    public final List<? extends CommandParameter> params;

    public PreparedStatementPrepareReadParamsAck(boolean isQuery,
            List<? extends CommandParameter> params) {
        this.isQuery = isQuery;
        this.params = params;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_PREPARE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBoolean(isQuery);
        out.writeInt(params.size());
        for (CommandParameter p : params) {
            writeParameterMetaData(out, p);
        }
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param p the parameter
     */
    private static void writeParameterMetaData(NetOutputStream out, CommandParameter p)
            throws IOException {
        out.writeInt(p.getType());
        out.writeLong(p.getPrecision());
        out.writeInt(p.getScale());
        out.writeInt(p.getNullable());
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementPrepareReadParamsAck> {
        @Override
        public PreparedStatementPrepareReadParamsAck decode(NetInputStream in, int version)
                throws IOException {
            boolean isQuery = in.readBoolean();
            int paramCount = in.readInt();
            ArrayList<CommandParameter> params = new ArrayList<>(paramCount);
            for (int i = 0; i < paramCount; i++) {
                ClientCommandParameter p = new ClientCommandParameter(i);
                p.readMetaData(in);
                params.add(p);
            }
            return new PreparedStatementPrepareReadParamsAck(isQuery, params);
        }
    }

    /**
     * A client side parameter.
     */
    private static class ClientCommandParameter implements CommandParameter {

        private final int index;
        private Value value;
        private int dataType = Value.UNKNOWN;
        private long precision;
        private int scale;
        private int nullable = ResultSetMetaData.columnNullableUnknown;

        public ClientCommandParameter(int index) {
            this.index = index;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void setValue(Value newValue, boolean closeOld) {
            if (closeOld && value != null) {
                value.close();
            }
            value = newValue;
        }

        @Override
        public void setValue(Value value) {
            this.value = value;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public void checkSet() {
            if (value == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
            }
        }

        @Override
        public boolean isValueSet() {
            return value != null;
        }

        @Override
        public int getType() {
            return value == null ? dataType : value.getType();
        }

        @Override
        public long getPrecision() {
            return value == null ? precision : value.getPrecision();
        }

        @Override
        public int getScale() {
            return value == null ? scale : value.getScale();
        }

        @Override
        public int getNullable() {
            return nullable;
        }

        /**
         * Read the parameter meta data from the out object.
         *
         * @param in the NetInputStream
         */
        public void readMetaData(NetInputStream in) throws IOException {
            dataType = in.readInt();
            precision = in.readLong();
            scale = in.readInt();
            nullable = in.readInt();
        }
    }
}
