/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.server.protocol.result;

import java.io.IOException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class ResultFetchRowsAck implements AckPacket {

    public final NetInputStream in;
    public final Result result;
    public final int count;

    public ResultFetchRowsAck(NetInputStream in) {
        this.in = in;
        this.result = null;
        this.count = 0;
    }

    public ResultFetchRowsAck(Result result, int count) {
        this.in = null;
        this.result = result;
        this.count = count;
    }

    @Override
    public PacketType getType() {
        return PacketType.RESULT_FETCH_ROWS_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        writeRow(out, result, count);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ResultFetchRowsAck> {
        @Override
        public ResultFetchRowsAck decode(NetInputStream in, int version) throws IOException {
            return new ResultFetchRowsAck(in);
        }
    }

    public static void writeRow(NetOutputStream out, Result result, int count) throws IOException {
        try {
            int visibleColumnCount = result.getVisibleColumnCount();
            for (int i = 0; i < count; i++) {
                if (result.next()) {
                    out.writeBoolean(true);
                    Value[] v = result.currentRow();
                    for (int j = 0; j < visibleColumnCount; j++) {
                        out.writeValue(v[j]);
                    }
                } else {
                    out.writeBoolean(false);
                    break;
                }
            }
        } catch (Throwable e) {
            // 如果取结果集的下一行记录时发生了异常，
            // 结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            out.writeBoolean(false);
            throw DbException.convert(e);
        }
    }
}
