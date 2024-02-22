/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.SmallLRUCache;
import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLob;
import com.lealone.net.TransferOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.lob.LobRead;
import com.lealone.server.protocol.lob.LobReadAck;

public class LobPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.LOB_READ, new Read());
    }

    private static class Read implements PacketHandler<LobRead> {
        @Override
        public Packet handle(ServerSession session, LobRead packet) {
            long lobId = packet.lobId;
            byte[] hmac = packet.hmac;
            long offset = packet.offset;
            int length = packet.length;
            SmallLRUCache<String, InputStream> lobs = session.getLobCache();
            try {
                boolean useTableLobStorage = false;
                int tableId = TransferOutputStream.verifyLobMac(session, hmac, lobId);
                if (tableId < 0) {
                    tableId = -tableId;
                    useTableLobStorage = true;
                }
                String key = tableId + "_" + lobId;
                CachedInputStream cachedInputStream = (CachedInputStream) lobs.get(key);
                if (cachedInputStream == null) {
                    cachedInputStream = new CachedInputStream(null);
                    lobs.put(key, cachedInputStream);
                }
                if (cachedInputStream.getPos() != offset) {
                    DataHandler dh = useTableLobStorage ? session.getDatabase().getDataHandler(tableId)
                            : session.getDatabase();
                    // only the lob id is used
                    ValueLob lob = ValueLob.create(Value.BLOB, dh, tableId, lobId, hmac, -1);
                    lob.setUseTableLobStorage(useTableLobStorage);
                    InputStream lobIn = dh.getLobStorage().getInputStream(lob, hmac, -1);
                    cachedInputStream = new CachedInputStream(lobIn);
                    lobs.put(key, cachedInputStream);
                    lobIn.skip(offset);
                }
                // limit the buffer size
                length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
                byte[] buff = new byte[length];
                length = IOUtils.readFully(cachedInputStream, buff);
                if (length != buff.length) {
                    byte[] newBuff = new byte[length];
                    System.arraycopy(buff, 0, newBuff, 0, length);
                    buff = newBuff;
                }
                return new LobReadAck(buff);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
    }

    /**
    * An input stream with a position.
    */
    private static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY = new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }
    }
}
