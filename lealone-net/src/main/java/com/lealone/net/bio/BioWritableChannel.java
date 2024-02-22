/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetBufferFactory;
import com.lealone.net.WritableChannel;

public class BioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final int maxPacketSize;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private DataBuffer dataBuffer;

    public BioWritableChannel(Map<String, String> config, Socket socket, InetSocketAddress address)
            throws IOException {
        host = address.getHostString();
        port = address.getPort();
        maxPacketSize = getMaxPacketSize(config);
        this.socket = socket;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
    }

    public DataBuffer getDataBuffer(int capacity) {
        if (dataBuffer == null) {
            dataBuffer = DataBuffer.create(null, capacity, false);
        } else if (dataBuffer.capacity() > 8096)
            dataBuffer = DataBuffer.create(null, 8096, false);
        dataBuffer.clear();
        return dataBuffer;
    }

    @Override
    public void write(NetBuffer data) {
        ByteBuffer bb = data.getByteBuffer();
        try {
            if (bb.hasArray()) {
                out.write(bb.array(), bb.arrayOffset(), bb.limit());
            } else {
                byte[] bytes = new byte[bb.limit()];
                bb.get(bytes);
                out.write(bytes);
            }
            out.flush();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable t) {
            }
            socket = null;
            in = null;
            out = null;
        }
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NetBufferFactory.getInstance();
    }

    @Override
    public boolean isBio() {
        return true;
    }

    @Override
    public void read(AsyncConnection conn) {
        try {
            if (conn.isClosed())
                return;
            int packetLength = in.readInt();
            checkPacketLength(maxPacketSize, packetLength);
            DataBuffer dataBuffer = getDataBuffer(packetLength);
            // 返回的DatBuffer的Capacity可能大于packetLength，所以设置一下limit，不会多读
            dataBuffer.limit(packetLength);
            ByteBuffer buffer = dataBuffer.getBuffer();
            in.read(buffer.array(), buffer.arrayOffset(), packetLength);
            NetBuffer netBuffer = new NetBuffer(dataBuffer, true);
            conn.handle(netBuffer);
        } catch (Exception e) {
            conn.handleException(e);
        }
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return new BioDataBufferFactory(this);
    }

    public static int getMaxPacketSize(Map<String, String> config) {
        return MapUtils.getInt(config, ConnectionSetting.MAX_PACKET_SIZE.name(), 16 * 1024 * 1024);
    }

    public static void checkPacketLength(int maxPacketSize, int packetLength) throws IOException {
        if (packetLength > maxPacketSize)
            throw new IOException(
                    "Packet too large, maxPacketSize: " + maxPacketSize + ", receive: " + packetLength);
    }
}
