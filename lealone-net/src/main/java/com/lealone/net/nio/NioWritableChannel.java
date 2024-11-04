/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.lealone.db.DataBufferFactory;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetEventLoop;
import com.lealone.net.WritableChannel;

public class NioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final String localHost;
    private final int localPort;

    private final DataBufferFactory dataBufferFactory;
    private final NetEventLoop eventLoop;
    private final SocketChannel channel;
    private SelectionKey selectionKey; // 注册成功了才设置
    private NetBuffer buffer;

    public NioWritableChannel(Scheduler scheduler, SocketChannel channel) throws IOException {
        this.dataBufferFactory = scheduler.getDataBufferFactory();
        this.eventLoop = (NetEventLoop) scheduler.getNetEventLoop();
        this.channel = channel;
        SocketAddress sa = channel.getRemoteAddress();
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) sa;
            host = address.getHostString();
            port = address.getPort();
        } else {
            host = "";
            port = -1;
        }
        sa = channel.getLocalAddress();
        if (sa instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) sa;
            localHost = address.getHostString();
            localPort = address.getPort();
        } else {
            localHost = "";
            localPort = -1;
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
    public String getLocalHost() {
        return localHost;
    }

    @Override
    public int getLocalPort() {
        return localPort;
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return dataBufferFactory;
    }

    @Override
    public NetBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void setBuffer(NetBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return channel;
    }

    @Override
    public NetEventLoop getEventLoop() {
        return eventLoop;
    }

    @Override
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    @Override
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    @Override
    public boolean isClosed() {
        return selectionKey == null;
    }

    @Override
    public void close() {
        eventLoop.closeChannel(this);
        buffer = null;
        selectionKey = null;
    }

    @Override
    public void read() {
        throw new UnsupportedOperationException("read");
    }

    @Override
    public void write(NetBuffer buffer) {
        eventLoop.write(this, buffer);
    }
}
