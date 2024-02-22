/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.lealone.db.DataBufferFactory;
import com.lealone.db.scheduler.Scheduler;

public interface NetEventLoop {

    Scheduler getScheduler();

    void setScheduler(Scheduler scheduler);

    void setPreferBatchWrite(boolean preferBatchWrite);

    DataBufferFactory getDataBufferFactory();

    Selector getSelector();

    void select() throws IOException;

    void select(long timeout) throws IOException;

    void register(AsyncConnection conn);

    void wakeup();

    void addSocketChannel(SocketChannel channel);

    void addNetBuffer(SocketChannel channel, NetBuffer netBuffer);

    void read(SelectionKey key);

    void write();

    void write(SelectionKey key);

    void setNetClient(NetClient netClient);

    NetClient getNetClient();

    void handleSelectedKeys();

    void closeChannel(SocketChannel channel);

    void close();

    boolean isInLoop();

    boolean isQueueLarge();
}
