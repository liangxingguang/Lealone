/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.scheduler.SchedulerBase;
import com.lealone.server.ProtocolServer;

public abstract class NetScheduler extends SchedulerBase {

    protected final NetEventLoop netEventLoop;

    public NetScheduler(int id, String name, int schedulerCount, Map<String, String> config,
            boolean isThreadSafe) {
        super(id, name, schedulerCount, config);
        netEventLoop = NetFactory.getFactory(config).createNetEventLoop(loopInterval, isThreadSafe);
        netEventLoop.setScheduler(this);
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return netEventLoop.getDataBufferFactory();
    }

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    public Selector getSelector() {
        return netEventLoop.getSelector();
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
        DbException.throwInternalError();
    }

    // --------------------- 网络事件循环 ---------------------

    @Override
    public void wakeUp() {
        netEventLoop.wakeup();
    }

    @Override
    protected void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            netEventLoop.handleSelectedKeys();
        } catch (Throwable t) {
            getLogger().warn("Failed to runEventLoop", t);
        }
    }

    @Override
    protected void onStopped() {
        super.onStopped();
        netEventLoop.close();
    }
}
