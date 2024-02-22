/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.session;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.lealone.client.command.ClientPreparedSQLCommand;
import org.lealone.client.command.ClientSQLCommand;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.DbSetting;
import org.lealone.db.LocalDataHandler;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.ConcurrentAsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.async.SingleThreadAsyncCallback;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionBase;
import org.lealone.net.NetInputStream;
import org.lealone.net.TcpClientConnection;
import org.lealone.net.TransferOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketDecoders;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.lob.LobRead;
import org.lealone.server.protocol.lob.LobReadAck;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.lob.LobLocalStorage;

/**
 * The client side part of a session when using the server mode. 
 * This object communicates with a session on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
// 一个ClientSession对应一条JdbcConnection，多个ClientSession共用一个TcpClientConnection。
// 同JdbcConnection一样，每个ClientSession对象也不是线程安全的，只能在单线程中使用。
// 另外，每个ClientSession只对应一个server，
// 虽然ConnectionInfo允许在JDBC URL中指定多个server，但是放在ClientSessionFactory中处理了。
public class ClientSession extends SessionBase implements LobLocalStorage.LobReader {

    private final TcpClientConnection tcpConnection;
    private final ConnectionInfo ci;
    private final String server;
    private final Session parent;
    private final int id;
    private final LocalDataHandler dataHandler;
    private final Trace trace;

    ClientSession(TcpClientConnection tcpConnection, ConnectionInfo ci, String server, Session parent,
            int id) {
        this.tcpConnection = tcpConnection;
        this.ci = ci;
        this.server = server;
        this.parent = parent;
        this.id = id;

        String cipher = ci.getProperty(DbSetting.CIPHER.getName());
        dataHandler = new LocalDataHandler(cipher);
        dataHandler.setLobReader(this);

        initTraceSystem(ci);
        trace = traceSystem == null ? Trace.NO_TRACE : traceSystem.getTrace(TraceModuleType.JDBC);
    }

    @Override
    public String toString() {
        return "ClientSession[" + id + ", " + server + "]";
    }

    @Override
    public int getId() {
        return id;
    }

    public int getNextId() {
        checkClosed();
        return tcpConnection.getNextId();
    }

    public int getCurrentId() {
        return tcpConnection.getCurrentId();
    }

    InetSocketAddress getInetSocketAddress() {
        return tcpConnection.getInetSocketAddress();
    }

    @Override
    public void checkClosed() {
        if (tcpConnection.isClosed()) {
            String msg = tcpConnection.getWritableChannel().getHost() + " tcp connection closed";
            throw getConnectionBrokenException(msg);
        }
        if (isClosed()) {
            throw getConnectionBrokenException("session closed");
        }
    }

    private DbException getConnectionBrokenException(String msg) {
        return DbException.get(ErrorCode.CONNECTION_BROKEN_1, tcpConnection.getPendingException(), msg);
    }

    @Override
    public void cancel() {
        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     *
     * @param statementId the statement id
     */
    @Override
    public void cancelStatement(int statementId) {
        try {
            send(new SessionCancelStatement(statementId));
        } catch (Exception e) {
            trace.debug(e, "could not cancel statement");
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        if (this.autoCommit == autoCommit)
            return;
        setAutoCommitSend(autoCommit);
        this.autoCommit = autoCommit;
    }

    private void setAutoCommitSend(boolean autoCommit) {
        try {
            traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
            send(new SessionSetAutoCommit(autoCommit));
        } catch (Exception e) {
            handleException(e);
        }
    }

    public void handleException(Throwable e) {
        checkClosed();
        if (e instanceof DbException)
            throw (DbException) e;
        throw DbException.convert(e);
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize, boolean prepared) {
        checkClosed();
        if (prepared)
            return new ClientPreparedSQLCommand(this, sql, fetchSize);
        else
            return new ClientSQLCommand(this, sql, fetchSize);
    }

    @Override
    public void close() {
        if (closed)
            return;
        try {
            RuntimeException closeError = null;
            try {
                // 只有当前Session有效时服务器端才持有对应的session
                if (isValid()) {
                    send(new SessionClose());
                    tcpConnection.removeSession(id);
                }
            } catch (RuntimeException e) {
                trace.error(e, "close");
                closeError = e;
            } catch (Exception e) {
                trace.error(e, "close");
            }
            closeTraceSystem();
            if (closeError != null) {
                throw DbException.convert(closeError);
            }
        } finally {
            super.close();
        }
    }

    public Trace getTrace() {
        return trace;
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     *
     * @param operation the operation performed
     * @param id the id of the operation
     */
    public void traceOperation(String operation, int id) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} {1}", operation, id);
        }
    }

    @Override
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    @Override
    public synchronized int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off,
            int length) {
        try {
            AsyncCallback<Integer> ac = createCallback();
            this.<LobReadAck> send(new LobRead(lobId, hmac, offset, length)).onComplete(ar -> {
                if (ar.isSucceeded()) {
                    LobReadAck ack = ar.getResult();
                    if (ack.buff != null && ack.buff.length > 0) {
                        System.arraycopy(ack.buff, 0, buff, off, ack.buff.length);
                        ac.setAsyncResult(ack.buff.length);
                    } else {
                        ac.setAsyncResult(-1);
                    }
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
            });
            return ac.get();
        } catch (Exception e) {
            handleException(e);
        }
        return -1;
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        parent.runModeChanged(newTargetNodes);
    }

    @Override
    public String getURL() {
        return ci.getURL();
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return ci;
    }

    @Override
    public void setNetworkTimeout(int milliseconds) {
        ci.setNetworkTimeout(milliseconds);
    }

    @Override
    public int getNetworkTimeout() {
        return ci.getNetworkTimeout();
    }

    @Override
    public String getLocalHostAndPort() {
        try {
            SocketAddress sa = tcpConnection.getWritableChannel().getSocketChannel().getLocalAddress();
            String host;
            int port;
            if (sa instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) sa;
                host = address.getHostString();
                port = address.getPort();
            } else {
                host = InetAddress.getLocalHost().getHostAddress();
                port = 0;
            }
            return host + ":" + port;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet,
            AckPacketHandler<R, P> ackPacketHandler) {
        int packetId = getNextId();
        return send(packet, packetId, ackPacketHandler);
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        traceOperation(packet.getType().name(), packetId);
        AsyncCallback<R> ac;
        if (packet.getAckType() != PacketType.VOID) {
            if (isSingleThreadCallback()) {
                ac = new SingleThreadAsyncCallback<R>() {
                    @Override
                    public void runInternal(NetInputStream in) throws Exception {
                        handleAsyncCallback(in, packet.getAckType(), ackPacketHandler, this);
                    }
                };
            } else {
                ac = new ConcurrentAsyncCallback<R>() {
                    @Override
                    public void runInternal(NetInputStream in) throws Exception {
                        handleAsyncCallback(in, packet.getAckType(), ackPacketHandler, this);
                    }
                };
            }
            ac.setPacket(packet);
            ac.setStartTime(System.currentTimeMillis());
            ac.setNetworkTimeout(getNetworkTimeout());
            tcpConnection.addAsyncCallback(packetId, ac);
        } else {
            ac = null;
        }
        try {
            checkClosed();
            TransferOutputStream out = tcpConnection.createTransferOutputStream(this);
            out.writeRequestHeader(packetId, packet.getType());
            packet.encode(out, getProtocolVersion());
            out.flush();
            if (ac != null && tcpConnection.getWritableChannel().isBio())
                tcpConnection.getWritableChannel().read(tcpConnection);
        } catch (Throwable e) {
            if (ac != null) {
                removeAsyncCallback(packetId);
                ac.setAsyncResult(e);
            } else {
                handleException(e);
            }
        }
        return ac;
    }

    @SuppressWarnings("unchecked")
    private <R, P extends AckPacket> void handleAsyncCallback(NetInputStream in, PacketType packetType,
            AckPacketHandler<R, P> ackPacketHandler, AsyncCallback<R> ac) throws IOException {
        PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
        Packet packet = decoder.decode(in, getProtocolVersion());
        if (ackPacketHandler != null) {
            try {
                ac.setAsyncResult(ackPacketHandler.handle((P) packet));
            } catch (Throwable e) {
                ac.setAsyncResult(e);
            }
        }
    }

    public void removeAsyncCallback(int packetId) {
        tcpConnection.removeAsyncCallback(packetId);
    }

    private boolean singleThreadCallback;

    @Override
    public void setSingleThreadCallback(boolean singleThreadCallback) {
        this.singleThreadCallback = singleThreadCallback;
    }

    @Override
    public boolean isSingleThreadCallback() {
        return singleThreadCallback;
    }

    @Override
    public <T> AsyncCallback<T> createCallback() {
        return AsyncCallback.create(singleThreadCallback);
    }
}
