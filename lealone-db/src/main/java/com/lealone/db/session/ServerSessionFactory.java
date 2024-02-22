/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Database;
import com.lealone.db.DbSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.Mode;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.auth.User;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerLock;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.net.NetNode;

public class ServerSessionFactory extends SessionFactoryBase {

    private static final ServerSessionFactory instance = new ServerSessionFactory();

    public static ServerSessionFactory getInstance() {
        return instance;
    }

    @Override
    public Future<Session> createSession(ConnectionInfo ci, boolean allowRedirect) {
        if (ci.isEmbedded() && !SchedulerThread.isScheduler()) {
            Scheduler scheduler = EmbeddedScheduler.getScheduler(ci);
            AsyncCallback<Session> ac = AsyncCallback.create(ci.isSingleThreadCallback());
            scheduler.handle(() -> {
                ServerSession session = createServerSession(ci);
                ac.setAsyncResult(session);
            });
            return ac;
        }
        return Future.succeededFuture(createServerSession(ci));
    }

    private ServerSession createServerSession(ConnectionInfo ci) {
        String dbName = ci.getDatabaseName();
        // 内嵌数据库，如果不存在，则自动创建
        if (ci.isEmbedded() && LealoneDatabase.getInstance().findDatabase(dbName) == null) {
            LealoneDatabase.getInstance().createEmbeddedDatabase(dbName, ci);
        }
        Database database = LealoneDatabase.getInstance().getDatabase(dbName);
        String targetNodes;
        if (ci.isEmbedded()) {
            targetNodes = null;
        } else {
            NetNode localNode = NetNode.getLocalTcpNode();
            targetNodes = database.getTargetNodes();
            // 为null时总是认为当前节点就是数据库所在的节点
            if (targetNodes == null) {
                targetNodes = localNode.getHostAndPort();
            } else if (!database.isTargetNode(localNode)) {
                ServerSession session = new ServerSession(database,
                        LealoneDatabase.getInstance().getSystemSession().getUser(), 0);
                session.setTargetNodes(targetNodes);
                session.setRunMode(database.getRunMode());
                session.setInvalid(true);
                return session;
            }
        }

        // 如果数据库正在关闭过程中，不等待重试了，直接抛异常
        // 如果数据库已经关闭了，那么在接下来的init中会重新打开
        if (database.isClosing()) {
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSING);
        }
        if (!database.isInitialized() && !initDatabase(database, ci)) {
            return null;
        }
        User user = validateUser(database, ci);
        ServerSession session = database.createSession(user, ci);
        session.setTargetNodes(targetNodes);
        session.setRunMode(database.getRunMode());
        return session;
    }

    // 只能有一个线程初始化数据库
    private boolean initDatabase(Database database, ConnectionInfo ci) {
        SchedulerLock schedulerLock = database.getSchedulerLock();
        if (schedulerLock.tryLock(SchedulerThread.currentScheduler())) {
            try {
                // sharding模式下访问remote page时会用到
                database.setLastConnectionInfo(ci);
                database.init();
            } finally {
                schedulerLock.unlock();
            }
            return true;
        }
        return false;
    }

    private User validateUser(Database database, ConnectionInfo ci) {
        User user = null;
        if (database.validateFilePasswordHash(ci.getProperty(DbSetting.CIPHER.getName(), null),
                ci.getFilePasswordHash())) {
            user = database.findUser(null, ci.getUserName());
            if (user != null) {
                Mode mode = Mode.getInstance(
                        ci.getProperty(DbSetting.MODE.name(), Mode.getDefaultMode().getName()));
                if (!user.validateUserPasswordHash(ci.getUserPasswordHash(), ci.getSalt(), mode)) {
                    user = null;
                } else {
                    database.setLastConnectionInfo(ci);
                }
            }
        }
        if (user == null) {
            throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
        }
        return user;
    }
}
