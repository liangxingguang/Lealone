/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import com.lealone.client.LealoneClient;
import com.lealone.client.jdbc.JdbcConnection;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.async.Future;
import com.lealone.test.TestBase;
import com.lealone.test.TestBase.MainTest;

public class SharedConnectionTest implements MainTest {

    public static void main(String[] args) throws Exception {
        TestBase test = new TestBase();
        test.addConnectionParameter(ConnectionSetting.IS_SHARED, "false");
        String url = test.getURL(LealoneDatabase.NAME);
        Future<JdbcConnection> f1 = LealoneClient.getConnection(url);

        test = new TestBase();
        // test.addConnectionParameter(ConnectionSetting.IS_SHARED, "true"); // 不设置时默认是共享模式
        test.addConnectionParameter(ConnectionSetting.MAX_SHARED_SIZE, "2");
        url = test.getURL(LealoneDatabase.NAME);
        Future<JdbcConnection> f2 = LealoneClient.getConnection(url);
        Future<JdbcConnection> f3 = LealoneClient.getConnection(url);
        Future<JdbcConnection> f4 = LealoneClient.getConnection(url);

        f1.get().close();
        f2.get().close();
        f3.get().close();
        f4.get().close();
    }
}
