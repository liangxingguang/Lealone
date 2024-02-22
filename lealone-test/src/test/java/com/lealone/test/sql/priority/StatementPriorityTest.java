/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.priority;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import com.lealone.common.util.JdbcUtils;
import com.lealone.test.TestBase.MainTest;
import com.lealone.test.sql.SqlTestBase;

public class StatementPriorityTest extends SqlTestBase implements MainTest {

    public static void main(String[] args) throws Exception {
        new StatementPriorityTest().runTest();
    }

    static Connection getConn(String url) throws Exception {
        return DriverManager.getConnection(url);
    }

    class MyThread extends Thread {
        Connection connection;
        Statement statement;

        boolean insert;

        public MyThread(boolean insert) {
            this.insert = insert;
            try {
                connection = StatementPriorityTest.this.getConnection();
                statement = connection.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Random random = new Random();

        @Override
        public void run() {
            try {
                if (insert) {
                    insert();
                } else {
                    select();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        public void close() throws SQLException {
            statement.close();
            connection.close();
        }

        void insert() throws Exception {
            int loop = 10002;
            for (int i = 10000; i < loop; i++) {
                String sql = "INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('" + i
                        + "', 'a1', 'b', 51)";
                statement.executeUpdate(sql);
            }
        }

        void select() throws Exception {
            int loop = 10;
            while (loop-- > 0) {
                try {
                    stmt.executeQuery("SELECT * FROM StatementPriorityTest");
                    // stmt.executeQuery("SELECT * FROM StatementPriorityTest where pk = " + random.nextInt(20000));
                    // statement.executeQuery(StatementPriorityTest.this.sql);
                    // Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    // @Test
    public void test() throws Exception {
        init();
        sql = "select * from StatementPriorityTest where pk = '01'";
        // oneThread();
        // multiThreads();

        Thread t1 = new Thread(() -> {
            Connection connection = null;
            Statement statement = null;
            try {
                connection = StatementPriorityTest.this.getConnection();
                statement = connection.createStatement();
                statement.executeUpdate("delete from StatementPriorityTest");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                JdbcUtils.closeSilently(statement);
                JdbcUtils.closeSilently(connection);
            }
        });

        Thread t2 = new Thread(() -> {
            Connection connection = null;
            Statement statement = null;
            try {
                connection = StatementPriorityTest.this.getConnection();
                statement = connection.createStatement();
                String sql = "INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)";
                statement.executeUpdate(sql);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                JdbcUtils.closeSilently(statement);
                JdbcUtils.closeSilently(connection);
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    public void multiThreads() throws Exception {
        int count = 2;
        MyThread[] insertThreads = new MyThread[count];

        MyThread[] selectThreads = new MyThread[count];

        for (int i = 0; i < count; i++) {
            insertThreads[i] = new MyThread(true);
            selectThreads[i] = new MyThread(false);
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].start();
            selectThreads[i].start();
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].join();
            selectThreads[i].join();
        }

        for (int i = 0; i < count; i++) {
            insertThreads[i].close();
            selectThreads[i].close();
        }
    }

    public void oneThread() throws Exception {
        int count = 342;
        Connection[] connections = new Connection[count];

        Statement[] statements = new Statement[count];

        for (int i = 0; i < count; i++) {
            connections[i] = getConnection();
            statements[i] = connections[i].createStatement();
        }

        int loop = 1000000;
        while (loop-- > 0) {
            for (int i = 0; i < count; i++) {
                statements[i].executeQuery(sql);
            }
            // Thread.sleep(1000);

            Thread.sleep(500);
        }

        for (int i = 0; i < count; i++) {
            statements[i].close();
            connections[i].close();
        }
    }

    void init() throws Exception {
        createTable("StatementPriorityTest");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");
        //
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");
        //
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");
        //
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        // executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");

        for (int i = 1000; i < 2000; i++)
            executeUpdate("INSERT INTO StatementPriorityTest(pk, f1, f2, f3) VALUES('" + i
                    + "', 'a1', 'b', 51)");
    }
}
