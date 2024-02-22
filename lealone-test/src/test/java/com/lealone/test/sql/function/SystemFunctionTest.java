/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.function;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class SystemFunctionTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        testCASE();
        testSystemFunction();
    }

    private void testCASE() throws Exception {
        // sql = "SELECT CASE(1>0, 1, b<0, 2)"; //不能这样用

        // 如果为null，取ELSE
        sql = "SELECT CASE @v3 WHEN 0 THEN 'No' WHEN 1 THEN 'One' ELSE 'Some' END";
        assertCASE("Some", 1);

        // 如果为null，取ELSE
        sql = "SELECT SET(@v, null), CASE @v WHEN 0 THEN 'No' WHEN 1 THEN 'One' ELSE 'Some' END";
        assertCASE("Some", 2);

        // 如果为null，没有ELSE，返回null
        sql = "SELECT SET(@v, null), CASE @v WHEN 0 THEN 'No' WHEN 1 THEN 'One' END";
        assertCASE(null, 2);

        sql = "SELECT SET(@v, 1), CASE @v WHEN 0 THEN 'No' WHEN 1 THEN 'One' ELSE 'Some' END";
        assertCASE("One", 2);

        sql = "SELECT CASE @v5 WHEN 0 THEN 'No' WHEN 1 THEN 'One'END";
        assertCASE(null, 1);

        sql = "SELECT SET(@v, 9), CASE WHEN @v<10 THEN 'Low' ELSE 'High' END";
        assertCASE("Low", 2);

        sql = "SELECT SET(@v, 15), CASE WHEN @v<10 THEN 'Low' ELSE 'High' END";
        assertCASE("High", 2);

        sql = "SELECT SET(@v, 15), CASE WHEN @v<10 THEN 'Low'END";
        assertCASE(null, 2);

        // https://github.com/lealone/Lealone/issues/147
        sql = "SELECT CASE WHEN SEQUENCE_NAME IS NULL THEN 0 ELSE 1 END IS_AUTOINCREMENT"
                + " FROM INFORMATION_SCHEMA.COLUMNS ";
        // 执行两次，触发缓存
        executeQuery(sql);
        executeQuery(sql);

        // https://github.com/lealone/Lealone/issues/177
        executeQuery("SELECT SET(@v, 15)");
        sql = "SELECT SUM(CASE WHEN @v<10 THEN 1 ELSE 2 END)";
        assertCASE("2", 1);
        executeQuery("SELECT SET(@v, 5)");
        // 不能这么用，会先对sum的on求值
        sql = "SELECT SET(@v, 5), SUM(CASE WHEN @v<10 THEN 1 ELSE 2 END)";
        sql = "SELECT SUM(CASE WHEN @v<10 THEN 1 ELSE 2 END)";
        assertCASE("1", 1);

        sql = "SELECT SUM(CASE @v WHEN 1 THEN 1 WHEN 5 THEN 5 END)";
        assertCASE("5", 1);
    }

    private void assertCASE(Object expected, int index) throws Exception {
        executeQuery(sql);
        assertEquals(expected, getStringValue(index, true));
    }

    private void testSystemFunction() throws Exception {
        sql = "SELECT DECODE(RAND()>0.5, 0, 'Red', 1, 'Black')";

        sql = "SELECT DECODE(RAND()>0.5, 0, 'Red1', 0, 'Red2', 1, 'Black1', 1, 'Black2')";

        sql = "SELECT DECODE(RAND()>0.5, 2, 'Red1', 2, 'Red2', 2, 'Black1', 2)";
        sql = "SELECT DECODE(RAND()>0.5, 2, 'Red1', 2, 'Red2', 2, 'Black1', 2, 'Black2')";

        sql = "SELECT DECODE(0, 0, 'v1', 0,/'v2', 1, 'v3', 1, 'v4')";

        // ROW_NUMBER函数虽然定义了，但ROW_NUMBER()函数无效，不支持这样的语法
        sql = "SELECT ROW_NUMBER()";
        // ROWNUM函数虽然没有定义，但ROWNUM()是有效，Parser在解析时把他当成ROWNUM伪字段处理
        // 当成了com.lealone.expression.Rownum，见com.lealone.command.Parser.readTerm()
        sql = "SELECT ROWNUM()";
        // 这样就没问题了,在这个方法中com.lealone.command.Parser.readFunction(Schema, String)
        // 把ROW_NUMBER转成com.lealone.expression.Rownum了
        sql = "SELECT ROW_NUMBER()OVER()";

        // 相等返回null，不相等返回v0
        sql = "SELECT NULLIF(1,2)"; // 1
        sql = "SELECT NULLIF(1,1)"; // null

        sql = "SELECT DATABASE()";
        sql = "SELECT USER(), CURRENT_USER()";
        sql = "SELECT IDENTITY(), SCOPE_IDENTITY()";
        sql = "SELECT LOCK_TIMEOUT()";
        sql = "SELECT MEMORY_FREE(), MEMORY_USED()";

        sql = "SELECT GREATEST(1,2,3), LEAST(1,2,3)";

        sql = "SELECT ARRAY_GET(('Hello', 'World'), 2), ARRAY_LENGTH(('Hello', 'World')), "
                + "ARRAY_CONTAINS(('Hello', 'World'), 'Hello')";

        executeUpdate("CREATE SEQUENCE IF NOT EXISTS SEQ_ID");
        sql = "SELECT CURRVAL('SEQ_ID'), NEXTVAL('SEQ_ID')";
        printResultSet();

        sql = "SELECT CAST(65535 AS BINARY);";
        printResultSet();
    }
}
