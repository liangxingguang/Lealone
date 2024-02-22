/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.schema;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;
import com.lealone.db.schema.Schema;
import com.lealone.test.db.DbObjectTestBase;

public class SchemaTest extends DbObjectTestBase {

    String userName = "sa1";
    String schemaName = "SchemaTest";
    Schema schema;
    int id;

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        executeUpdate("CREATE USER IF NOT EXISTS " + userName + " PASSWORD 'abc' ADMIN");

        id = db.allocateObjectId();

        Schema schema = new Schema(db, id, schemaName, db.getUser(session, userName), false);
        assertEquals(id, schema.getId());

        db.addDatabaseObject(session, schema, null);
        assertNotNull(db.findSchema(session, schemaName));
        assertNotNull(findMeta(id));

        db.removeDatabaseObject(session, schema, null);
        assertNull(db.findSchema(session, schemaName));
        assertNull(findMeta(id));

        session.commit();

        // 测试SQL
        // -----------------------------------------------
        executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName + " AUTHORIZATION " + userName);
        schema = db.findSchema(session, schemaName);
        assertNotNull(schema);
        id = schema.getId();
        assertNotNull(findMeta(id));

        try {
            executeUpdate("CREATE SCHEMA " + schemaName + " AUTHORIZATION " + userName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_ALREADY_EXISTS_1);
        }

        try {
            // SchemaTest_u1需要有Right.ALTER_ANY_SCHEMA权限
            executeUpdate("CREATE USER IF NOT EXISTS SchemaTest_u1 PASSWORD 'abc'");
            executeUpdate("CREATE SCHEMA IF NOT EXISTS SchemaTest_s1 AUTHORIZATION SchemaTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ADMIN_RIGHTS_REQUIRED);
        } finally {
            executeUpdate("DROP USER IF EXISTS SchemaTest_u1");
        }
    }

    void alter() {
        executeUpdate("CREATE SCHEMA IF NOT EXISTS SchemaTest_s1 AUTHORIZATION " + userName);

        try {
            // 不能RENAME INFORMATION_SCHEMA(system Schema)
            executeUpdate("ALTER SCHEMA INFORMATION_SCHEMA RENAME TO SchemaTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1);
        }

        try {
            executeUpdate("ALTER SCHEMA SchemaTest_s1 RENAME TO " + schemaName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_ALREADY_EXISTS_1);
        }

        String schemaName = "SchemaTest_s2";
        executeUpdate("ALTER SCHEMA SchemaTest_s1 RENAME TO " + schemaName);
        assertNull(db.findSchema(session, "SchemaTest_s1"));
        assertNotNull(db.findSchema(session, schemaName));
        executeUpdate("DROP SCHEMA IF EXISTS " + schemaName);
    }

    void drop() {
        schema = db.findSchema(session, schemaName);
        // 增加一个CONSTANT，可以看看是否同时删了
        String constantName = "ConstantTest";
        executeUpdate("CREATE CONSTANT IF NOT EXISTS " + schemaName + ".ConstantTest VALUE 10");
        assertNotNull(schema.findConstant(session, constantName));
        executeUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        assertNull(schema.findConstant(session, constantName));
        schema = db.findSchema(session, schemaName);
        assertNull(schema);
        assertNull(findMeta(id));
        try {
            executeUpdate("DROP SCHEMA " + schemaName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_NOT_FOUND_1);
        }

        try {
            // 不能删除system Schema
            executeUpdate("DROP SCHEMA INFORMATION_SCHEMA");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1);
        }
        executeUpdate("DROP USER IF EXISTS " + userName);
    }
}
