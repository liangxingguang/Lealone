/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.schema;

import com.lealone.db.DbObjectType;
import com.lealone.db.table.Column;

/**
 * Represents a domain (user-defined data type).
 *
 * @author H2 Group
 * @author zhh
 */
public class UserDataType extends SchemaObjectBase {

    private Column column;

    public UserDataType(Schema schema, int id, String name) {
        super(schema, id, name);
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.USER_DATATYPE;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    @Override
    public String getCreateSQL() {
        return "CREATE DOMAIN " + getSQL() + " AS " + column.getCreateSQL();
    }

    @Override
    public String getDropSQL() {
        return "DROP DOMAIN IF EXISTS " + getSQL();
    }
}
