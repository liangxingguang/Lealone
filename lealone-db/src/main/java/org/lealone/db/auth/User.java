/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.auth;

import java.util.ArrayList;
import java.util.Arrays;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.security.SHA256;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectType;
import org.lealone.db.Mode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.MetaTable;
import org.lealone.db.table.RangeTable;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableType;
import org.lealone.db.table.TableView;

/**
 * Represents a user object.
 */
public class User extends RightOwner {

    private final boolean systemUser;
    private boolean admin;
    private byte[] salt;
    private byte[] passwordHash;
    private byte[] userPasswordHash;

    private byte[] saltMongo;
    private byte[] passwordHashMongo;

    private byte[] saltMySQL;
    private byte[] passwordHashMySQL;;

    private byte[] saltPostgreSQL;
    private byte[] passwordHashPostgreSQL;

    public User(Database database, int id, String userName, boolean systemUser) {
        super(database, id, userName);
        this.systemUser = systemUser;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.USER;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public boolean isAdmin() {
        return admin;
    }

    /**
     * Set the salt and hash of the password for this user.
     *
     * @param salt the salt
     * @param hash the password hash
     */
    public void setSaltAndHash(byte[] salt, byte[] hash) {
        this.salt = salt;
        this.passwordHash = hash;
    }

    public void setSaltAndHashMongo(byte[] salt, byte[] hash) {
        this.saltMongo = salt;
        this.passwordHashMongo = hash;
    }

    public void setSaltAndHashMySQL(byte[] salt, byte[] hash) {
        this.saltMySQL = salt;
        this.passwordHashMySQL = hash;
    }

    public void setSaltAndHashPostgreSQL(byte[] salt, byte[] hash) {
        this.saltPostgreSQL = salt;
        this.passwordHashPostgreSQL = hash;
    }

    public byte[] getSalt() {
        return salt;
    }

    public byte[] getPasswordHash() {
        return passwordHash;
    }

    public byte[] getSaltMongo() {
        return saltMongo;
    }

    public byte[] getPasswordHashMongo() {
        return passwordHashMongo;
    }

    public byte[] getSaltMySQL() {
        return saltMySQL;
    }

    public byte[] getPasswordHashMySQL() {
        return passwordHashMySQL;
    }

    public byte[] getSaltPostgreSQL() {
        return saltPostgreSQL;
    }

    public byte[] getPasswordHashPostgreSQL() {
        return passwordHashPostgreSQL;
    }

    /**
     * Set the user name password hash. A random salt is generated as well.
     * The parameter is filled with zeros after use.
     *
     * @param userPasswordHash the user name password hash
     */
    public void setUserPasswordHash(byte[] userPasswordHash) {
        if (userPasswordHash != null) {
            if (userPasswordHash.length == 0) {
                salt = passwordHash = userPasswordHash;
            } else {
                salt = new byte[Constants.SALT_LEN];
                MathUtils.randomBytes(salt);
                passwordHash = SHA256.getHashWithSalt(userPasswordHash, salt);
            }
        }
        this.userPasswordHash = userPasswordHash;
    }

    public byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    /**
     * Checks that this user has the given rights for this database object.
     *
     * @param table the database object
     * @param rightMask the rights required
     * @throws DbException if this user does not have the required rights
     */
    public void checkRight(Table table, int rightMask) {
        if (!hasRight(table, rightMask)) {
            throw DbException.get(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, table.getSQL());
        }
    }

    /**
     * See if this user has the given rights for this database object.
     *
     * @param table the database object, or null for schema-only check
     * @param rightMask the rights required
     * @return true if the user has the rights
     */
    public boolean hasRight(Table table, int rightMask) {
        if (rightMask != Right.SELECT && !systemUser && table != null) {
            table.checkWritingAllowed();
        }
        if (admin) {
            return true;
        }
        Role publicRole = database.getPublicRole();
        if (publicRole.isRightGrantedRecursive(table, rightMask)) {
            return true;
        }
        if (table instanceof MetaTable || table instanceof RangeTable) {
            // everybody has access to the metadata information
            return true;
        }
        if (table != null) {
            if (hasRight(null, Right.ALTER_ANY_SCHEMA)) {
                return true;
            }
            TableType tableType = table.getTableType();
            if (tableType == TableType.VIEW) {
                TableView v = (TableView) table;
                if (v.getOwner() == this) {
                    // the owner of a view has access:
                    // SELECT * FROM (SELECT * FROM ...)
                    return true;
                }
            } else if (tableType == TableType.FUNCTION_TABLE) {
                return true;
            }
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                // the owner has all rights on local temporary tables
                return true;
            }
        }
        if (isRightGrantedRecursive(table, rightMask)) {
            return true;
        }
        return false;
    }

    public void checkRight(Service service, int rightMask) {
        if (!hasServiceRight(service, rightMask)) {
            throw DbException.get(ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1, service.getSQL());
        }
    }

    private boolean hasServiceRight(Service service, int rightMask) {
        if (admin) {
            return true;
        }
        Role publicRole = database.getPublicRole();
        if (publicRole.isRightGrantedRecursive(service, rightMask)) {
            return true;
        }
        if (service != null) {
            if (hasRight(null, Right.ALTER_ANY_SCHEMA)) {
                return true;
            }
        }
        if (isRightGrantedRecursive(service, rightMask)) {
            return true;
        }
        return false;
    }

    /**
     * Check the password of this user.
     *
     * @param userPasswordHash the password data (the user password hash)
     * @return true if the user password hash is correct
     */
    public boolean validateUserPasswordHash(byte[] userPasswordHash, byte[] salt, Mode mode) {
        return PasswordHash.validateUserPasswordHash(this, userPasswordHash, salt, mode);
    }

    /**
     * Check if this user has admin rights. An exception is thrown if he does
     * not have them.
     *
     * @throws DbException if this user is not an admin
     */
    public void checkAdmin() {
        if (!admin) {
            throw DbException.get(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    /**
     * Check if this user has schema admin rights. An exception is thrown if he
     * does not have them.
     *
     * @throws DbException if this user is not a schema admin
     */
    public void checkSchemaAdmin() {
        if (!hasRight(null, Right.ALTER_ANY_SCHEMA)) {
            throw DbException.get(ErrorCode.ADMIN_RIGHTS_REQUIRED);
        }
    }

    /**
     * Check that this user does not own any schema. An exception is thrown if
     * he owns one or more schemas.
     *
     * @throws DbException if this user owns a schema
     */
    public void checkOwnsNoSchemas(ServerSession session) {
        for (Schema s : session.getDatabase().getAllSchemas()) {
            if (this == s.getOwner()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_2, getName(), s.getName());
            }
        }
    }

    public void checkSystemSchema(ServerSession session, Schema schema) {
        Database db = session.getDatabase();
        if (db.getSystemSession().getUser() != session.getUser() && db.isSystemSchema(schema))
            throw DbException.get(ErrorCode.ACCESS_DENIED_TO_SCHEMA_1, schema.getName());
    }

    /**
     * Get the CREATE SQL statement for this object.
     *
     * @param password true if the password (actually the salt and hash) should
     *            be returned
     * @return the SQL statement
     */
    public String getCreateSQL(boolean password) {
        StringBuilder buff = new StringBuilder("CREATE USER IF NOT EXISTS ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (password) {
            buff.append(" SALT '").append(StringUtils.convertBytesToHex(salt)).append("' HASH '")
                    .append(StringUtils.convertBytesToHex(passwordHash)).append('\'');
            if (passwordHashMongo != null && saltMongo != null)
                buff.append(" SALT_MONGO '").append(StringUtils.convertBytesToHex(saltMongo))
                        .append("' HASH_MONGO '")
                        .append(StringUtils.convertBytesToHex(passwordHashMongo)).append('\'');
            if (passwordHashMySQL != null && saltMySQL != null)
                buff.append(" SALT_MYSQL '").append(StringUtils.convertBytesToHex(saltMySQL))
                        .append("' HASH_MYSQL '")
                        .append(StringUtils.convertBytesToHex(passwordHashMySQL)).append('\'');
            if (passwordHashPostgreSQL != null && saltPostgreSQL != null)
                buff.append(" SALT_POSTGRESQL '").append(StringUtils.convertBytesToHex(saltPostgreSQL))
                        .append("' HASH_POSTGRESQL '")
                        .append(StringUtils.convertBytesToHex(passwordHashPostgreSQL)).append('\'');
        } else {
            buff.append(" PASSWORD ''");
        }
        if (admin) {
            buff.append(" ADMIN");
        }
        return buff.toString();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(true);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = new ArrayList<>();
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                children.add(right);
            }
        }
        for (Schema schema : database.getAllSchemas()) {
            if (schema.getOwner() == this) {
                children.add(schema);
            }
        }
        return children;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        for (Right right : database.getAllRights()) {
            if (right.getGrantee() == this) {
                database.removeDatabaseObject(session, right, lock);
            }
        }
        super.removeChildrenAndResources(session, lock);
    }

    @Override
    public void invalidate() {
        salt = null;
        Arrays.fill(passwordHash, (byte) 0);
        passwordHash = null;
        super.invalidate();
    }
}
