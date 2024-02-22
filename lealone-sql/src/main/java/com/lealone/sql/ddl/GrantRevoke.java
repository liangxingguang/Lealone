/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.DbObject;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.auth.RightOwner;
import com.lealone.db.auth.Role;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.service.Service;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statements
 * GRANT RIGHT,
 * GRANT ROLE,
 * REVOKE RIGHT,
 * REVOKE ROLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class GrantRevoke extends AuthStatement {

    private final ArrayList<DbObject> dbObjects = new ArrayList<>();
    private int operationType;
    private int rightMask;
    private ArrayList<String> roleNames;
    private Schema schema;
    private RightOwner grantee;

    public GrantRevoke(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return operationType;
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    /**
     * Add the specified right bit to the rights bitmap.
     *
     * @param right the right bit
     */
    public void addRight(int right) {
        this.rightMask |= right;
    }

    /**
     * Add the specified role to the list of roles.
     *
     * @param roleName the role
     */
    public void addRoleName(String roleName) {
        if (roleNames == null) {
            roleNames = new ArrayList<>();
        }
        roleNames.add(roleName);
    }

    public void setGranteeName(String granteeName) {
        Database db = session.getDatabase();
        grantee = db.findUser(session, granteeName);
        if (grantee == null) {
            grantee = db.findRole(session, granteeName);
            if (grantee == null) {
                throw DbException.get(ErrorCode.USER_OR_ROLE_NOT_FOUND_1, granteeName);
            }
        }
    }

    /**
     * Add the specified table to the list of tables.
     *
     * @param table the table
     */
    public void addTable(Table table) {
        dbObjects.add(table);
    }

    public void addService(Service service) {
        dbObjects.add(service);
    }

    /**
     * Set the specified schema
     *
     * @param schema the schema
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /**
     * @return true if this command is using Roles
     */
    public boolean isRoleMode() {
        return roleNames != null;
    }

    /**
     * @return true if this command is using Rights
     */
    public boolean isRightMode() {
        return rightMask != 0;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        DbObjectLock lock = db.tryExclusiveAuthLock(session);
        if (lock == null)
            return -1;

        if (roleNames != null) {
            for (String name : roleNames) {
                Role grantedRole = db.findRole(session, name);
                if (grantedRole == null) {
                    throw DbException.get(ErrorCode.ROLE_NOT_FOUND_1, name);
                }
                if (operationType == SQLStatement.GRANT) {
                    grantRole(grantedRole, lock);
                } else if (operationType == SQLStatement.REVOKE) {
                    revokeRole(grantedRole, lock);
                } else {
                    DbException.throwInternalError("type=" + operationType);
                }
            }
        } else {
            if (operationType == SQLStatement.GRANT) {
                grantRight(lock);
            } else if (operationType == SQLStatement.REVOKE) {
                revokeRight(lock);
            } else {
                DbException.throwInternalError("type=" + operationType);
            }
        }
        return 0;
    }

    private void grantRight(DbObjectLock lock) {
        if (schema != null) {
            grantRight(schema, lock);
        }
        for (DbObject object : dbObjects) {
            grantRight(object, lock);
        }
    }

    private void grantRight(DbObject object, DbObjectLock lock) {
        Database db = session.getDatabase();
        Right right = grantee.getRightForObject(object);
        if (right == null) {
            int id = getObjectId();
            right = new Right(db, id, grantee, rightMask, object);
            grantee.grantRight(object, right);
            db.addDatabaseObject(session, right, lock);
        } else {
            right.setRightMask(right.getRightMask() | rightMask);
            db.updateMeta(session, right);
        }
    }

    private void grantRole(Role grantedRole, DbObjectLock lock) {
        if (grantedRole != grantee && grantee.isRoleGranted(grantedRole)) {
            return;
        }
        if (grantee instanceof Role) {
            Role granteeRole = (Role) grantee;
            if (grantedRole.isRoleGranted(granteeRole)) {
                // cyclic role grants are not allowed
                throw DbException.get(ErrorCode.ROLE_ALREADY_GRANTED_1, grantedRole.getSQL());
            }
        }
        Database db = session.getDatabase();
        int id = getObjectId();
        Right right = new Right(db, id, grantee, grantedRole);
        db.addDatabaseObject(session, right, lock);
        grantee.grantRole(grantedRole, right);
    }

    private void revokeRight(DbObjectLock lock) {
        if (schema != null) {
            revokeRight(schema, lock);
        }
        for (DbObject object : dbObjects) {
            revokeRight(object, lock);
        }
    }

    private void revokeRight(DbObject object, DbObjectLock lock) {
        Right right = grantee.getRightForObject(object);
        if (right == null) {
            return;
        }
        int mask = right.getRightMask();
        int newRight = mask & ~rightMask;
        Database db = session.getDatabase();
        if (newRight == 0) {
            db.removeDatabaseObject(session, right, lock);
        } else {
            right.setRightMask(newRight);
            db.updateMeta(session, right);
        }
    }

    private void revokeRole(Role grantedRole, DbObjectLock lock) {
        Right right = grantee.getRightForRole(grantedRole);
        if (right == null) {
            return;
        }
        Database db = session.getDatabase();
        db.removeDatabaseObject(session, right, lock);
    }
}
