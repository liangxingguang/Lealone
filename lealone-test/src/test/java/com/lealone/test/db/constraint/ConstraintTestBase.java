/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.constraint;

import java.util.ArrayList;

import com.lealone.db.constraint.Constraint;
import com.lealone.test.db.DbObjectTestBase;

public abstract class ConstraintTestBase extends DbObjectTestBase {

    protected void assertFound(String tableName, String constraintName) {
        Constraint constraint = schema.findConstraint(session, constraintName);
        assertNotNull(constraint);
        ArrayList<Constraint> constraints = schema.findTableOrView(session, tableName).getConstraints();
        assertTrue(constraints.contains(constraint));
    }

    protected void assertNotFound(String tableName, String constraintName) {
        Constraint constraint = schema.findConstraint(session, constraintName);
        assertNull(constraint);
        ArrayList<Constraint> constraints = schema.findTableOrView(session, tableName).getConstraints();
        assertFalse(constraints.contains(constraint));
    }
}
