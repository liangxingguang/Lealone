/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import java.util.Set;

import com.lealone.db.DbObject;
import com.lealone.db.table.Table;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.aggregate.JavaAggregate;
import com.lealone.sql.expression.function.JavaFunction;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.Query;

public class DependenciesVisitor extends VoidExpressionVisitor {

    private Set<DbObject> dependencies;

    public DependenciesVisitor(Set<DbObject> dependencies) {
        this.dependencies = dependencies;
    }

    public void addDependency(DbObject obj) {
        dependencies.add(obj);
    }

    public Set<DbObject> getDependencies() {
        return dependencies;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        if (e.getColumn() != null)
            addDependency(e.getColumn().getTable());
        return null;
    }

    @Override
    public Void visitSequenceValue(SequenceValue e) {
        addDependency(e.getSequence());
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        addDependency(e.getUserAggregate());
        super.visitJavaAggregate(e);
        return null;
    }

    @Override
    public Void visitJavaFunction(JavaFunction e) {
        addDependency(e.getFunctionAlias());
        super.visitJavaFunction(e);
        return null;
    }

    @Override
    protected Void visitQuery(Query query) {
        super.visitQuery(query);
        for (int i = 0, size = query.getFilters().size(); i < size; i++) {
            TableFilter f = query.getFilters().get(i);
            Table table = f.getTable();
            addDependency(table);
            table.addDependencies(dependencies);
        }
        return null;
    }
}
