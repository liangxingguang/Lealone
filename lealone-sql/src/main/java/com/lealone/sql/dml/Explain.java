/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.lealone.db.Database;
import com.lealone.db.async.Future;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.StatementBase;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;

/**
 * This class represents the statement
 * EXPLAIN
 */
public class Explain extends ManipulationStatement {

    private StatementBase command;
    private LocalResult result;
    private boolean executeCommand;

    public Explain(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.EXPLAIN;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    public void setCommand(StatementBase command) {
        this.command = command;
    }

    public void setExecuteCommand(boolean executeCommand) {
        this.executeCommand = executeCommand;
    }

    @Override
    public Future<Result> getMetaData() {
        Result r = query(-1);
        return Future.succeededFuture(r);
    }

    @Override
    public PreparedSQLStatement prepare() {
        command.prepare();
        return this;
    }

    @Override
    public Result query(int maxRows) {
        Column column = new Column("PLAN", Value.STRING);
        Database db = session.getDatabase();
        ExpressionColumn expr = new ExpressionColumn(db, column);
        Expression[] expressions = { expr };
        result = new LocalResult(session, expressions, 1);
        if (maxRows >= 0) {
            String plan;
            if (executeCommand) {
                db.statisticsStart();
                if (command.isQuery()) {
                    command.query(maxRows);
                } else {
                    command.update();
                }
                plan = command.getPlanSQL();
                Map<String, Integer> statistics = db.statisticsEnd();
                if (statistics != null) {
                    int total = 0;
                    for (Entry<String, Integer> e : statistics.entrySet()) {
                        total += e.getValue();
                    }
                    if (total > 0) {
                        statistics = new TreeMap<String, Integer>(statistics);
                        StringBuilder buff = new StringBuilder();
                        if (statistics.size() > 1) {
                            buff.append("total: ").append(total).append('\n');
                        }
                        for (Entry<String, Integer> e : statistics.entrySet()) {
                            int value = e.getValue();
                            int percent = (int) (100L * value / total);
                            buff.append(e.getKey()).append(": ").append(value);
                            if (statistics.size() > 1) {
                                buff.append(" (").append(percent).append("%)");
                            }
                            buff.append('\n');
                        }
                        plan += "\n/*\n" + buff.toString() + "*/";
                    }
                }
            } else {
                plan = command.getPlanSQL();
            }
            add(plan);
        }
        result.done();
        return result;
    }

    private void add(String text) {
        Value[] row = { ValueString.get(text) };
        result.addRow(row);
    }
}
