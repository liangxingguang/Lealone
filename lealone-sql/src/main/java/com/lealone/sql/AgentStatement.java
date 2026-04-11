/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.agent.CodeAgent;
import com.lealone.db.result.LocalResult;
import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.sql.expression.ExpressionColumn;

public class AgentStatement extends StatementBase {

    private final String userPrompt;

    public AgentStatement(ServerSession session, String userPrompt) {
        super(session);
        this.userPrompt = userPrompt;
    }

    @Override
    public int getType() {
        return SQLStatement.UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public Result query(int maxRows) {
        CodeAgent agent = session.getDatabase().getCodeAgent();
        String content = agent.generateJavaCode(userPrompt);
        ExpressionColumn c = new ExpressionColumn(session.getDatabase(),
                new Column("content", Value.STRING));
        LocalResult result = new LocalResult(session, new IExpression[] { c, }, 1);
        Value[] row = { ValueString.get(content) };
        result.addRow(row);
        result.done();
        return result;
    }
}
