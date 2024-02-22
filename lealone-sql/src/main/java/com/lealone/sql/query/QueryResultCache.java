/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import java.util.ArrayList;

import com.lealone.db.Database;
import com.lealone.db.result.LocalResult;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.visitor.ExpressionVisitorFactory;

class QueryResultCache {

    private final Select select;
    private final ServerSession session;

    private boolean noCache;
    private int lastLimit;
    private long lastEvaluated;
    private Value[] lastParameters;
    private LocalResult lastResult;
    private boolean cacheableChecked;

    QueryResultCache(Select select) {
        this.select = select;
        session = select.getSession();
    }

    void disable() {
        noCache = true;
    }

    boolean isNotCachable() {
        return noCache || !session.getDatabase().getOptimizeReuseResults();
    }

    void setResult(LocalResult r) {
        if (isNotCachable())
            return;
        if (!isDeterministic())
            disable();
        else
            lastResult = r;
    }

    LocalResult getResult(int limit) {
        if (isNotCachable()) {
            return null;
        } else {
            Value[] params = getParameterValues();
            long now = session.getDatabase().getModificationDataId();
            // 当lastEvaluated != now时，说明数据已经有变化，缓存的结果不能用了
            if (lastEvaluated == now && lastResult != null && !lastResult.isClosed()
                    && limit == lastLimit) {
                if (sameResultAsLast(params)) {
                    lastResult = lastResult.createShallowCopy(session);
                    if (lastResult != null) {
                        lastResult.reset();
                        return lastResult;
                    }
                }
            }
            lastLimit = limit;
            lastEvaluated = now;
            lastParameters = params;
            if (lastResult != null) {
                lastResult.close();
                lastResult = null;
            }
            return null;
        }
    }

    private boolean isDeterministic() {
        return select.accept(ExpressionVisitorFactory.getDeterministicVisitor());
    }

    private Value[] getParameterValues() {
        ArrayList<Parameter> list = select.getParameters();
        if (list == null || list.isEmpty()) {
            return null;
        }
        int size = list.size();
        Value[] params = new Value[size];
        for (int i = 0; i < size; i++) {
            params[i] = list.get(i).getValue();
        }
        return params;
    }

    private boolean sameResultAsLast(Value[] params) {
        if (!cacheableChecked) {
            long max = select.getMaxDataModificationId();
            noCache = max == Long.MAX_VALUE;
            cacheableChecked = true;
        }
        if (noCache) {
            return false;
        }
        Database db = session.getDatabase();
        if (!sameParamsAsLast(db, params))
            return false;
        if (!select.accept(ExpressionVisitorFactory.getIndependentVisitor())) {
            return false;
        }
        if (db.getModificationDataId() > lastEvaluated
                && select.getMaxDataModificationId() > lastEvaluated) {
            return false;
        }
        return true;
    }

    private boolean sameParamsAsLast(Database db, Value[] params) {
        if (params == null && lastParameters == null)
            return true;
        if (params != null && lastParameters != null) {
            if (params.length != lastParameters.length)
                return false;
            for (int i = 0; i < params.length; i++) {
                Value a = lastParameters[i], b = params[i];
                if (a.getType() != b.getType() || !db.areEqual(a, b)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
