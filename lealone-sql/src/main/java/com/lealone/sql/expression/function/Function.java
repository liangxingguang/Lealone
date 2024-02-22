/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.function;

import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.lealone.common.util.StatementBuilder;
import com.lealone.common.util.StringUtils;
import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.ValueResultSet;
import com.lealone.sql.expression.Expression;

/**
 * This class is used by the built-in functions,
 * as well as the user-defined functions.
 *
 * @author H2 Group
 * @author zhh
 */
public abstract class Function extends Expression {

    protected static final int VAR_ARGS = -1;
    protected static final long PRECISION_UNKNOWN = -1;
    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    private static final CopyOnWriteArrayList<FunctionFactory> FACTORIES = new CopyOnWriteArrayList<>();
    private static boolean inited;

    // 放在FACTORIES字段之后，否则FACTORIES为null
    static {
        BuiltInFunctionFactory.register();
    }

    public static void registerFunctionFactory(FunctionFactory factory) {
        FACTORIES.add(factory);
    }

    public static void deregisterFunctionFactory(FunctionFactory factory) {
        FACTORIES.remove(factory);
    }

    private synchronized static void initFunctionFactories() {
        if (inited)
            return;
        for (FunctionFactory factory : FACTORIES) {
            factory.init();
        }
        inited = true;
    }

    protected static FunctionInfo addFunction(String name, int type, int parameterCount, int dataType,
            boolean nullIfParameterIsNull, boolean deterministic) {
        FunctionInfo info = new FunctionInfo();
        info.name = name;
        info.type = type;
        info.parameterCount = parameterCount;
        info.dataType = dataType;
        info.nullIfParameterIsNull = nullIfParameterIsNull;
        info.deterministic = deterministic;
        FUNCTIONS.put(name, info);
        return info;
    }

    protected static FunctionInfo addFunctionNotDeterministic(String name, int type, int parameterCount,
            int dataType) {
        return addFunction(name, type, parameterCount, dataType, true, false);
    }

    protected static FunctionInfo addFunction(String name, int type, int parameterCount, int dataType) {
        return addFunction(name, type, parameterCount, dataType, true, true);
    }

    protected static FunctionInfo addFunctionWithNull(String name, int type, int parameterCount,
            int dataType) {
        return addFunction(name, type, parameterCount, dataType, false, true);
    }

    // 允许一个函数有多个别名
    protected static FunctionInfo addFunction(String name, FunctionInfo info) {
        FUNCTIONS.put(name, info);
        return info;
    }

    /**
     * Get the function info object for this function, or null if there is no
     * such function.
     *
     * @param name the function name
     * @return the function info
     */
    public static FunctionInfo getFunctionInfo(String name) {
        FunctionInfo info = FUNCTIONS.get(name);
        if (info == null && !inited) {
            initFunctionFactories();
            info = FUNCTIONS.get(name);
        }
        return info;
    }

    /**
     * Get an instance of the given function for this database.
     * If no function with this name is found, null is returned.
     *
     * @param database the database
     * @param name the function name
     * @return the function object or null
     */
    public static Function getFunction(Database database, String name) {
        if (!database.getSettings().databaseToUpper) {
            // if not yet converted to uppercase, do it now
            name = StringUtils.toUpperEnglish(name);
        }
        FunctionInfo info = getFunctionInfo(name);
        if (info == null) {
            return null;
        }
        if (info.factory != null)
            return info.factory.createFunction(database, info);
        else
            return BuiltInFunctionFactory.INSTANCE.createFunction(database, info);
    }

    protected Expression[] args;

    /**
     * Get the function arguments.
     *
     * @return argument list
     */
    public Expression[] getArgs() {
        return args;
    }

    protected boolean optimizeArgs(ServerSession session) {
        boolean allConst = isDeterministic();
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e == null) {
                continue;
            }
            e = e.optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        return allConst;
    }

    protected void appendArgs(StatementBuilder buff) {
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
    }

    /**
     * Get the name of the function.
     *
     * @return the name
     */
    abstract String getName();

    /**
     * Get an empty result set with the column names set.
     *
     * @param session the session
     * @param args the argument list (some arguments may be null)
     * @return the empty result set
     */
    abstract ValueResultSet getValueForColumnList(ServerSession session, Expression[] args);

    /**
     * Get the data type.
     *
     * @return the data type
     */
    @Override
    public abstract int getType();

    /**
     * Optimize the function if possible.
     *
     * @param session the session
     * @return the optimized expression
     */
    @Override
    public abstract Expression optimize(ServerSession session);

    /**
     * Get the SQL snippet of the function (including arguments).
     *
     * @return the SQL snippet.
     */
    @Override
    public abstract String getSQL();

    public void setParameter(int index, Expression param) {
    }

    public void doneWithParameters() {
    }

    public void setDataType(Column col) {
    }

    public abstract int getFunctionType();

    /**
     * Whether the function always returns the same result for the same
     * parameters.
     *
     * @return true if it does
     */
    public abstract boolean isDeterministic();

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     *
     * @return true if it should be.
     */
    abstract boolean isBufferResultSetToLocalTemp();
}
