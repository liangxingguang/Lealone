/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.common.exceptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import com.lealone.common.util.SortedProperties;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Constants;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;

/**
 * This exception wraps a checked exception.
 * It is used in methods where checked exceptions are not supported,
 * for example in a Comparator.
 */
public class DbException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final Properties MESSAGES = new Properties();

    static {
        try {
            InputStream messages = Utils
                    .getResourceAsStream(Constants.RESOURCES_DIR + "_messages_en.prop");
            MESSAGES.load(messages);
            if (SysProperties.USE_TRANSLATION_MESSAGE) {
                String language = Locale.getDefault().getLanguage();
                if (!"en".equals(language)) {
                    byte[] translations = Utils
                            .getResource(Constants.RESOURCES_DIR + "_messages_" + language + ".prop");
                    // message: translated message + english
                    // (otherwise certain applications don't work)
                    Properties p = SortedProperties.fromLines(new String(translations, Constants.UTF8));
                    for (Entry<Object, Object> e : p.entrySet()) {
                        String key = (String) e.getKey();
                        String translation = (String) e.getValue();
                        if (translation != null && !translation.startsWith("#")) {
                            // String original = MESSAGES.getProperty(key);
                            // String message = translation + "\n" + original;
                            // MESSAGES.put(key, message);
                            MESSAGES.put(key, translation);
                        }
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            DbException.traceThrowable(e);
        } catch (IOException e) {
            DbException.traceThrowable(e);
        }
    }

    private DbException(SQLException e) {
        super(e.getMessage(), e);
    }

    private static String translate(String key, String... params) {
        String message = null;
        if (MESSAGES != null) {
            // Tomcat sets final static fields to null sometimes
            message = MESSAGES.getProperty(key);
        }
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String s = params[i];
                if (s != null && s.length() > 0) {
                    params[i] = StringUtils.quoteIdentifier(s);
                }
            }
            message = MessageFormat.format(message, (Object[]) params);
        }
        return message;
    }

    /**
     * Get the SQLException object.
     *
     * @return the exception
     */
    public SQLException getSQLException() {
        return (SQLException) getCause();
    }

    /**
     * Get the error code.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getSQLException().getErrorCode();
    }

    /**
     * Set the SQL statement of the given exception.
     * This method may create a new object.
     *
     * @param sql the SQL statement
     * @return the exception
     */
    public DbException addSQL(String sql) {
        SQLException e = getSQLException();
        if (e instanceof JdbcSQLException) {
            JdbcSQLException j = (JdbcSQLException) e;
            if (j.getSQL() == null) {
                j.setSQL(sql);
            }
            return this;
        }
        e = new JdbcSQLException(e.getMessage(), sql, e.getSQLState(), e.getErrorCode(), e, null);
        return new DbException(e);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @return the exception
     */
    public static DbException get(int errorCode) {
        return get(errorCode, (String) null);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String p1) {
        return get(errorCode, new String[] { p1 });
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, Throwable cause, String... params) {
        return new DbException(getJdbcSQLException(errorCode, cause, params));
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String... params) {
        return new DbException(getJdbcSQLException(errorCode, null, params));
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index) {
        sql = StringUtils.addAsterisk(sql, index);
        return get(ErrorCode.SYNTAX_ERROR_1, sql);
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @param message the message
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index, String message) {
        sql = StringUtils.addAsterisk(sql, index);
        return new DbException(getJdbcSQLException(ErrorCode.SYNTAX_ERROR_2, null, sql, message));
    }

    /**
     * Gets a SQL exception meaning this feature is not supported.
     *
     * @param message what exactly is not supported
     * @return the exception
     */
    public static DbException getUnsupportedException(String message) {
        return get(ErrorCode.FEATURE_NOT_SUPPORTED_1, message);
    }

    /**
     * Gets a SQL exception meaning this value is invalid.
     *
     * @param param the name of the parameter
     * @param value the value passed
     * @return the IllegalArgumentException object
     */
    public static DbException getInvalidValueException(String param, Object value) {
        return get(ErrorCode.INVALID_VALUE_2, value == null ? "null" : value.toString(), param);
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @param s the message
     * @return the RuntimeException object
     * @throws RuntimeException the exception
     */
    public static RuntimeException throwInternalError(String s) {
        throw getInternalError(s);
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @return the RuntimeException object
     */
    public static RuntimeException throwInternalError() {
        throw getInternalError();
    }

    public static RuntimeException getInternalError() {
        return getInternalError("Unexpected code path");
    }

    public static RuntimeException getInternalError(String s) {
        RuntimeException e = new RuntimeException(s);
        DbException.traceThrowable(e);
        return e;
    }

    /**
     * Convert an exception to a SQL exception using the default mapping.
     *
     * @param e the root cause
     * @return the SQL exception object
     */
    public static SQLException toSQLException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return convert(e).getSQLException();
    }

    /**
     * Convert a throwable to an SQL exception using the default mapping. All
     * errors except the following are re-thrown: StackOverflowError,
     * LinkageError.
     *
     * @param e the root cause
     * @return the exception object
     */
    public static DbException convert(Throwable e) {
        if (e instanceof DbException) {
            return (DbException) e;
        } else if (e instanceof SQLException) {
            return new DbException((SQLException) e);
        } else if (e instanceof InvocationTargetException) {
            return convertInvocation((InvocationTargetException) e, null);
        } else if (e instanceof IOException) {
            return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
        } else if (e instanceof OutOfMemoryError) {
            return get(ErrorCode.OUT_OF_MEMORY, e);
        } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
            return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
        } else if (e instanceof Error) {
            throw (Error) e;
        }
        return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
    }

    /**
     * Convert an InvocationTarget exception to a database exception.
     *
     * @param te the root cause
     * @param message the added message or null
     * @return the database exception object
     */
    public static DbException convertInvocation(InvocationTargetException te, String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof DbException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(ErrorCode.EXCEPTION_IN_FUNCTION_1, t, message);
    }

    /**
     * Convert an IO exception to a database exception.
     *
     * @param e the root cause
     * @param message the message or null
     * @return the database exception object
     */
    public static DbException convertIOException(IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof DbException) {
                return (DbException) t;
            }
            return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
        }
        return get(ErrorCode.IO_EXCEPTION_2, e, e.toString(), message);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the SQLException object
     */
    private static JdbcSQLException getJdbcSQLException(int errorCode, Throwable cause,
            String... params) {
        String sqlState = ErrorCode.getState(errorCode);
        String message = translate(sqlState, params);
        return new JdbcSQLException(message, null, sqlState, errorCode, cause, null);
    }

    /**
     * Convert an exception to an IO exception.
     *
     * @param e the root cause
     * @return the IO exception
     */
    public static IOException convertToIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof JdbcSQLException) {
            JdbcSQLException e2 = (JdbcSQLException) e;
            if (e2.getOriginalCause() != null) {
                e = e2.getOriginalCause();
            }
        }
        return new IOException(e.toString(), e);
    }

    /**
     * Write the exception to the driver manager log writer if configured.
     *
     * @param e the exception
     */
    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }

    public static Throwable getRootCause(Throwable cause) {
        Throwable root = cause;
        while (cause != null) {
            root = cause;
            cause = cause.getCause();
        }
        return root;
    }

    public static Throwable getCause(Throwable t) {
        if (t instanceof DbException)
            return ((DbException) t).getSQLException();
        else
            return t;
    }

    // 构建时可以手工改成false,这样就不会编译
    public static final boolean ASSERT = Utils.getProperty("lealone.assert", true);

    public static void assertTrue(boolean test) {
        if (!test)
            throwInternalError();
    }
}
