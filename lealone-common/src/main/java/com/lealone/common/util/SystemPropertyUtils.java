/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.util;

public class SystemPropertyUtils {

    private SystemPropertyUtils() {
        // utility class
    }

    public static int getInt(String key, int def) {
        String value = System.getProperty(key);
        return Utils.toInt(value, def);
    }

    public static long getLong(String key, long def) {
        String value = System.getProperty(key);
        return Utils.toLong(value, def);
    }

    public static boolean getBoolean(String key, boolean def) {
        String value = System.getProperty(key);
        return Utils.toBoolean(value, def);
    }
}
