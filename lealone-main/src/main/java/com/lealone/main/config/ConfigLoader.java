/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.main.config;

import com.lealone.common.exceptions.ConfigException;

public interface ConfigLoader {

    /**
     * Loads a {@link Config} object to use to configure a node.
     *
     * @return the {@link Config} to use.
     * @throws ConfigException if the configuration cannot be properly loaded.
     */
    Config loadConfig() throws ConfigException;

    void applyConfig(Config config) throws ConfigException;

}
