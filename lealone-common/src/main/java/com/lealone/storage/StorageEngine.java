/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.PluggableEngine;
import com.lealone.db.PluginManager;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.storage.lob.LobStorage;

public interface StorageEngine extends PluggableEngine {

    public static StorageEngine getDefaultStorageEngine() {
        return PluginManager.getPlugin(StorageEngine.class, Constants.DEFAULT_STORAGE_ENGINE_NAME);
    }

    StorageBuilder getStorageBuilder();

    LobStorage getLobStorage(DataHandler dataHandler, Storage storage);

    default Storage openStorage(String storagePath) {
        return getStorageBuilder().storagePath(storagePath).openStorage();
    }

    default Storage openStorage(String storagePath, SchedulerFactory schedulerFactory) {
        return getStorageBuilder().storagePath(storagePath).schedulerFactory(schedulerFactory)
                .openStorage();
    }
}
