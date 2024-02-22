/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import org.lealone.db.DataBuffer;
import org.lealone.db.DataBufferFactory;
import org.lealone.net.NetBufferFactory;

public class NioBufferFactory implements NetBufferFactory {

    private static final NioBufferFactory instance = new NioBufferFactory();

    public static NioBufferFactory getInstance() {
        return instance;
    }

    private NioBufferFactory() {
    }

    @Override
    public NioBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory) {
        return new NioBuffer(dataBufferFactory.create(initialSizeHint));
    }

    @Override
    public NioBuffer createBuffer(DataBuffer dataBuffer) {
        return new NioBuffer(dataBuffer);
    }
}
