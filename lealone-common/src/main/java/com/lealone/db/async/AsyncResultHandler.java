/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

public interface AsyncResultHandler<T> extends AsyncHandler<AsyncResult<T>> {

    @Override
    void handle(AsyncResult<T> ar);

    default void handleResult(T result) {
        handle(new AsyncResult<>(result));
    }

    default void handleException(Throwable cause) {
        handle(new AsyncResult<>(cause));
    }

    public static final AsyncResultHandler<?> EMPTY = ar -> {
    };

    @SuppressWarnings("unchecked")
    public static <E> AsyncResultHandler<E> emptyHandler() {
        return (AsyncResultHandler<E>) EMPTY;
    }
}
