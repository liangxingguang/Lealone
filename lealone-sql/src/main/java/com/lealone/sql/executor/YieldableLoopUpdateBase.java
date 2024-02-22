/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.executor;

import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.session.SessionStatus;
import com.lealone.sql.StatementBase;

public abstract class YieldableLoopUpdateBase extends YieldableUpdateBase {

    protected final AtomicInteger updateCount = new AtomicInteger();
    protected int loopCount;
    private boolean loopEnd;
    private int pendingOperationCount;

    public YieldableLoopUpdateBase(StatementBase statement,
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        while (!loopEnd && pendingException == null) {
            session.setStatus(SessionStatus.STATEMENT_RUNNING);
            executeLoopUpdate();
            if (session.getStatus() == SessionStatus.STATEMENT_YIELDED
                    || session.getStatus() == SessionStatus.WAITING) {
                return;
            }
        }
        handleResult();
    }

    protected abstract void executeLoopUpdate();

    private void handleResult() {
        if (loopEnd && pendingOperationCount <= 0) {
            setResult(updateCount.get());
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }

    protected void onLoopEnd() {
        // 循环已经结束了，但是异步更新可能没有完成，所以先把状态改成STATEMENT_RUNNING，避免调度器空转
        session.setStatus(SessionStatus.STATEMENT_RUNNING);
        loopEnd = true;
    }

    protected void onPendingOperationStart() {
        pendingOperationCount++;
    }

    // 执行回调的线程跟执行命令的线程都是同一个
    protected void onPendingOperationComplete(AsyncResult<Integer> ar) {
        if (ar.isSucceeded()) {
            updateCount.incrementAndGet();
        } else {
            setPendingException(ar.getCause());
        }
        pendingOperationCount--;
        handleResult();
    }
}
