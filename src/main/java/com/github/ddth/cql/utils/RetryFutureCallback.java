package com.github.ddth.cql.utils;

import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.github.ddth.cql.SessionManager;
import com.google.common.util.concurrent.FutureCallback;

/**
 * A {@link FutureCallback} that retries executing the query on {@link ExceedMaxAsyncJobsException}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0.2
 */
public abstract class RetryFutureCallback<T> implements FutureCallback<T> {

    protected SessionManager sessionManager;
    protected long permitTimeoutMs;
    protected ConsistencyLevel consistencyLevel;
    protected String cql;
    protected Object[] bindValuesArr;
    protected Map<String, Object> bindValuesMap;
    protected PreparedStatement pstm;
    protected Statement stm;

    private RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel) {
        this.sessionManager = sessionManager;
        this.permitTimeoutMs = permitTimeoutMs;
        this.consistencyLevel = consistencyLevel;
    }

    private RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, String cql) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.cql = cql;
    }

    public RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, String cql, Object... bindValuesArr) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, cql);
        this.bindValuesArr = bindValuesArr;
    }

    public RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, String cql, Map<String, Object> bindValuesMap) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, cql);
        this.bindValuesMap = bindValuesMap;
    }

    private RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, PreparedStatement pstm) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.pstm = pstm;
    }

    public RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, PreparedStatement pstm, Object... bindValuesArr) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, pstm);
        this.bindValuesArr = bindValuesArr;
    }

    public RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, PreparedStatement pstm,
            Map<String, Object> bindValuesMap) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, pstm);
        this.bindValuesMap = bindValuesMap;
    }

    public RetryFutureCallback(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, Statement stm) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.stm = stm;
    }

    /**
     * Replacement method for {@link #onFailure(Throwable)} that will never receive
     * {@link ExceedMaxAsyncJobsException}.
     * 
     * @param t
     */
    protected abstract void onError(Throwable t);
}
