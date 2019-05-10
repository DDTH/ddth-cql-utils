package com.github.ddth.cql.utils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.github.ddth.cql.SessionManager;

import java.util.Map;

/**
 * A {@link Callback} that retries executing the query on {@link ExceedMaxAsyncJobsException}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public abstract class RetryCallback<T> implements Callback<T> {
    protected SessionManager sessionManager;
    protected long permitTimeoutMs;
    protected ConsistencyLevel consistencyLevel;
    protected String cql;
    protected Object[] bindValuesArr;
    protected Map<String, Object> bindValuesMap;
    protected PreparedStatement pstm;
    protected Statement<?> stm;
    protected int maxRetries = 3;

    private RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel) {
        this.sessionManager = sessionManager;
        this.permitTimeoutMs = permitTimeoutMs;
        this.consistencyLevel = consistencyLevel;
    }

    private RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            String cql) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.cql = cql;
    }

    public RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            String cql, Object... bindValuesArr) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, cql);
        this.bindValuesArr = bindValuesArr;
    }

    public RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            String cql, Map<String, Object> bindValuesMap) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, cql);
        this.bindValuesMap = bindValuesMap;
    }

    private RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            PreparedStatement pstm) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.pstm = pstm;
    }

    public RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            PreparedStatement pstm, Object... bindValuesArr) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, pstm);
        this.bindValuesArr = bindValuesArr;
    }

    public RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            PreparedStatement pstm, Map<String, Object> bindValuesMap) {
        this(sessionManager, permitTimeoutMs, consistencyLevel, pstm);
        this.bindValuesMap = bindValuesMap;
    }

    public RetryCallback(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            Statement<?> stm) {
        this(sessionManager, permitTimeoutMs, consistencyLevel);
        this.stm = stm;
    }

    /**
     * {@link SessionManager} instance used to retry executing the query.
     *
     * @return
     */
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    /**
     * {@link SessionManager} instance used to retry executing the query.
     *
     * @param sessionManager
     * @return
     */
    public RetryCallback<T> setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    /**
     * Maximum number of milliseconds to wait for an async-job slot to be available.
     *
     * @return
     */
    public long getPermitTimeoutMs() {
        return permitTimeoutMs;
    }

    /**
     * Maximum number of milliseconds to wait for an async-job slot to be available.
     *
     * @param permitTimeoutMs
     * @return
     */
    public RetryCallback<T> getPermitTimeoutMs(long permitTimeoutMs) {
        this.permitTimeoutMs = permitTimeoutMs;
        return this;
    }

    /**
     * {@link ConsistencyLevel} used to retry executing the query.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * {@link ConsistencyLevel} used to retry executing the query.
     *
     * @param consistencyLevel
     * @return
     */
    public RetryCallback<T> setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Max number of retries (default 3).
     *
     * @return
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Max number of retries (default 3).
     *
     * @param maxRetries
     * @return
     */
    public RetryCallback<T> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    protected int numRetries = 0;

    /**
     * Get number of retries so far.
     *
     * @return
     */
    protected int getNumRetries() {
        return numRetries;
    }

    /**
     * Increase number of retries by 1 and return the post-value.
     *
     * @return
     */
    protected int incNumRetries() {
        numRetries++;
        return numRetries;
    }

    /**
     * Reset number of retries to 0.
     *
     * @return
     */
    protected RetryCallback<T> resetNumRetries() {
        numRetries = 0;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFailure(Throwable t) {
        if (t instanceof ExceedMaxAsyncJobsException) {
            if (numRetries >= maxRetries) {
                onError(t);
            } else {
                try {
                    doRetry();
                } catch (Throwable _t) {
                    onError(_t);
                } finally {
                    incNumRetries();
                }
            }
        } else {
            onError(t);
        }
    }

    /**
     * Replacement method for {@link #onFailure(Throwable)} that will never receive
     * {@link ExceedMaxAsyncJobsException}.
     * Sub-class is supposed to implements this method.
     *
     * @param t
     */
    protected abstract void onError(Throwable t);

    /**
     * Sub-class implements this method to do retry logic.
     *
     * @throws Exception
     */
    protected abstract void doRetry() throws Exception;
}
