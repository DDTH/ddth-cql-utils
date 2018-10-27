package com.github.ddth.cql.utils;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.github.ddth.cql.SessionManager;
import com.google.common.util.concurrent.FutureCallback;

/**
 * A {@link FutureCallback} that retries executing the query on {@link ExceedMaxAsyncJobsException}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0.2
 */
public abstract class RetryFutureCallbackResultSet extends RetryFutureCallback<ResultSet> {

    public RetryFutureCallbackResultSet(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, String cql, Object... bindValuesArr) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, cql, bindValuesArr);
    }

    public RetryFutureCallbackResultSet(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, String cql, Map<String, Object> bindValuesMap) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, cql, bindValuesMap);
    }

    public RetryFutureCallbackResultSet(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, PreparedStatement pstm, Object... bindValuesArr) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, pstm, bindValuesArr);
    }

    public RetryFutureCallbackResultSet(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, PreparedStatement pstm,
            Map<String, Object> bindValuesMap) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, pstm, bindValuesMap);
    }

    public RetryFutureCallbackResultSet(SessionManager sessionManager, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, Statement stm) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, stm);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFailure(Throwable t) {
        if (t instanceof ExceedMaxAsyncJobsException) {
            try {
                if (stm != null) {
                    sessionManager.executeAsync(this, permitTimeoutMs, stm, consistencyLevel);
                } else if (pstm != null) {
                    if (bindValuesArr != null) {
                        sessionManager.executeAsync(this, permitTimeoutMs, pstm, consistencyLevel,
                                bindValuesArr);
                    } else {
                        sessionManager.executeAsync(this, permitTimeoutMs, pstm, consistencyLevel,
                                bindValuesMap);
                    }
                } else if (!StringUtils.isBlank(cql)) {
                    if (bindValuesArr != null) {
                        sessionManager.executeAsync(this, permitTimeoutMs, cql, consistencyLevel,
                                bindValuesArr);
                    } else {
                        sessionManager.executeAsync(this, permitTimeoutMs, cql, consistencyLevel,
                                bindValuesMap);
                    }
                } else {
                    onError(new Exception("No query is defined to retry!"));
                }
            } catch (Throwable _t) {
                onError(_t);
            }
        } else {
            onError(t);
        }
    }
}
