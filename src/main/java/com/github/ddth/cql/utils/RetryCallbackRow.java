package com.github.ddth.cql.utils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.github.ddth.cql.SessionManager;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * A {@link Callback} that retries executing the query on {@link ExceedMaxAsyncJobsException}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public abstract class RetryCallbackRow extends RetryCallback<Row> {

    public RetryCallbackRow(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            String cql, Object... bindValuesArr) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, cql, bindValuesArr);
    }

    public RetryCallbackRow(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            String cql, Map<String, Object> bindValuesMap) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, cql, bindValuesMap);
    }

    public RetryCallbackRow(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            PreparedStatement pstm, Object... bindValuesArr) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, pstm, bindValuesArr);
    }

    public RetryCallbackRow(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            PreparedStatement pstm, Map<String, Object> bindValuesMap) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, pstm, bindValuesMap);
    }

    public RetryCallbackRow(SessionManager sessionManager, long permitTimeoutMs, ConsistencyLevel consistencyLevel,
            Statement<?> stm) {
        super(sessionManager, permitTimeoutMs, consistencyLevel, stm);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doRetry() throws Exception {
        if (stm != null) {
            sessionManager.executeOneAsync(this, permitTimeoutMs, stm, consistencyLevel);
        } else if (pstm != null) {
            if (bindValuesArr != null) {
                sessionManager.executeOneAsync(this, permitTimeoutMs, pstm, consistencyLevel, bindValuesArr);
            } else {
                sessionManager.executeOneAsync(this, permitTimeoutMs, pstm, consistencyLevel, bindValuesMap);
            }
        } else if (!StringUtils.isBlank(cql)) {
            if (bindValuesArr != null) {
                sessionManager.executeOneAsync(this, permitTimeoutMs, cql, consistencyLevel, bindValuesArr);
            } else {
                sessionManager.executeOneAsync(this, permitTimeoutMs, cql, consistencyLevel, bindValuesMap);
            }
        } else {
            throw new IllegalStateException("No query defined to retry!");
        }
    }
}
