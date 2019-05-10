package com.github.ddth.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalListener;
import com.github.ddth.cql.internal.SessionIdentifier;
import com.github.ddth.cql.utils.Callback;
import com.github.ddth.cql.utils.ExceedMaxAsyncJobsException;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Utility class to manage {@link CqlSession} instances.
 *
 * <p>
 * Usage:
 * <ul>
 * <li>Create & initialize a {@link SessionManager} instance:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code SessionManager sessionManager = new SessionManager().init();}</li>
 * <li>Obtain a {@link CqlSession}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code CqlSession session = sessionManager.getSession("host1:port1,host2:port2", "keyspace");}
 * </li>
 * <li>...do business work with the obtained {@link CqlSession}
 * ...</li>
 * <li>Before existing the application:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code sessionManager.destroy();}</li>
 * </ul>
 * </p>
 *
 * <p>
 * Performance practices:
 * <ul>
 * <li>{@link CqlSession} is thread-safe; create a single instance (per target Cassandra cluster) and share it throughout the application.</li>
 * <li>Create {@link CqlSession} without default keyspace:
 * {@code sessionManager.getSession("host1:port1,host2:port2", null)} and
 * prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * </ul>
 * </p>
 *
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SessionManager implements Closeable {

    private Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private int maxSyncJobs = 100;
    private Semaphore asyncSemaphore;
    private ExecutorService asyncExecutor;

    private String defaultHostsAndPorts, defaultKeyspace;
    private DriverConfigLoader configLoader;

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("maxSyncJobs", getMaxAsyncJobs()).append("defaultHostsAndPorts", getDefaultHostsAndPorts())
                .append("defaultKeyspace", getDefaultKeyspace());
        return tsb.toString();
    }

    /*----------------------------------------------------------------------*/

    /**
     * Max number of allowed async-jobs. Then number of async-jobs exceeds this number,
     * {@link ExceedMaxAsyncJobsException} is thrown.
     *
     * @return
     * @since 0.4.0
     */
    public int getMaxAsyncJobs() {
        return maxSyncJobs;
    }

    /**
     * Max number of allowed async-jobs. Then number of async-jobs exceeds this number,
     * {@link ExceedMaxAsyncJobsException} is thrown.
     *
     * @param maxSyncJobs
     * @return
     * @since 0.4.0
     */
    public SessionManager setMaxAsyncJobs(int maxSyncJobs) {
        this.maxSyncJobs = maxSyncJobs;
        return this;
    }

    /**
     * Default hosts and ports used by {@link #getSession()}.
     *
     * @return
     * @since 0.4.0
     */
    public String getDefaultHostsAndPorts() {
        return defaultHostsAndPorts;
    }

    /**
     * Default hosts and ports used by {@link #getSession()}.
     *
     * @param defaultHostsAndPorts
     * @return
     * @since 0.4.0
     */
    public SessionManager setDefaultHostsAndPorts(String defaultHostsAndPorts) {
        this.defaultHostsAndPorts = defaultHostsAndPorts;
        return this;
    }

    /**
     * Default username used by {@link #getSession()}.
     *
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0, this method always return {@code null}
     */
    public String getDefaultUsername() {
        return null;
    }

    /**
     * Default username used by {@link #getSession()}.
     *
     * @param defaultUsername
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0
     */
    public SessionManager setDefaultUsername(String defaultUsername) {
        return this;
    }

    /**
     * Default password used by {@link #getSession()}.
     *
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0, this method always return {@code null}
     */
    public String getDefaultPassword() {
        return null;
    }

    /**
     * Default password used by {@link #getSession()}.
     *
     * @param defaultPassword
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0
     */
    public SessionManager setDefaultPassword(String defaultPassword) {
        return this;
    }

    /**
     * Default keyspace used by {@link #getSession()}.
     *
     * @return
     * @since 0.4.0
     */
    public String getDefaultKeyspace() {
        return defaultKeyspace;
    }

    /**
     * Default keyspace used by {@link #getSession()}.
     *
     * @param defaultKeyspace
     * @return
     * @since 0.4.0
     */
    public SessionManager setDefaultKeyspace(String defaultKeyspace) {
        this.defaultKeyspace = defaultKeyspace;
        return this;
    }

    /**
     * If not {@code null}, session-manager uses this configuration to build {@link CqlSession}.
     *
     * @return
     * @since 1.0.0
     */
    public DriverConfigLoader getConfigLoader() {
        return configLoader;
    }

    /**
     * If not {@code null}, session-manager uses this configuration to build {@link CqlSession}.
     *
     * @param configLoader
     * @return
     * @since 1.0.0
     */
    public SessionManager setConfigLoader(DriverConfigLoader configLoader) {
        this.configLoader = configLoader;
        return this;
    }

    /**
     * This method builds a file-{@link DriverConfigLoader} and passes it to
     * {@link #setConfigLoader(DriverConfigLoader)}.
     *
     * @param configFile
     * @return
     */
    public SessionManager setConfig(File configFile) {
        return setConfigLoader(DriverConfigLoader.fromFile(configFile));
    }

    /**
     * This method builds a url-{@link DriverConfigLoader} and passes it to
     * {@link #setConfigLoader(DriverConfigLoader)}.
     *
     * @param configFile
     * @return
     */
    public SessionManager setConfig(URL configFile) {
        return setConfigLoader(DriverConfigLoader.fromUrl(configFile));
    }

    /*----------------------------------------------------------------------*/

    /**
     * Map {session_info -> CqlSession}
     */
    private LoadingCache<SessionIdentifier, CqlSession> cqlSessionCache;

    /**
     * Initialize the session builder.
     *
     * @param builder
     * @since 1.0.0
     */
    protected void initBuilder(SessionBuilder<?, ?> builder, SessionIdentifier si) {
        if (configLoader != null) {
            builder.withConfigLoader(configLoader);
        }
        List<InetSocketAddress> contactPoints = si.parseHostsAndPorts();
        if (contactPoints != null && contactPoints.size() > 0) {
            builder.addContactPoints(contactPoints);
        }
        if (!StringUtils.isBlank(si.keyspace)) {
            builder.withKeyspace(si.keyspace);
        }
    }

    /**
     * Create a new instance of {@link CqlSession}.
     *
     * @param si
     * @return
     * @since 1.0.0
     */
    protected CqlSession createCqlSession(SessionIdentifier si) {
        CqlSessionBuilder builder = CqlSession.builder();
        initBuilder(builder, si);
        return builder.build();
    }

    public SessionManager init() {
        cqlSessionCache = CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
                .removalListener((RemovalListener<SessionIdentifier, CqlSession>) entry -> {
                    SessionIdentifier key = entry.getKey();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Removing cluster from cache: " + key);
                    }
                    CqlSession session = entry.getValue();
                    if (!session.isClosed()) {
                        session.closeAsync();
                    }
                }).build(new CacheLoader<>() {
                    @Override
                    public CqlSession load(SessionIdentifier key) {
                        return createCqlSession(key);
                    }
                });
        asyncExecutor = Executors.newCachedThreadPool();
        asyncSemaphore = new Semaphore(maxSyncJobs, true);
        return this;
    }

    public void destroy() {
        try {
            if (cqlSessionCache != null) {
                cqlSessionCache.invalidateAll();
                cqlSessionCache = null;
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        try {
            if (asyncExecutor != null) {
                asyncExecutor.shutdown();
                asyncExecutor = null;
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.3.0
     */
    @Override
    public void close() {
        destroy();
    }

    /*----------------------------------------------------------------------*/

    /**
     * Obtain a Cassandra session instance.
     *
     * <p>
     * The existing session instance will be returned if such exists.
     * </p>
     *
     * @param hostsAndPorts
     * @param keyspace
     * @return
     */
    public CqlSession getSession(String hostsAndPorts, String keyspace) {
        return getSession(hostsAndPorts, keyspace, false);
    }

    /**
     * Obtain a Cassandra session instance.
     *
     * @param hostsAndPorts
     * @param keyspace
     * @param forceNew      force create new session instance (the existing one, if any,
     *                      will be closed)
     * @return
     * @since 0.2.2
     */
    public CqlSession getSession(String hostsAndPorts, String keyspace, boolean forceNew) {
        return getSession(SessionIdentifier.getInstance(hostsAndPorts, keyspace), forceNew);
    }

    /**
     * Obtain a Cassandra session instance using {@link #getDefaultHostsAndPorts()},
     * {@link #getDefaultUsername()}, {@link #getDefaultPassword()} and
     * {@link #getDefaultKeyspace()}.
     *
     * @return
     * @since 0.4.0
     */
    public CqlSession getSession() {
        return getSession(false);
    }

    /**
     * Obtain a Cassandra session instance using {@link #getDefaultHostsAndPorts()},
     * {@link #getDefaultUsername()}, {@link #getDefaultPassword()} and
     * {@link #getDefaultKeyspace()}.
     *
     * @param forceNew force create new session instance (the existing one, if any,
     *                 will be closed)
     * @return
     * @since 0.4.0
     */
    public CqlSession getSession(boolean forceNew) {
        return getSession(defaultHostsAndPorts, defaultKeyspace, forceNew);
    }

    /**
     * Obtain a Cassandra session instance.
     *
     * @param si
     * @param forceNew
     * @return
     */
    synchronized protected CqlSession getSession(SessionIdentifier si, boolean forceNew) {
        try {
            if (forceNew) {
                cqlSessionCache.invalidate(si);
            }
            CqlSession session = cqlSessionCache.get(si);
            if (session != null && session.isClosed()) {
                LOGGER.info("Session [" + session + "] was closed, obtaining a new one...");
                cqlSessionCache.invalidate(si);
                return getSession(si, forceNew);
            }
            return session;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Prepare a CQL query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to prepare the CQL query.
     * </p>
     *
     * @param cql
     * @return
     * @since 0.4.0
     */
    public PreparedStatement prepareStatement(String cql) {
        return getSession().prepare(cql);
    }

    /**
     * Bind values to a {@link PreparedStatement}.
     *
     * @param stm
     * @param values
     * @return
     * @since 0.4.0
     */
    public BoundStatement bindValues(PreparedStatement stm, Object... values) {
        return CqlUtils.bindValues(stm, values);
    }

    /**
     * Bind values to a {@link PreparedStatement}.
     *
     * @param stm
     * @param values
     * @return
     * @since 0.4.0
     */
    public BoundStatement bindValues(PreparedStatement stm, Map<String, Object> values) {
        return CqlUtils.bindValues(stm, values);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(String cql, Object... bindValues) {
        return CqlUtils.execute(getSession(), cql, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(String cql, Map<String, Object> bindValues) {
        return CqlUtils.execute(getSession(), cql, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(String cql, ConsistencyLevel consistencyLevel, Object... bindValues) {
        return CqlUtils.execute(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(String cql, ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return CqlUtils.execute(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(PreparedStatement stm, Object... bindValues) {
        return CqlUtils.execute(getSession(), stm, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(PreparedStatement stm, Map<String, Object> bindValues) {
        return CqlUtils.execute(getSession(), stm, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(PreparedStatement stm, ConsistencyLevel consistencyLevel, Object... bindValues) {
        return CqlUtils.execute(getSession(), stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public ResultSet execute(PreparedStatement stm, ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return CqlUtils.execute(getSession(), stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public ResultSet execute(Statement<?> stm) {
        return execute(stm, null);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public ResultSet execute(Statement<?> stm, ConsistencyLevel consistencyLevel) {
        return CqlUtils.execute(getSession(), stm, consistencyLevel);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(String cql, Object... bindValues) {
        return CqlUtils.executeOne(getSession(), cql, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(String cql, Map<String, Object> bindValues) {
        return CqlUtils.executeOne(getSession(), cql, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(String cql, ConsistencyLevel consistencyLevel, Object... bindValues) {
        return CqlUtils.executeOne(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(String cql, ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return CqlUtils.executeOne(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(PreparedStatement stm, Object... bindValues) {
        return CqlUtils.executeOne(getSession(), stm, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(PreparedStatement stm, Map<String, Object> bindValues) {
        return CqlUtils.executeOne(getSession(), stm, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(PreparedStatement stm, ConsistencyLevel consistencyLevel, Object... bindValues) {
        return CqlUtils.executeOne(getSession(), stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.4.0
     */
    public Row executeOne(PreparedStatement stm, ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return CqlUtils.executeOne(getSession(), stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public Row executeOne(Statement<?> stm) {
        return executeOne(stm, null);
    }

    /**
     * Execute a SELECT query and returns just one row.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public Row executeOne(Statement<?> stm, ConsistencyLevel consistencyLevel) {
        return CqlUtils.executeOne(getSession(), stm, consistencyLevel);
    }

    /*----------------------------------------------------------------------*/
    private <T> void completeAsync(CompletionStage<T> stage, Callback<T> callback) {
        stage.whenCompleteAsync((resultSet, error) -> {
            if (error != null) {
                if (error instanceof ExecutionException) {
                    callback.onFailure(error.getCause());
                } else {
                    callback.onFailure(error);
                }
            } else {
                callback.onSuccess(resultSet);
            }
        });
    }

    private boolean tryAccquireSemaphore(long timeoutMs, Callback<?> callback) throws InterruptedException {
        if (asyncSemaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
            return true;
        }
        if (callback == null) {
            throw new ExceedMaxAsyncJobsException(maxSyncJobs);
        }
        callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        return false;
    }

    /*----------------------------------------------------------------------*/
    private Callback<AsyncResultSet> wrapCallbackResultSet(Callback<AsyncResultSet> callback) {
        return new Callback<>() {
            @Override
            public void onSuccess(AsyncResultSet result) {
                try {
                    callback.onSuccess(result);
                } finally {
                    asyncSemaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    callback.onFailure(t);
                } finally {
                    asyncSemaphore.release();
                }
            }
        };
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, String cql, Object... bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, String cql, Map<String, Object> bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, String cql, ConsistencyLevel consistencyLevel,
            Object... bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, PreparedStatement stm, Object... bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, PreparedStatement stm, Map<String, Object> bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback {@link ExceedMaxAsyncJobsException} will be passed to
     *                 {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                 {@link #getMaxAsyncJobs()}.
     * @param stm
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeAsync(Callback<AsyncResultSet> callback, Statement<?> stm)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, null);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeAsync(Callback<AsyncResultSet> callback, Statement<?> stm, ConsistencyLevel consistencyLevel)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeAsync(callback, 0, stm, consistencyLevel);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, String cql, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, String cql,
            Map<String, Object> bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, PreparedStatement stm,
            Object... bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, PreparedStatement stm,
            Map<String, Object> bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0.2
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, Statement<?> stm)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, stm, null);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0.2
     */
    public void executeAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, Statement<?> stm,
            ConsistencyLevel consistencyLevel) throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel),
                        wrapCallbackResultSet(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    private Callback<AsyncResultSet> wrapCallbackRow(Callback<Row> callback) {
        return new Callback<>() {
            @Override
            public void onSuccess(AsyncResultSet result) {
                try {
                    callback.onSuccess(result.one());
                } finally {
                    asyncSemaphore.release();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    callback.onFailure(t);
                } finally {
                    asyncSemaphore.release();
                }
            }
        };
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, String cql, Object... bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, String cql, Map<String, Object> bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, String cql, ConsistencyLevel consistencyLevel,
            Object... bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, PreparedStatement stm, Object... bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, PreparedStatement stm, Map<String, Object> bindValues)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback {@link ExceedMaxAsyncJobsException} will be passed to
     *                 {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                 {@link #getMaxAsyncJobs()}.
     * @param stm
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeOneAsync(Callback<Row> callback, Statement<?> stm)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, null);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeOneAsync(Callback<Row> callback, Statement<?> stm, ConsistencyLevel consistencyLevel)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeOneAsync(callback, 0, stm, consistencyLevel);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, String cql, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, String cql,
            Map<String, Object> bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, prepareStatement(cql), null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, prepareStatement(cql), consistencyLevel, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, PreparedStatement stm,
            Object... bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, PreparedStatement stm,
            Map<String, Object> bindValues) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, stm, null, bindValues);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0.2
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, Statement<?> stm)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeOneAsync(callback, permitTimeoutMs, stm, null);
    }

    /**
     * Async-execute a query.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0.2
     */
    public void executeOneAsync(Callback<Row> callback, long permitTimeoutMs, Statement<?> stm,
            ConsistencyLevel consistencyLevel) throws InterruptedException, ExceedMaxAsyncJobsException {
        if (tryAccquireSemaphore(permitTimeoutMs, callback)) {
            try {
                completeAsync(CqlUtils.executeAsync(getSession(), stm, consistencyLevel), wrapCallbackRow(callback));
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param statements
     * @return
     * @since 0.4.0
     */
    public ResultSet executeBatch(Statement<?>... statements) {
        return CqlUtils.executeBatch(getSession(), statements);
    }

    /**
     * Execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param statements
     * @return
     * @since 0.4.0
     */
    public ResultSet executeBatch(ConsistencyLevel consistencyLevel, Statement<?>... statements) {
        return CqlUtils.executeBatch(getSession(), consistencyLevel, statements);
    }

    /**
     * Execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param batchType
     * @param statements
     * @return
     * @since 0.4.0
     */
    public ResultSet executeBatch(BatchType batchType, Statement<?>... statements) {
        return CqlUtils.executeBatch(getSession(), batchType, statements);
    }

    /**
     * Execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @return
     * @since 0.4.0
     */
    public ResultSet executeBatch(ConsistencyLevel consistencyLevel, BatchType batchType, Statement<?>... statements) {
        return CqlUtils.executeBatch(getSession(), consistencyLevel, batchType, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param statements
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, Statement<?>... statements)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeBatchAsync(callback, 0, null, null, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param statements
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, ConsistencyLevel consistencyLevel,
            Statement<?>... statements) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeBatchAsync(callback, 0, consistencyLevel, null, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param batchType
     * @param callback   {@link ExceedMaxAsyncJobsException} will be passed to
     *                   {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                   {@link #getMaxAsyncJobs()}.
     * @param statements
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, BatchType batchType, Statement<?>... statements)
            throws ExceedMaxAsyncJobsException, InterruptedException {
        executeBatchAsync(callback, 0, null, batchType, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param batchType
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param statements
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, ConsistencyLevel consistencyLevel,
            BatchType batchType, Statement<?>... statements) throws ExceedMaxAsyncJobsException, InterruptedException {
        executeBatchAsync(callback, 0, consistencyLevel, batchType, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, Statement<?>... statements)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeBatchAsync(callback, permitTimeoutMs, null, null, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, Statement<?>... statements)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeBatchAsync(callback, permitTimeoutMs, consistencyLevel, null, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param batchType
     * @param callback        {@link ExceedMaxAsyncJobsException} will be passed to
     *                        {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                        {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs wait up to this milliseconds before failing the execution with
     *                        {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs, BatchType batchType,
            Statement<?>... statements) throws InterruptedException, ExceedMaxAsyncJobsException {
        executeBatchAsync(callback, permitTimeoutMs, null, batchType, statements);
    }

    /**
     * Async-execute a batch of statements.
     *
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     *
     * @param consistencyLevel
     * @param batchType
     * @param callback         {@link ExceedMaxAsyncJobsException} will be passed to
     *                         {@link Callback#onFailure(Throwable)} if number of async-jobs exceeds
     *                         {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs  wait up to this milliseconds before failing the execution with
     *                         {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @throws ExceedMaxAsyncJobsException If number of running jobs exceeds a threshold <i>and</i> {@code callback} is
     *                                     {@code null}.
     * @since 0.4.0
     */
    public void executeBatchAsync(Callback<AsyncResultSet> callback, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, BatchType batchType, Statement<?>... statements)
            throws InterruptedException, ExceedMaxAsyncJobsException {
        executeAsync(callback, permitTimeoutMs, CqlUtils.buildBatch(batchType, statements), consistencyLevel);
    }
}
