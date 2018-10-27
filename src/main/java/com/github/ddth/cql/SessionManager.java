package com.github.ddth.cql;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.github.ddth.cql.internal.ClusterIdentifier;
import com.github.ddth.cql.internal.SessionIdentifier;
import com.github.ddth.cql.utils.ExceedMaxAsyncJobsException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Datastax's session manager.
 * 
 * <p>
 * Features:
 * <ul>
 * <li>Cache opened {@link Cluster}s.</li>
 * <li>Cache opened {@link Session}s (per {@link Cluster}).</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Usage:
 * <ul>
 * <li>Create & initialize a {@link SessionManager} instance:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code SessionManager sessionManager = new SessionManager().init();}</li>
 * <li>Obtain a {@link Cluster}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code Cluster cluster = sessionManager.getCluster("host1:port1,host2:port2", "username", "password");}
 * </li>
 * <li>Obtain a {@link Session}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code Session session = sessionManager.getSession("host1:port1,host2:port2", "username", "password", "keyspace");}
 * </li>
 * <li>...do business work with the obtained {@link Cluster} or {@link Session}
 * ...</li>
 * <li>Before existing the application:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code sessionManager.destroy();}</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Best practices:
 * <ul>
 * <li>{@link Cluster} is thread-safe; create a single instance (per target Cassandra cluster), and
 * share it throughout the application.</li>
 * <li>{@link Session} is thread-safe; create a single instance (per target Cassandra cluster), and
 * share it throughout the application (prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * <li>Create {@link Session} without associated keyspace:
 * {@code sessionManager.getSession("host1:port1,host2:port2", "username", "password", null)}
 * (prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SessionManager implements Closeable {

    private Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private Configuration configuration;
    private MetricsOptions metricsOptions;
    private PoolingOptions poolingOptions;
    private ProtocolOptions protocolOptions;
    private QueryOptions queryOptions;
    private SocketOptions socketOptions;
    private ThreadingOptions threadingOptions;

    private AddressTranslator addressTranslator = Policies.defaultAddressTranslator();
    private LoadBalancingPolicy loadBalancingPolicy = Policies.defaultLoadBalancingPolicy();
    private ReconnectionPolicy reconnectionPolicy = Policies.defaultReconnectionPolicy();
    private RetryPolicy retryPolicy = Policies.defaultRetryPolicy();
    private SpeculativeExecutionPolicy speculativeExecutionPolicy = Policies
            .defaultSpeculativeExecutionPolicy();
    private TimestampGenerator timestampGenerator = Policies.defaultTimestampGenerator();

    private int maxSyncJobs = 100;
    private Semaphore asyncSemaphore;
    private ExecutorService asyncExecutor;

    private String defaultHostsAndPorts, defaultUsername, defaultPassword, defaultKeyspace;

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
     * Default hosts and ports used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @return
     * @since 0.4.0
     */
    public String getDefaultHostsAndPorts() {
        return defaultHostsAndPorts;
    }

    /**
     * Default hosts and ports used by {@link #getCluster()} and {@link #getSession()}.
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
     * Default username used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @return
     * @since 0.4.0
     */
    public String getDefaultUsername() {
        return defaultUsername;
    }

    /**
     * Default username used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @param defaultUsername
     * @return
     * @since 0.4.0
     */
    public SessionManager setDefaultUsername(String defaultUsername) {
        this.defaultUsername = defaultUsername;
        return this;
    }

    /**
     * Default password used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @return
     * @since 0.4.0
     */
    public String getDefaultPassword() {
        return defaultPassword;
    }

    /**
     * Default password used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @param defaultPassword
     * @return
     * @since 0.4.0
     */
    public SessionManager setDefaultPassword(String defaultPassword) {
        this.defaultPassword = defaultPassword;
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

    /*----------------------------------------------------------------------*/

    /**
     * Get the configured {@link Configuration} object.
     * 
     * @return
     * @since 0.4.0
     */
    protected Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Gets metrics options.
     * 
     * @return
     * @since 0.3.0
     */
    public MetricsOptions getMetricsOptions() {
        return metricsOptions;
    }

    /**
     * Sets metrics options for new cluster instances.
     * 
     * @param metricsOptions
     * @return
     * @since 0.3.0
     */
    public SessionManager setMetricsOptions(MetricsOptions metricsOptions) {
        this.metricsOptions = metricsOptions;
        return this;
    }

    /**
     * Gets pooling options.
     * 
     * @return
     * @since 0.2.4
     */
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    /**
     * Sets pooling options for new cluster instances.
     * 
     * @param poolingOptions
     * @return
     * @since 0.2.4
     */
    public SessionManager setPoolingOptions(PoolingOptions poolingOptions) {
        this.poolingOptions = poolingOptions;
        return this;
    }

    /**
     * Gets protocol options.
     * 
     * @return
     * @since 0.3.0
     */
    public ProtocolOptions getProtocolOptions() {
        return protocolOptions;
    }

    /**
     * Sets protocol options for new cluster instances.
     * 
     * @param protocolOptions
     * @return
     * @since 0.3.0
     */
    public SessionManager setProtocolOptions(ProtocolOptions protocolOptions) {
        this.protocolOptions = protocolOptions;
        return this;
    }

    /**
     * Gets query options.
     * 
     * @return
     * @since 0.3.0
     */
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    /**
     * Sets query options for new cluster instances.
     * 
     * @param queryOptions
     * @return
     * @since 0.3.0
     */
    public SessionManager setQueryOptions(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
        return this;
    }

    /**
     * Gets socket options.
     * 
     * @return
     * @since 0.3.0
     */
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    /**
     * Sets socket options for new cluster instances.
     * 
     * @param socketOptions
     * @return
     * @since 0.3.0
     */
    public SessionManager setSocketOptions(SocketOptions socketOptions) {
        this.socketOptions = socketOptions;
        return this;
    }

    /**
     * Gets threading options.
     * 
     * @return
     * @since 0.3.0
     */
    public ThreadingOptions getThreadingOptions() {
        return threadingOptions;
    }

    /**
     * Sets threading options for new cluster instances.
     * 
     * @param threadingOptions
     * @return
     * @since 0.3.0
     */
    public SessionManager setThreadingOptions(ThreadingOptions threadingOptions) {
        this.threadingOptions = threadingOptions;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Gets address translator.
     * 
     * @return
     * @since 0.3.0
     */
    public AddressTranslator getAddressTranslator() {
        return addressTranslator;
    }

    /**
     * Sets address translator for new cluster instances.
     * 
     * @param addressTranslator
     * @return
     * @since 0.3.0
     */
    public SessionManager setAddressTranslator(AddressTranslator addressTranslator) {
        this.addressTranslator = addressTranslator;
        return this;
    }

    /**
     * Gets load balancing policies.
     * 
     * @return
     * @since 0.3.0
     */
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    /**
     * Sets load balancing policies for new cluster instances.
     * 
     * @param loadBalancingPolicy
     * @return
     * @since 0.3.0
     */
    public SessionManager setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
        return this;
    }

    /**
     * Gets reconnection policy
     * 
     * @return
     * @since 0.2.4
     */
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    /**
     * Sets reconnection policy for new cluster instance.
     * 
     * @param reconnectionPolicy
     * @return
     * @since 0.2.4
     */
    public SessionManager setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
        return this;
    }

    /**
     * Gets retry policy
     * 
     * @return
     * @since 0.2.4
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets retry policy for new cluster instance.
     * 
     * @param retryPolicy
     * @return
     * @since 0.2.4
     */
    public SessionManager setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    /**
     * Gets speculative execution policies.
     * 
     * @return
     * @since 0.3.0
     */
    public SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
        return speculativeExecutionPolicy;
    }

    /**
     * Sets speculative execution policies for new cluster instances.
     * 
     * @param speculativeExecutionPolicy
     * @return
     * @since 0.3.0
     */
    public SessionManager setSpeculativeExecutionPolicy(
            SpeculativeExecutionPolicy speculativeExecutionPolicy) {
        this.speculativeExecutionPolicy = speculativeExecutionPolicy;
        return this;
    }

    /**
     * Gets timestamp generator.
     * 
     * @return
     * @since 0.3.0
     */
    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }

    /**
     * Sets timestamp generator for new cluster instances.
     * 
     * @param timestampGenerator
     * @return
     * @since 0.3.0
     */
    public SessionManager setTimestampGenerator(TimestampGenerator timestampGenerator) {
        this.timestampGenerator = timestampGenerator;
        return this;
    }

    /** Map {cluster_info -> Cluster} */
    private LoadingCache<ClusterIdentifier, Cluster> clusterCache;

    /** Map {cluster_info -> {session_info -> Session}} */
    private LoadingCache<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>> sessionCache;

    /**
     * Create a {@link Cluster} instance.
     * 
     * @param ci
     * @return
     * @since 0.4.0
     */
    protected Cluster createCluster(ClusterIdentifier ci) {
        return CqlUtils.newCluster(ci.hostsAndPorts, ci.username, ci.password, getConfiguration());
    }

    /**
     * Create a {@link Session} instance.
     * 
     * @param cluster
     * @param keyspace
     * @return
     */
    protected Session createSession(Cluster cluster, String keyspace) {
        return CqlUtils.newSession(cluster, keyspace);
    }

    public SessionManager init() {
        clusterCache = CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
                .removalListener((RemovalListener<ClusterIdentifier, Cluster>) entry -> {
                    ClusterIdentifier key = entry.getKey();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Removing cluster from cache: " + key);
                    }
                    try {
                        sessionCache.invalidate(key);
                    } finally {
                        entry.getValue().closeAsync();
                    }
                }).build(new CacheLoader<ClusterIdentifier, Cluster>() {
                    @Override
                    public Cluster load(ClusterIdentifier key) {
                        return createCluster(key);
                    }
                });

        /** Map {cluster_info -> {session_info -> Session}} */
        sessionCache = CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
                .removalListener(
                        (RemovalListener<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>>) entry -> {
                            ClusterIdentifier key = entry.getKey();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Removing session cache for cluster: " + key);
                            }
                            entry.getValue().invalidateAll();
                        })
                .build(new CacheLoader<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>>() {
                    @Override
                    public LoadingCache<SessionIdentifier, Session> load(
                            ClusterIdentifier clusterKey) {
                        LoadingCache<SessionIdentifier, Session> _sessionCache = CacheBuilder
                                .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
                                .removalListener(
                                        (RemovalListener<SessionIdentifier, Session>) entry -> {
                                            SessionIdentifier key = entry.getKey();
                                            if (LOGGER.isDebugEnabled()) {
                                                LOGGER.debug("Removing session from cache: " + key);
                                            }
                                            entry.getValue().closeAsync();
                                        })
                                .build(new CacheLoader<SessionIdentifier, Session>() {
                                    @Override
                                    public Session load(SessionIdentifier sessionKey)
                                            throws Exception {
                                        try {
                                            Cluster cluster = clusterCache.get(sessionKey);
                                            return createSession(cluster, sessionKey.keyspace);
                                        } catch (IllegalStateException e) {
                                            /*
                                             * since v0.2.1: rebuild {@code Cluster}
                                             * when {@code java.lang.IllegalStateException}
                                             * occurred
                                             */
                                            LOGGER.warn(e.getMessage(), e);
                                            clusterCache.invalidate(sessionKey);
                                            Cluster cluster = clusterCache.get(sessionKey);
                                            return createSession(cluster, sessionKey.keyspace);
                                        }
                                    }
                                });
                        return _sessionCache;
                    }
                });

        asyncExecutor = Executors.newCachedThreadPool();
        asyncSemaphore = new Semaphore(maxSyncJobs, true);

        Policies.Builder polBuilder = Policies.builder();
        if (this.addressTranslator == null) {
            addressTranslator = Policies.defaultAddressTranslator();
        }
        polBuilder.withAddressTranslator(addressTranslator);
        if (this.loadBalancingPolicy == null) {
            loadBalancingPolicy = Policies.defaultLoadBalancingPolicy();
        }
        polBuilder.withLoadBalancingPolicy(loadBalancingPolicy);
        if (this.reconnectionPolicy == null) {
            reconnectionPolicy = new ExponentialReconnectionPolicy(1000, 10 * 1000);
        }
        polBuilder.withReconnectionPolicy(reconnectionPolicy);
        if (this.retryPolicy == null) {
            retryPolicy = Policies.defaultRetryPolicy();
        }
        polBuilder.withRetryPolicy(retryPolicy);
        if (this.speculativeExecutionPolicy == null) {
            speculativeExecutionPolicy = Policies.defaultSpeculativeExecutionPolicy();
        }
        polBuilder.withSpeculativeExecutionPolicy(speculativeExecutionPolicy);
        if (this.timestampGenerator == null) {
            timestampGenerator = Policies.defaultTimestampGenerator();
        }
        polBuilder.withTimestampGenerator(timestampGenerator);
        Policies policies = polBuilder.build();

        Configuration.Builder confBuilder = Configuration.builder();
        if (this.metricsOptions != null) {
            confBuilder.withMetricsOptions(metricsOptions);
        }
        if (this.poolingOptions == null) {
            poolingOptions = new PoolingOptions();
            poolingOptions.setConnectionsPerHost(HostDistance.REMOTE, 1, 1);
            poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 1, 2);
            poolingOptions.setHeartbeatIntervalSeconds(10);
        }
        confBuilder.withPoolingOptions(poolingOptions);
        if (this.protocolOptions != null) {
            confBuilder.withProtocolOptions(protocolOptions);
        }
        if (this.queryOptions == null) {
            queryOptions = new QueryOptions();
            queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
            queryOptions.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
            queryOptions.setFetchSize(1024);
            queryOptions.setDefaultIdempotence(false);
        }
        confBuilder.withQueryOptions(queryOptions);
        if (this.socketOptions != null) {
            confBuilder.withSocketOptions(socketOptions);
        }
        if (this.threadingOptions != null) {
            confBuilder.withThreadingOptions(threadingOptions);
        }
        confBuilder.withPolicies(policies);
        this.configuration = confBuilder.build();

        return this;
    }

    public void destroy() {
        try {
            if (clusterCache != null) {
                clusterCache.invalidateAll();
                clusterCache = null;
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        try {
            if (sessionCache != null) {
                sessionCache.invalidateAll();
                sessionCache = null;
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

    /**
     * Obtain a Cassandra cluster instance.
     * 
     * @param key
     * @return
     * @since 0.4.0
     */
    synchronized protected Cluster getCluster(ClusterIdentifier key) {
        Cluster cluster;
        try {
            cluster = clusterCache.get(key);
            if (cluster.isClosed()) {
                LOGGER.info("Cluster [" + cluster + "] was closed, obtaining a new one...");
                clusterCache.invalidate(key);
                cluster = clusterCache.get(key);
            }
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
        return cluster;
    }

    /**
     * Obtain a Cassandra cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link CqlUtils#DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @return
     */
    public Cluster getCluster(String hostsAndPorts, String username, String password) {
        return getCluster(ClusterIdentifier.getInstance(hostsAndPorts, username, password));
    }

    /**
     * Obtain a Cassandra cluster instance using {@link #defaultHostsAndPorts},
     * {@link #defaultUsername} and {@link #defaultPassword}.
     * 
     * @return
     * @since 0.4.0
     */
    public Cluster getCluster() {
        return getCluster(defaultHostsAndPorts, defaultPassword, defaultPassword);
    }

    /*----------------------------------------------------------------------*/

    /**
     * Obtain a Cassandra session instance.
     * 
     * <p>
     * The existing session instance will be returned if such existed.
     * </p>
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param keyspace
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     */
    public Session getSession(String hostsAndPorts, String username, String password,
            String keyspace) {
        return getSession(hostsAndPorts, username, password, keyspace, false);
    }

    /**
     * Obtain a Cassandra session instance.
     * 
     * @param si
     * @param forceNew
     * @return
     */
    synchronized protected Session getSession(SessionIdentifier si, boolean forceNew) {
        /*
         * Since 0.2.6: refresh cluster cache before obtaining the session to
         * avoid exception
         * "You may have used a PreparedStatement that was created with another Cluster instance"
         */
        Cluster cluster = getCluster(si);
        if (cluster == null) {
            return null;
        }

        try {
            LoadingCache<SessionIdentifier, Session> cacheSessions = sessionCache.get(si);
            Session existingSession = cacheSessions.getIfPresent(si);
            if (existingSession != null && existingSession.isClosed()) {
                LOGGER.info("Session [" + existingSession + "] was closed, obtaining a new one...");
                cacheSessions.invalidate(si);
                return cacheSessions.get(si);
            }
            if (forceNew) {
                if (existingSession != null) {
                    cacheSessions.invalidate(si);
                }
                return cacheSessions.get(si);
            }
            return existingSession != null ? existingSession : cacheSessions.get(si);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
    }

    /**
     * Obtain a Cassandra session instance.
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param keyspace
     * @param forceNew
     *            force create new session instance (the existing one, if any,
     *            will be closed)
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     * @since 0.2.2
     */
    public Session getSession(String hostsAndPorts, String username, String password,
            String keyspace, boolean forceNew) {
        return getSession(
                SessionIdentifier.getInstance(hostsAndPorts, username, password, keyspace),
                forceNew);
    }

    /**
     * Obtain a Cassandra session instance using {@link #defaultHostsAndPorts},
     * {@link #defaultUsername}, {@link #defaultPassword} and {@link #defaultKeyspace}.
     * 
     * @return
     * @since 0.4.0
     */
    public Session getSession() {
        return getSession(false);
    }

    /**
     * Obtain a Cassandra session instance using {@link #defaultHostsAndPorts},
     * {@link #defaultUsername}, {@link #defaultPassword} and {@link #defaultKeyspace}.
     * 
     * @param forceNew
     *            force create new session instance (the existing one, if any,
     *            will be closed)
     * @return
     * @since 0.4.0
     */
    public Session getSession(boolean forceNew) {
        return getSession(defaultHostsAndPorts, defaultUsername, defaultPassword, defaultKeyspace,
                forceNew);
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
        return CqlUtils.prepareStatement(getSession(), cql);
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
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param cql
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(String, Object...)}
     */
    public void executeNonSelect(String cql, Object... bindValues) {
        CqlUtils.executeNonSelect(getSession(), cql, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param cql
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(String, Map)}
     */
    public void executeNonSelect(String cql, Map<String, Object> bindValues) {
        CqlUtils.executeNonSelect(getSession(), cql, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(String, ConsistencyLevel, Object...)}
     */
    public void executeNonSelect(String cql, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
        CqlUtils.executeNonSelect(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(String, ConsistencyLevel, Map)}
     */
    public void executeNonSelect(String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
        CqlUtils.executeNonSelect(getSession(), cql, consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param stm
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(PreparedStatement, Object...)}
     */
    public void executeNonSelect(PreparedStatement stm, Object... bindValues) {
        CqlUtils.executeNonSelect(getSession(), stm, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param stm
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(PreparedStatement, Map)}
     */
    public void executeNonSelect(PreparedStatement stm, Map<String, Object> bindValues) {
        CqlUtils.executeNonSelect(getSession(), stm, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use
     *             {@link #execute(PreparedStatement, ConsistencyLevel, Object...)}
     */
    public void executeNonSelect(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
        CqlUtils.executeNonSelect(getSession(), stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(PreparedStatement, ConsistencyLevel, Map)}
     */
    public void executeNonSelect(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
        CqlUtils.executeNonSelect(getSession(), stm, consistencyLevel, bindValues);
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
    public ResultSet execute(String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public ResultSet execute(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
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
    public ResultSet execute(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public ResultSet execute(Statement stm) {
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
    public ResultSet execute(Statement stm, ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm.setConsistencyLevel(consistencyLevel);
            }
        }
        return getSession().execute(stm);
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
    public Row executeOne(String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public Row executeOne(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
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
    public Row executeOne(PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public Row executeOne(Statement stm) {
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
    public Row executeOne(Statement stm, ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm.setConsistencyLevel(consistencyLevel);
            }
        }
        return getSession().execute(stm).one();
    }

    /*----------------------------------------------------------------------*/

    private FutureCallback<ResultSet> wrapCallbackResultSet(FutureCallback<ResultSet> callback) {
        return new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, String cql, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, String cql,
            Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, PreparedStatement stm,
            Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, PreparedStatement stm,
            Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @since 0.4.0.2
     */
    public void executeAsync(FutureCallback<ResultSet> callback, Statement stm) {
        executeAsync(callback, stm, null);
    }

    /**
     * Async-execute a query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @since 0.4.0.2
     */
    public void executeAsync(FutureCallback<ResultSet> callback, Statement stm,
            ConsistencyLevel consistencyLevel) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                if (consistencyLevel != null) {
                    if (consistencyLevel == ConsistencyLevel.SERIAL
                            || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                        stm.setSerialConsistencyLevel(consistencyLevel);
                    } else {
                        stm.setConsistencyLevel(consistencyLevel);
                    }
                }
                Futures.addCallback(getSession().executeAsync(stm), wrapCallbackResultSet(callback),
                        asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs, String cql,
            Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, cql, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs, String cql,
            Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, cql, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, cql, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, cql, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            PreparedStatement stm, Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, stm, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            PreparedStatement stm, Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, stm, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            PreparedStatement stm, ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, stm, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, stm, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackResultSet(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            Statement stm) throws InterruptedException {
        executeAsync(callback, permitTimeoutMs, stm, null);
    }

    /**
     * Async-execute a query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            Statement stm, ConsistencyLevel consistencyLevel) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeAsync(callback, stm, consistencyLevel);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                if (consistencyLevel != null) {
                    if (consistencyLevel == ConsistencyLevel.SERIAL
                            || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                        stm.setSerialConsistencyLevel(consistencyLevel);
                    } else {
                        stm.setConsistencyLevel(consistencyLevel);
                    }
                }
                Futures.addCallback(getSession().executeAsync(stm), wrapCallbackResultSet(callback),
                        asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    private FutureCallback<ResultSet> wrapCallbackRow(FutureCallback<Row> callback) {
        return new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, String cql, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, String cql,
            Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, PreparedStatement stm,
            Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, PreparedStatement stm,
            Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @since 0.4.0.2
     */
    public void executeOneAsync(FutureCallback<Row> callback, Statement stm) {
        executeOneAsync(callback, stm, null);
    }

    /**
     * Async-execute a query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param stm
     * @param consistencyLevel
     * @since 0.4.0.2
     */
    public void executeOneAsync(FutureCallback<Row> callback, Statement stm,
            ConsistencyLevel consistencyLevel) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                if (consistencyLevel != null) {
                    if (consistencyLevel == ConsistencyLevel.SERIAL
                            || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                        stm.setSerialConsistencyLevel(consistencyLevel);
                    } else {
                        stm.setConsistencyLevel(consistencyLevel);
                    }
                }
                Futures.addCallback(getSession().executeAsync(stm), wrapCallbackRow(callback),
                        asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, String cql,
            Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, cql, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, String cql,
            Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, cql, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), cql, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, cql, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues)
            throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, cql, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), cql, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs,
            PreparedStatement stm, Object... bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, stm, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs,
            PreparedStatement stm, Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, stm, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeAsync(getSession(), stm, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs,
            PreparedStatement stm, ConsistencyLevel consistencyLevel, Object... bindValues)
            throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, stm, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs,
            PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, stm, consistencyLevel, bindValues);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeAsync(getSession(), stm, consistencyLevel, bindValues),
                        wrapCallbackRow(callback), asyncExecutor);
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
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, Statement stm)
            throws InterruptedException {
        executeOneAsync(callback, permitTimeoutMs, stm, null);
    }

    /**
     * Async-execute a query.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param stm
     * @param consistencyLevel
     * @throws InterruptedException
     * @since 0.4.0.2
     */
    public void executeOneAsync(FutureCallback<Row> callback, long permitTimeoutMs, Statement stm,
            ConsistencyLevel consistencyLevel) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeOneAsync(callback, stm, consistencyLevel);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                if (consistencyLevel != null) {
                    if (consistencyLevel == ConsistencyLevel.SERIAL
                            || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                        stm.setSerialConsistencyLevel(consistencyLevel);
                    } else {
                        stm.setConsistencyLevel(consistencyLevel);
                    }
                }
                Futures.addCallback(getSession().executeAsync(stm), wrapCallbackRow(callback),
                        asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param bStm
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(Statement)}
     */
    public void executeBatchNonSelect(BatchStatement bStm) {
        CqlUtils.executeBatchNonSelect(getSession(), bStm);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param bStm
     * @param consistencyLevel
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(Statement, ConsistencyLevel)}
     */
    public void executeBatchNonSelect(BatchStatement bStm, ConsistencyLevel consistencyLevel) {
        CqlUtils.executeBatchNonSelect(getSession(), bStm, consistencyLevel);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param statements
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #executeBatch(Statement...)}
     */
    public void executeBatchNonSelect(Statement... statements) {
        CqlUtils.executeBatchNonSelect(getSession(), statements);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param statements
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #executeBatch(ConsistencyLevel, Statement...)}
     */
    public void executeBatchNonSelect(ConsistencyLevel consistencyLevel, Statement... statements) {
        CqlUtils.executeBatchNonSelect(getSession(), consistencyLevel, statements);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param batchType
     * @param statements
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #executeBatch(BatchStatement.Type, Statement...)}
     */
    public void executeBatchNonSelect(BatchStatement.Type batchType, Statement... statements) {
        CqlUtils.executeBatchNonSelect(getSession(), batchType, statements);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use
     *             {@link #executeBatch(ConsistencyLevel, BatchStatement.Type, Statement...)}
     */
    public void executeBatchNonSelect(ConsistencyLevel consistencyLevel,
            BatchStatement.Type batchType, Statement... statements) {
        CqlUtils.executeBatchNonSelect(getSession(), consistencyLevel, batchType, statements);
    }

    /**
     * Execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param bStm
     * @return
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(Statement)}
     */
    public ResultSet executeBatch(BatchStatement bStm) {
        return CqlUtils.executeBatch(getSession(), bStm);
    }

    /**
     * Execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param bStm
     * @param consistencyLevel
     * @return
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #execute(Statement, ConsistencyLevel)}
     */
    public ResultSet executeBatch(BatchStatement bStm, ConsistencyLevel consistencyLevel) {
        return CqlUtils.executeBatch(getSession(), bStm, consistencyLevel);
    }

    /**
     * Execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param statements
     * @return
     * @since 0.4.0
     */
    public ResultSet executeBatch(Statement... statements) {
        return CqlUtils.executeBatch(getSession(), statements);
    }

    /**
     * Execute a batch statement.
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
    public ResultSet executeBatch(ConsistencyLevel consistencyLevel, Statement... statements) {
        return CqlUtils.executeBatch(getSession(), consistencyLevel, statements);
    }

    /**
     * Execute a batch statement.
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
    public ResultSet executeBatch(BatchStatement.Type batchType, Statement... statements) {
        return CqlUtils.executeBatch(getSession(), batchType, statements);
    }

    /**
     * Execute a batch statement.
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
    public ResultSet executeBatch(ConsistencyLevel consistencyLevel, BatchStatement.Type batchType,
            Statement... statements) {
        return CqlUtils.executeBatch(getSession(), consistencyLevel, batchType, statements);
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param bStm
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #executeAsync(FutureCallback, Statement)}
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, BatchStatement bStm) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), bStm),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param bStm
     * @param consistencyLevel
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use
     *             {@link #executeAsync(FutureCallback, Statement, ConsistencyLevel)}
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, BatchStatement bStm,
            ConsistencyLevel consistencyLevel) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeBatchAsync(getSession(), bStm, consistencyLevel),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param statements
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, Statement... statements) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param statements
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback,
            ConsistencyLevel consistencyLevel, Statement... statements) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeBatchAsync(getSession(), consistencyLevel, statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param batchType
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param statements
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, BatchStatement.Type batchType,
            Statement... statements) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), batchType, statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param batchType
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param statements
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback,
            ConsistencyLevel consistencyLevel, BatchStatement.Type batchType,
            Statement... statements) {
        if (!asyncSemaphore.tryAcquire()) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), consistencyLevel,
                        batchType, statements), wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param bStm
     * @throws InterruptedException
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use {@link #executeAsync(FutureCallback, long, Statement)}
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            BatchStatement bStm) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, bStm);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), bStm),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param bStm
     * @param consistencyLevel
     * @throws InterruptedException
     * @since 0.4.0
     * @deprecated since 0.4.0.2 use
     *             {@link #executeAsync(FutureCallback, long, Statement, ConsistencyLevel)}
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            BatchStatement bStm, ConsistencyLevel consistencyLevel) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, bStm, consistencyLevel);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeBatchAsync(getSession(), bStm, consistencyLevel),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            Statement... statements) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, statements);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, Statement... statements)
            throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, consistencyLevel, statements);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(
                        CqlUtils.executeBatchAsync(getSession(), consistencyLevel, statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param batchType
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            BatchStatement.Type batchType, Statement... statements) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, batchType, statements);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), batchType, statements),
                        wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Async-execute a batch statement.
     * 
     * <p>
     * The default session (obtained via {@link #getSession()} is used to execute the query.
     * </p>
     * 
     * @param consistencyLevel
     * @param batchType
     * @param callback
     *            {@link ExceedMaxAsyncJobsException} will be passed to
     *            {@link FutureCallback#onFailure(Throwable)} if number of async-jobs exceeds
     *            {@link #getMaxAsyncJobs()}.
     * @param permitTimeoutMs
     *            wait up to this milliseconds before failing the execution with
     *            {@code ExceedMaxAsyncJobsException}
     * @param statements
     * @throws InterruptedException
     * @since 0.4.0
     */
    public void executeBatchAsync(FutureCallback<ResultSet> callback, long permitTimeoutMs,
            ConsistencyLevel consistencyLevel, BatchStatement.Type batchType,
            Statement... statements) throws InterruptedException {
        if (permitTimeoutMs <= 0) {
            executeBatchAsync(callback, consistencyLevel, batchType, statements);
        } else if (!asyncSemaphore.tryAcquire(permitTimeoutMs, TimeUnit.MILLISECONDS)) {
            callback.onFailure(new ExceedMaxAsyncJobsException(maxSyncJobs));
        } else {
            try {
                Futures.addCallback(CqlUtils.executeBatchAsync(getSession(), consistencyLevel,
                        batchType, statements), wrapCallbackResultSet(callback), asyncExecutor);
            } catch (Exception e) {
                asyncSemaphore.release();
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
