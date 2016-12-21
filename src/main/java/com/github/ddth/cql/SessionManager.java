package com.github.ddth.cql;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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
 * {@code Cluster cluster = sessionManager.getSession("host1:port1,host2:port2", "username", "password", "keyspace");}
 * </li>
 * <li>...do business work with the obtained {@link Cluster} or {@link Session}
 * ...</li>
 * <li>Before existing the application:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code sessionManager.destroy();}</li>
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

    /*----------------------------------------------------------------------*/

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
    private LoadingCache<ClusterIdentifier, Cluster> clusterCache = CacheBuilder.newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<ClusterIdentifier, Cluster>() {
                @Override
                public void onRemoval(RemovalNotification<ClusterIdentifier, Cluster> entry) {
                    ClusterIdentifier key = entry.getKey();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Removing cluster from cache: " + key);
                    }
                    try {
                        sessionCache.invalidate(key);
                    } finally {
                        entry.getValue().closeAsync();
                    }
                }
            }).build(new CacheLoader<ClusterIdentifier, Cluster>() {
                @Override
                public Cluster load(ClusterIdentifier key) throws Exception {
                    return CqlUtils.newCluster(key.hostsAndPorts, key.username, key.password,
                            configuration);
                }
            });

    /** Map {cluster_info -> {session_info -> Session}} */
    private LoadingCache<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>> sessionCache = CacheBuilder
            .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS).removalListener(
                    new RemovalListener<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>>() {
                        @Override
                        public void onRemoval(
                                RemovalNotification<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>> entry) {
                            ClusterIdentifier key = entry.getKey();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Removing session cache for cluster: " + key);
                            }
                            entry.getValue().invalidateAll();
                        }
                    })
            .build(new CacheLoader<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>>() {
                @Override
                public LoadingCache<SessionIdentifier, Session> load(ClusterIdentifier clusterKey)
                        throws Exception {
                    LoadingCache<SessionIdentifier, Session> _sessionCache = CacheBuilder
                            .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
                            .removalListener(new RemovalListener<SessionIdentifier, Session>() {
                                @Override
                                public void onRemoval(
                                        RemovalNotification<SessionIdentifier, Session> entry) {
                                    SessionIdentifier key = entry.getKey();
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Removing session from cache: " + key);
                                    }
                                    entry.getValue().closeAsync();
                                }
                            }).build(new CacheLoader<SessionIdentifier, Session>() {
                                @Override
                                public Session load(SessionIdentifier sessionKey) throws Exception {
                                    try {
                                        Cluster cluster = clusterCache.get(sessionKey);
                                        return CqlUtils.newSession(cluster, sessionKey.keyspace);
                                    } catch (IllegalStateException e) {
                                        /*
                                         * since v0.2.1: rebuild {@code Cluster}
                                         * when {@code
                                         * java.lang.IllegalStateException}
                                         * occurred
                                         */
                                        LOGGER.warn(e.getMessage(), e);
                                        clusterCache.invalidate(sessionKey);
                                        Cluster cluster = clusterCache.get(sessionKey);
                                        return CqlUtils.newSession(cluster, sessionKey.keyspace);
                                    }
                                }
                            });
                    return _sessionCache;
                }
            });

    public SessionManager init() {
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
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.3.0
     */
    @Override
    public void close() throws IOException {
        destroy();
    }

    /**
     * Obtains a Cassandra cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @return
     */
    synchronized public Cluster getCluster(final String hostsAndPorts, final String username,
            final String password) {
        ClusterIdentifier key = new ClusterIdentifier(hostsAndPorts, username, password);
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
     * Obtains a Cassandra session instance.
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
    public Session getSession(final String hostsAndPorts, final String username,
            final String password, final String keyspace) {
        return getSession(hostsAndPorts, username, password, keyspace, false);
    }

    /**
     * Obtains a Cassandra session instance.
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param keyspace
     * @param forceNew
     *            force create new session instance (the existing one, if any,
     *            will be closed by calling {@link Session#closeAsync()})
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     * @since 0.2.2
     */
    synchronized public Session getSession(final String hostsAndPorts, final String username,
            final String password, final String keyspace, final boolean forceNew) {
        /*
         * Since 0.2.6: refresh cluster cache before obtaining the session to
         * avoid exception
         * "You may have used a PreparedStatement that was created with another Cluster instance"
         */
        Cluster cluster = getCluster(hostsAndPorts, username, password);
        if (cluster == null) {
            return null;
        }

        SessionIdentifier key = new SessionIdentifier(hostsAndPorts, username, password, keyspace);
        try {
            LoadingCache<SessionIdentifier, Session> cacheSessions = sessionCache.get(key);
            Session existingSession = cacheSessions.getIfPresent(key);
            if (existingSession != null && existingSession.isClosed()) {
                LOGGER.info("Session [" + existingSession + "] was closed, obtaining a new one...");
                cacheSessions.invalidate(key);
                return cacheSessions.get(key);
            }
            if (forceNew) {
                if (existingSession != null) {
                    cacheSessions.invalidate(key);
                }
                return cacheSessions.get(key);
            }
            return existingSession != null ? existingSession : cacheSessions.get(key);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
    }

}
