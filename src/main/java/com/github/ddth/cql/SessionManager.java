package com.github.ddth.cql;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
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
public class SessionManager {

    private Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private PoolingOptions poolingOptions;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;

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
     * Sets pooling options for new cluster instance.
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
                        Cluster cluster = entry.getValue();
                        cluster.closeAsync();
                    }
                }
            }).build(new CacheLoader<ClusterIdentifier, Cluster>() {
                @Override
                public Cluster load(ClusterIdentifier key) throws Exception {
                    return CqlUtils.newCluster(key.hostsAndPorts, key.username, key.password,
                            poolingOptions, reconnectionPolicy, retryPolicy);
                }
            });

    /** Map {cluster_info -> {session_info -> Session}} */
    private LoadingCache<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>> sessionCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(
                    new RemovalListener<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>>() {
                        @Override
                        public void onRemoval(
                                RemovalNotification<ClusterIdentifier, LoadingCache<SessionIdentifier, Session>> entry) {
                            ClusterIdentifier key = entry.getKey();
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Removing session cache for cluster: " + key);
                            }
                            LoadingCache<SessionIdentifier, Session> _sessionCache = entry
                                    .getValue();
                            _sessionCache.invalidateAll();
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
                                    Session session = entry.getValue();
                                    session.closeAsync();
                                }
                            }).build(new CacheLoader<SessionIdentifier, Session>() {
                                @Override
                                public Session load(SessionIdentifier sessionKey) throws Exception {
                                    try {
                                        Cluster cluster = clusterCache.get(sessionKey);
                                        return CqlUtils.newSession(cluster, sessionKey.keyspace);
                                    } catch (IllegalStateException e) {
                                        /*
                                         * since v0.2.1: rebuild `Cluster` when
                                         * `java.lang.IllegalStateException`
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
        return this;
    }

    public void destroy() {
        try {
            if (clusterCache != null) {
                clusterCache.invalidateAll();
            }
        } catch (Exception e) {
        }

        try {
            if (sessionCache != null) {
                sessionCache.invalidateAll();
            }
        } catch (Exception e) {
        }
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
    public Cluster getCluster(final String hostsAndPorts, final String username,
            final String password) {
        ClusterIdentifier key = new ClusterIdentifier(hostsAndPorts, username, password);
        Cluster cluster;
        try {
            cluster = clusterCache.get(key);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException(t);
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
     *            force create new session instance (and close the existing one,
     *            if any)
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     * @since 0.2.2
     */
    public Session getSession(final String hostsAndPorts, final String username,
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
            if (forceNew) {
                cacheSessions.invalidate(key);
            }
            return cacheSessions.get(key);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
