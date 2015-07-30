package com.github.ddth.cql;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
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

    private LoadingCache<ClusterIdentifier, Cluster> clusterCache = CacheBuilder.newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<ClusterIdentifier, Cluster>() {
                @Override
                public void onRemoval(RemovalNotification<ClusterIdentifier, Cluster> entry) {
                    ClusterIdentifier key = entry.getKey();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Removing cluster from cache: " + key);
                    }
                    sessionCache.invalidate(key);
                    Cluster cluster = entry.getValue();
                    cluster.close();
                }
            }).build(new CacheLoader<ClusterIdentifier, Cluster>() {
                @Override
                public Cluster load(ClusterIdentifier key) throws Exception {
                    return CqlUtils.newCluster(key.hostsAndPorts, key.username, key.password);
                }
            });

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
                                    try {
                                        Session session = entry.getValue();
                                        session.close();
                                    } catch (Exception e) {
                                        LOGGER.warn(e.getMessage(), e);
                                    }
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
            throw new RuntimeException(e);
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
        SessionIdentifier key = new SessionIdentifier(hostsAndPorts, username, password, keyspace);
        try {
            LoadingCache<SessionIdentifier, Session> cacheSessions = sessionCache.get(key);
            if (forceNew) {
                cacheSessions.invalidate(key);
            }
            return cacheSessions.get(key);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
