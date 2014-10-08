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
                                    Cluster cluster = clusterCache.get(sessionKey);
                                    return CqlUtils.newSession(cluster, sessionKey.keyspace);
                                }

                            });
                    return _sessionCache;
                }
            });

    public void init() {
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
        SessionIdentifier key = new SessionIdentifier(hostsAndPorts, username, password, keyspace);
        try {
            return sessionCache.get(key).get(key);
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
