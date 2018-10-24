package com.github.ddth.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.github.ddth.cql.internal.ClusterIdentifier;
import com.github.ddth.cql.internal.SessionIdentifier;

/**
 * Datastax's session manager for DSE.
 * 
 * <p>
 * Features:
 * <ul>
 * <li>Cache opened {@link DseCluster}s.</li>
 * <li>Cache opened {@link DseSession}s (per {@link DseCluster}).</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Usage:
 * <ul>
 * <li>Create & initialize a {@link DseSessionManager} instance:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code SessionManager sessionManager = new SessionManager().init();}</li>
 * <li>Obtain a {@link DseCluster}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code DseCluster cluster = sessionManager.getCluster("host1:port1,host2:port2", "username", "password", "authorizationId");}
 * </li>
 * <li>Obtain a {@link DseCluster}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;
 * {@code DseSession session = sessionManager.getSession("host1:port1,host2:port2", "username", "password", "authorizationId", "keyspace");}
 * </li>
 * <li>...do business work with the obtained {@link DseCluster} or {@link DseSession}
 * ...</li>
 * <li>Before existing the application:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code sessionManager.destroy();}</li>
 * </ul>
 * </p>
 * 
 * <p>
 * {@code authorizationId}: DSE allows a user to connect as another user or role, provide the name
 * of the user/role you want to connect as via this parameter.
 * </p>
 * 
 * <p>
 * Best practices:
 * <ul>
 * <li>{@link DseCluster} is thread-safe; create a single instance (per target DSE cluster), and
 * share it throughout the application.</li>
 * <li>{@link DseSession} is thread-safe; create a single instance (per target DSE cluster), and
 * share it throughout the application (prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * <li>Create {@link DseSession} without associated keyspace:
 * {@code sessionManager.getSession("host1:port1,host2:port2", "username", "password", "authorizationId", null)}
 * (prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class DseSessionManager extends SessionManager {

    private String defaultAuthorizationId;

    /**
     * Default authorization-id used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @return
     * @since 0.4.0
     */
    public String getDefaultAuthorizationId() {
        return defaultAuthorizationId;
    }

    /**
     * Default authorization-id used by {@link #getCluster()} and {@link #getSession()}.
     * 
     * @param defaultAuthorizationId
     * @return
     * @since 0.4.0
     */
    public DseSessionManager getDefaultAuthorizationId(String defaultAuthorizationId) {
        this.defaultAuthorizationId = defaultAuthorizationId;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DseCluster createCluster(ClusterIdentifier ci) {
        return DseUtils.newDseCluster(ci.hostsAndPorts, ci.username, ci.password,
                ci.authorizationId, getConfiguration());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DseSession createSession(Cluster cluster, String keyspace) {
        return DseUtils.newDseSession((DseCluster) cluster, keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSessionManager init() {
        super.init();
        return this;
    }

    /**
     * Obtain a DSE cluster instance.
     * 
     * @param key
     * @return
     */
    @Override
    synchronized protected DseCluster getCluster(ClusterIdentifier key) {
        return (DseCluster) super.getCluster(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseCluster getCluster(String hostsAndPorts, String username, String password) {
        return getCluster(hostsAndPorts, username, password, null);
    }

    /**
     * Obtain a DSE cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @param authorizationId
     *            DSE allows a user to connect as another user or role, provide the name of the
     *            user/role you want to connect as via this parameter
     * @return
     */
    public DseCluster getCluster(String hostsAndPorts, String username, String password,
            String authorizationId) {
        return getCluster(
                ClusterIdentifier.getInstance(hostsAndPorts, username, password, authorizationId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseCluster getCluster() {
        return getCluster(getDefaultHostsAndPorts(), getDefaultPassword(), getDefaultPassword(),
                defaultAuthorizationId);
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized protected DseSession getSession(SessionIdentifier si, boolean forceNew) {
        return (DseSession) super.getSession(si, forceNew);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSession getSession(String hostsAndPorts, String username, String password,
            String keyspace) {
        return getSession(hostsAndPorts, username, password, null, keyspace, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSession getSession(String hostsAndPorts, String username, String password,
            String keyspace, boolean forceNew) {
        return getSession(hostsAndPorts, username, password, null, keyspace, forceNew);
    }

    /**
     * Obtain a DSE session instance.
     * 
     * <p>
     * The existing session instance will be returned if such existed.
     * </p>
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param authorizationId
     *            DSE allows a user to connect as another user or role, provide the name of the
     *            user/role you want to connect as via this parameter
     * @param keyspace
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     */
    public DseSession getSession(String hostsAndPorts, String username, String password,
            String authorizationId, String keyspace) {
        return getSession(hostsAndPorts, username, password, authorizationId, keyspace, false);
    }

    /**
     * Obtain a DSE session instance.
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param authorizationId
     *            DSE allows a user to connect as another user or role, provide the name of the
     *            user/role you want to connect as via this parameter
     * @param keyspace
     * @param forceNew
     *            force create new session instance (the existing one, if any,
     *            will be closed by calling {@link Session#closeAsync()})
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     */
    public DseSession getSession(String hostsAndPorts, String username, String password,
            String authorizationId, String keyspace, boolean forceNew) {
        return getSession(SessionIdentifier.getInstance(hostsAndPorts, username, password,
                authorizationId, keyspace), forceNew);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSession getSession() {
        return getSession(false);
    }

    /**
     * {@inheritDoc}
     */
    public DseSession getSession(boolean forceNew) {
        return getSession(getDefaultHostsAndPorts(), getDefaultUsername(), getDefaultPassword(),
                defaultAuthorizationId, getDefaultKeyspace(), forceNew);
    }

    /*----------------------------------------------------------------------*/
}
