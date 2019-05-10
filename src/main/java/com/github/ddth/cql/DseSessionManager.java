package com.github.ddth.cql;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.github.ddth.cql.internal.SessionIdentifier;

/**
 * Utility class to manage {@link DseSession} instances.
 *
 * <p>{@link DseSessionManager} extends {@link SessionManager}. Thus, it can be a drop-in replacement for {@link SessionManager}.</p>
 *
 * <p>
 * Usage:
 * <ul>
 * <li>Create & initialize a {@link DseSessionManager} instance:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code DseSessionManager sessionManager = new DseSessionManager().init();}</li>
 * <li>Obtain a {@link DseSession}:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code DseSession session = sessionManager.getSession("host1:port1,host2:port2", "keyspace");}
 * </li>
 * <li>...do business work with the obtained {@link DseSession}
 * ...</li>
 * <li>Before existing the application:<br/>
 * &nbsp;&nbsp;&nbsp;&nbsp;{@code sessionManager.destroy();}</li>
 * </ul>
 * </p>
 *
 * <p>
 * Performance practices:
 * <ul>
 * <li>{@link DseSession} is thread-safe; create a single instance (per target Cassandra cluster) and share it throughout the application.</li>
 * <li>Create {@link DseSession} without default keyspace:
 * {@code sessionManager.getSession("host1:port1,host2:port2", null)} and
 * prefix table name with keyspace in all queries, e.g.
 * {@code SELECT * FROM my_keyspace.my_table}).</li>
 * </ul>
 * </p>
 *
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class DseSessionManager extends SessionManager {

    /**
     * Default authorization-id used by {@link #getSession()}.
     *
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0, this method always return {@code null}
     */
    public String getDefaultAuthorizationId() {
        return null;
    }

    /**
     * Default authorization-id used by {@link #getSession()}.
     *
     * @param defaultAuthorizationId
     * @return
     * @since 0.4.0
     * @deprecated since v1.0.0
     */
    public DseSessionManager getDefaultAuthorizationId(String defaultAuthorizationId) {
        return this;
    }

    /**
     * Create a new instance of {@link DseSession}.
     *
     * @param si
     * @return
     * @since 1.0.0
     */
    @Override
    protected DseSession createCqlSession(SessionIdentifier si) {
        DseSessionBuilder builder = DseSession.builder();
        initBuilder(builder, si);
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSessionManager init() {
        super.init();
        return this;
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
    public DseSession getSession(String hostsAndPorts, String keyspace) {
        return getSession(hostsAndPorts, keyspace, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DseSession getSession(String hostsAndPorts, String keyspace, boolean forceNew) {
        return getSession(SessionIdentifier.getInstance(hostsAndPorts, keyspace), forceNew);
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
    @Override
    public DseSession getSession(boolean forceNew) {
        return getSession(getDefaultHostsAndPorts(), getDefaultKeyspace(), forceNew);
    }
}
