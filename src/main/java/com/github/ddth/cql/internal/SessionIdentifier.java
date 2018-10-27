package com.github.ddth.cql.internal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * For internal use.
 * 
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SessionIdentifier extends ClusterIdentifier {

    private static LoadingCache<String[], SessionIdentifier> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .build(new CacheLoader<String[], SessionIdentifier>() {
                @Override
                public SessionIdentifier load(String[] key) {
                    return new SessionIdentifier(key[0], key[1], key[2], key[3], key[4]);
                }
            });

    /**
     * Helper method to get an instance of {@link SessionIdentifier}.
     *
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param keyspace
     *            keyspace to connect to
     * @return
     */
    public static SessionIdentifier getInstance(String hostsAndPorts, String username,
            String password, String keyspace) {
        return getInstance(hostsAndPorts, username, password, null, keyspace);
    }

    /**
     * Helper method to get an instance of {@link SessionIdentifier}.
     *
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param authorizationId
     *            DSE's proxied user/role id (used with DSE only)
     * @param keyspace
     *            keyspace to connect to
     * @return
     */
    public static SessionIdentifier getInstance(String hostsAndPorts, String username,
            String password, String authorizationId, String keyspace) {
        String[] key = { hostsAndPorts, username, password, authorizationId, keyspace };
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public final String keyspace;

    /**
     * Construct a new {@link SessionIdentifier}
     * 
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param keyspace
     *            keyspace to connect to
     */
    protected SessionIdentifier(String hostsAndPorts, String username, String password,
            String keyspace) {
        // TODO cache SessionIdentifier
        this(hostsAndPorts, username, password, null, keyspace);
    }

    /**
     * Construct a new {@link SessionIdentifier}
     * 
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param authorizationId
     *            DSE's proxied user/role id (used with DSE only)
     * @param keyspace
     *            keyspace to connect to
     */
    protected SessionIdentifier(String hostsAndPorts, String username, String password,
            String authorizationId, String keyspace) {
        super(hostsAndPorts, username, password, authorizationId);
        this.keyspace = keyspace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(keyspace).append(super.hashCode());
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof SessionIdentifier) {
            SessionIdentifier other = (SessionIdentifier) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.appendSuper(super.equals(obj));
            eq.append(keyspace, other.keyspace);
            return eq.isEquals();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("keyspace", keyspace).appendSuper(super.toString());
        return tsb.toString();
    }
}
