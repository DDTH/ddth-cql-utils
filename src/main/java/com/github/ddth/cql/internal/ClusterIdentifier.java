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
public class ClusterIdentifier {

    private static LoadingCache<String[], ClusterIdentifier> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(3600, TimeUnit.SECONDS)
            .build(new CacheLoader<String[], ClusterIdentifier>() {
                @Override
                public ClusterIdentifier load(String[] key) {
                    return new ClusterIdentifier(key[0], key[1], key[2], key[3]);
                }
            });

    /**
     * Helper method to get an instance of {@link ClusterIdentifier}.
     * 
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @return
     */
    public static ClusterIdentifier getInstance(String hostsAndPorts, String username,
            String password) {
        return getInstance(hostsAndPorts, username, password, null);
    }

    /**
     * Helper method to get an instance of {@link ClusterIdentifier}.
     *
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param authorizationId
     *            DSE's proxied user/role id (used with DSE only)
     * @return
     */
    public static ClusterIdentifier getInstance(String hostsAndPorts, String username,
            String password, String authorizationId) {
        String[] key = { hostsAndPorts, username, password, authorizationId };
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public final String hostsAndPorts, username, password, authorizationId;

    /**
     * Construct a new {@link ClusterIdentifier}
     * 
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     */
    protected ClusterIdentifier(String hostsAndPorts, String username, String password) {
        this(hostsAndPorts, username, password, null);
    }

    /**
     * Construct a new {@link ClusterIdentifier}
     * 
     * @param hostsAndPorts
     *            example {@code "localhost:9042,host2,host3:19042"}
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @param authorizationId
     *            DSE's proxied user/role id (used with DSE only)
     */
    protected ClusterIdentifier(String hostsAndPorts, String username, String password,
            String authorizationId) {
        this.hostsAndPorts = hostsAndPorts;
        this.username = username;
        this.password = password;
        this.authorizationId = authorizationId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(hostsAndPorts).append(username);
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
        if (obj instanceof ClusterIdentifier) {
            ClusterIdentifier other = (ClusterIdentifier) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(hostsAndPorts, other.hostsAndPorts).append(username, other.username)
                    .append(password, other.password)
                    .append(authorizationId, other.authorizationId);
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
        tsb.append("hostsAndPorts", hostsAndPorts).append("username", username)
                .append("password", "*").append("authorizationId", authorizationId);
        return tsb.toString();
    }
}
