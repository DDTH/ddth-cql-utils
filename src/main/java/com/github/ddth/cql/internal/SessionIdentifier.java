package com.github.ddth.cql.internal;

import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.github.ddth.cql.CqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * For internal use.
 *
 * @author Thanh Ba Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SessionIdentifier {
    private static LoadingCache<String[], SessionIdentifier> cache;

    static {
        cache = CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS).build(new CacheLoader<>() {
            @Override
            public SessionIdentifier load(String[] key) {
                return new SessionIdentifier(key[0], key[1]);
            }
        });
    }

    /**
     * Helper method to get an instance of {@link SessionIdentifier}.
     *
     * @param hostsAndPorts example {@code "localhost:9042,host2,host3:19042"}
     * @param keyspace      keyspace to connect to
     * @return
     */
    public static SessionIdentifier getInstance(String hostsAndPorts, String keyspace) {
        String[] key = { hostsAndPorts, keyspace };
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public final String hostsAndPorts;
    public final String keyspace;

    /**
     * Construct a new {@link SessionIdentifier}
     *
     * @param hostsAndPorts example {@code "localhost:9042,host2,host3:19042"}
     * @param keyspace      keyspace to connect to
     */
    protected SessionIdentifier(String hostsAndPorts, String keyspace) {
        this.hostsAndPorts = hostsAndPorts;
        this.keyspace = keyspace;
    }

    private List<InetSocketAddress> contactPointsWithPorts = null;

    /**
     * Parse {@link #hostsAndPorts} to list of {@link InetSocketAddress}.
     *
     * @return
     * @since 1.0.0
     */
    synchronized public List<InetSocketAddress> parseHostsAndPorts() {
        if (contactPointsWithPorts == null) {
            contactPointsWithPorts = new ArrayList<>();
            String[] hostAndPortArr = StringUtils.split(hostsAndPorts, ";, ");
            if (hostAndPortArr != null) {
                for (String hostAndPort : hostAndPortArr) {
                    String[] tokens = StringUtils.split(hostAndPort, ':');
                    String host = tokens[0];
                    int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : CqlUtils.DEFAULT_CASSANDRA_PORT;
                    contactPointsWithPorts.add(new InetSocketAddress(host, port));
                }
            }
        }
        return Collections.unmodifiableList(contactPointsWithPorts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(hostsAndPorts).append(keyspace);
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
            SessionIdentifier that = (SessionIdentifier) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(hostsAndPorts, that.hostsAndPorts).append(keyspace, that.keyspace);
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
        tsb.append("hostsAndPorts", hostsAndPorts).append("keyspace", keyspace);
        return tsb.toString();
    }
}
