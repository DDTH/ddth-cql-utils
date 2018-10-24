package com.github.ddth.cql;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;

/**
 * DSE utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0.1
 */
public class DseUtils extends CqlUtils {

    /**
     * Build a new DSE cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @param proxiedUser
     *            DSE allows a user to connect as another user or role, provide the name of the
     *            user/role you want to connect as via this parameter
     * @return
     */
    public static DseCluster newDseCluster(String hostsAndPorts, String username, String password,
            String proxiedUser) {
        return newDseCluster(hostsAndPorts, username, password, proxiedUser, null);
    }

    /**
     * Build a new DSE cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @param proxiedUser
     *            DSE allows a user to connect as another user or role, provide the name of the
     *            user/role you want to connect as via this parameter
     * @param configuration
     * @return
     */
    public static DseCluster newDseCluster(String hostsAndPorts, String username, String password,
            String proxiedUser, Configuration configuration) {
        DseCluster.Builder builder = DseCluster.builder();
        if (!StringUtils.isBlank(username)) {
            AuthProvider authProvider;
            if (StringUtils.isBlank(proxiedUser)) {
                authProvider = new DsePlainTextAuthProvider(username, password);
            } else {
                authProvider = new DsePlainTextAuthProvider(username, password, proxiedUser);
            }
            builder = builder.withAuthProvider(authProvider);
        }
        Collection<InetSocketAddress> contactPointsWithPorts = new HashSet<InetSocketAddress>();
        String[] hostAndPortArr = StringUtils.split(hostsAndPorts, ";, ");
        for (String hostAndPort : hostAndPortArr) {
            String[] tokens = StringUtils.split(hostAndPort, ':');
            String host = tokens[0];
            int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : DEFAULT_CASSANDRA_PORT;
            contactPointsWithPorts.add(new InetSocketAddress(host, port));
        }
        builder = builder.addContactPointsWithPorts(contactPointsWithPorts);

        buildPolicies(configuration, builder);
        buildOptions(configuration, builder);

        DseCluster cluster = builder.build();
        return cluster;
    }

    /**
     * Create a new session for a DSE cluster, initializes it and sets the keyspace
     * to the provided one.
     * 
     * @param cluster
     * @param keyspace
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     * @throws IllegalStateException
     */
    public static DseSession newDseSession(DseCluster cluster, String keyspace) {
        return cluster.connect(StringUtils.isBlank(keyspace) ? null : keyspace);
    }
}