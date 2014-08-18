package com.github.ddth.cql;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Cassandra utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraUtils {
    public final static int DEFAULT_CASSANDRA_PORT = 9042;

    /**
     * Builds a new Cassandra cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @return
     */
    public static Cluster newCluster(String hostsAndPorts, String username, String password) {
        Cluster.Builder builder = Cluster.builder();
        if (!StringUtils.isBlank(username)) {
            builder = builder.withCredentials(username, password);
        }
        Collection<InetSocketAddress> contactPointsWithPorts = new HashSet<InetSocketAddress>();
        String[] hostAndPortArr = StringUtils.split(hostsAndPorts, ", ");
        for (String hostAndPort : hostAndPortArr) {
            String[] tokens = StringUtils.split(hostAndPort, ':');
            String host = tokens[0];
            int port = tokens.length > 1 ? Integer.parseInt(tokens[1]) : DEFAULT_CASSANDRA_PORT;
            contactPointsWithPorts.add(new InetSocketAddress(host, port));
        }
        builder = builder.addContactPointsWithPorts(contactPointsWithPorts);
        Cluster cluster = builder.build();
        return cluster;
    }

    /**
     * Creates a new session for a cluster, initializes it and sets the keyspace
     * to the provided one.
     * 
     * @param cluster
     * @param keyspace
     * @return
     * @throws NoHostAvailableException
     * @throws AuthenticationException
     * @throws IllegalStateException
     */
    public static Session newSession(Cluster cluster, String keyspace) {
        Session session = cluster.connect(keyspace);
        return session;
    }

    /**
     * Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     */
    public static void executeNonSelect(Session session, String cql, Object... bindValues) {
        executeNonSelect(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            Object... bindValues) {
        BoundStatement bstm = stm.bind();
        if (bindValues != null && bindValues.length > 0) {
            bstm.bind(bindValues);
        }
        session.execute(bstm);
    }

    /**
     * Executes a SELECT query and returns results.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, String cql, Object... bindValues) {
        return execute(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a SELECT query and returns results.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, PreparedStatement stm, Object... bindValues) {
        BoundStatement bstm = stm.bind();
        if (bindValues != null && bindValues.length > 0) {
            bstm.bind(bindValues);
        }
        return session.execute(bstm);
    }

    /**
     * Executes a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, String cql, Object... bindValues) {
        return executeOne(session, session.prepare(cql), bindValues);
    }

    /**
     * Executes a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, PreparedStatement stm, Object... bindValues) {
        ResultSet rs = execute(session, stm, bindValues);
        return rs != null ? rs.one() : null;
    }
}