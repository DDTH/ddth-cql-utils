package com.github.ddth.cql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.MetricsOptions;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.TimestampGenerator;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

/**
 * Cassandra utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 * @since 0.2.0 renamed to CqlUtils
 */
public class CqlUtils {
    public final static int DEFAULT_CASSANDRA_PORT = 9042;

    /**
     * Build a new Cassandra cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     *            username to authenticate against Cassandra cluster
     * @param password
     *            password to authenticate against Cassandra cluster
     * @return
     */
    public static Cluster newCluster(String hostsAndPorts, String username, String password) {
        return newCluster(hostsAndPorts, username, password, null);
    }

    /**
     * Build configuration's policies.
     * 
     * @param conf
     * @param builder
     * @since 0.3.0
     */
    protected static void buildPolicies(Configuration conf, Cluster.Builder builder) {
        Policies policies = conf != null ? conf.getPolicies() : null;
        AddressTranslator at = policies != null ? policies.getAddressTranslator() : null;
        if (at != null) {
            builder.withAddressTranslator(at);
        }
        LoadBalancingPolicy lbp = policies != null ? policies.getLoadBalancingPolicy() : null;
        if (lbp != null) {
            builder.withLoadBalancingPolicy(lbp);
        }
        ReconnectionPolicy rnp = policies != null ? policies.getReconnectionPolicy() : null;
        if (rnp != null) {
            builder.withReconnectionPolicy(rnp);
        }
        RetryPolicy rp = policies != null ? policies.getRetryPolicy() : null;
        if (rp != null) {
            builder.withRetryPolicy(rp);
        }
        SpeculativeExecutionPolicy sep = policies != null ? policies.getSpeculativeExecutionPolicy()
                : null;
        if (sep != null) {
            builder.withSpeculativeExecutionPolicy(sep);
        }
        TimestampGenerator tg = policies != null ? policies.getTimestampGenerator() : null;
        if (tg != null) {
            builder.withTimestampGenerator(tg);
        }
    }

    /**
     * 
     * @param conf
     * @param builder
     * @since 0.3.0
     */
    protected static void buildOptions(Configuration conf, Cluster.Builder builder) {
        CodecRegistry cr = conf != null ? conf.getCodecRegistry() : null;
        if (cr != null) {
            builder.withCodecRegistry(cr);
        }
        MetricsOptions mOpt = conf != null ? conf.getMetricsOptions() : null;
        if (mOpt != null) {
            if (!mOpt.isEnabled()) {
                builder.withoutMetrics();
            }
            if (!mOpt.isJMXReportingEnabled()) {
                builder.withoutJMXReporting();
            }
        }
        NettyOptions nOpt = conf != null ? conf.getNettyOptions() : null;
        if (nOpt != null) {
            builder.withNettyOptions(nOpt);
        }
        PoolingOptions pOpt = conf != null ? conf.getPoolingOptions() : null;
        if (pOpt != null) {
            builder.withPoolingOptions(pOpt);
        }
        ProtocolOptions proOpt = conf != null ? conf.getProtocolOptions() : null;
        if (proOpt != null) {
            if (proOpt.getCompression() != null) {
                builder.withCompression(proOpt.getCompression());
            }
            if (proOpt.getMaxSchemaAgreementWaitSeconds() > 0) {
                builder.withMaxSchemaAgreementWaitSeconds(
                        proOpt.getMaxSchemaAgreementWaitSeconds());
            }
            if (proOpt.getProtocolVersion() != null) {
                builder.withProtocolVersion(proOpt.getProtocolVersion());
            }
            if (proOpt.getSSLOptions() != null) {
                builder.withSSL(proOpt.getSSLOptions());
            }
        }
        QueryOptions qOpt = conf != null ? conf.getQueryOptions() : null;
        if (qOpt != null) {
            builder.withQueryOptions(qOpt);
        }
        SocketOptions sOpt = conf != null ? conf.getSocketOptions() : null;
        if (sOpt != null) {
            builder.withSocketOptions(sOpt);
        }
        ThreadingOptions tOpt = conf != null ? conf.getThreadingOptions() : null;
        if (tOpt != null) {
            builder.withThreadingOptions(tOpt);
        }
    }

    /**
     * Build a new Cassandra cluster instance.
     * 
     * @param hostsAndPorts
     *            format: "host1:port1,host2,host3:port3". If no port is
     *            specified, the {@link #DEFAULT_CASSANDRA_PORT} is used.
     * @param username
     * @param password
     * @param configuration
     * @return
     * @since 0.3.0
     */
    public static Cluster newCluster(String hostsAndPorts, String username, String password,
            Configuration configuration) {
        Cluster.Builder builder = Cluster.builder();
        if (!StringUtils.isBlank(username)) {
            builder = builder.withCredentials(username, password);
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

        Cluster cluster = builder.build();
        return cluster;
    }

    /**
     * Create a new session for a cluster, initializes it and sets the keyspace
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
        return cluster.connect(StringUtils.isBlank(keyspace) ? null : keyspace);
    }

    /*----------------------------------------------------------------------*/
    /**
     * Since v0.3.0: change mapping from {Session -> {String ->
     * PreparedStatement}} to {Cluster -> {String -> PreparedStatement}}
     */
    private static LoadingCache<Cluster, Cache<String, PreparedStatement>> cachePreparedStms = CacheBuilder
            .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(
                    (RemovalListener<Cluster, Cache<String, PreparedStatement>>) notification -> notification
                            .getValue().invalidateAll())
            .build(new CacheLoader<Cluster, Cache<String, PreparedStatement>>() {
                @Override
                public Cache<String, PreparedStatement> load(Cluster cluster) {
                    Cache<String, PreparedStatement> _cache = CacheBuilder.newBuilder()
                            .expireAfterAccess(3600, TimeUnit.SECONDS).build();
                    return _cache;
                }
            });

    /**
     * Prepare a CQL query.
     * 
     * @param session
     * @param cql
     * @return
     * @since 0.2.0
     */
    public static PreparedStatement prepareStatement(Session session, String cql) {
        try {
            Cache<String, PreparedStatement> _cache = cachePreparedStms.get(session.getCluster());
            return _cache.get(cql, () -> session.prepare(cql));
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
        }
    }

    /**
     * Make sure the CQL is prepared by the correct Cluster.
     * 
     * @since 0.2.5
     */
    private static PreparedStatement ensurePrepareStatement(Session session,
            PreparedStatement pstm) {
        final String cql = pstm.getQueryString();
        return prepareStatement(session, cql);
    }

    /**
     * Bind values to a {@link PreparedStatement}.
     * 
     * @param stm
     * @param values
     * @return
     * @since 0.3.1
     */
    public static BoundStatement bindValues(PreparedStatement stm, Object... values) {
        BoundStatement bstm = stm.bind();
        if (values != null && values.length > 0) {
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (value instanceof Boolean) {
                    bstm.setBool(i, ((Boolean) value).booleanValue());
                } else if (value instanceof Byte) {
                    bstm.setByte(i, ((Byte) value).byteValue());
                } else if (value instanceof byte[]) {
                    bstm.setBytes(i, ByteBuffer.wrap((byte[]) value));
                } else if (value instanceof ByteBuffer) {
                    bstm.setBytes(i, (ByteBuffer) value);
                } else if (value instanceof BigDecimal) {
                    bstm.setDecimal(i, (BigDecimal) value);
                } else if (value instanceof Double) {
                    bstm.setDouble(i, ((Double) value).doubleValue());
                } else if (value instanceof Float) {
                    bstm.setFloat(i, ((Float) value).floatValue());
                } else if (value instanceof InetAddress) {
                    bstm.setInet(i, (InetAddress) value);
                } else if (value instanceof Integer) {
                    bstm.setInt(i, ((Integer) value).intValue());
                } else if (value instanceof List<?>) {
                    bstm.setList(i, (List<?>) value);
                } else if (value instanceof Object[]) {
                    bstm.setList(i, Arrays.asList((Object[]) value));
                } else if (value instanceof Long) {
                    bstm.setLong(i, ((Long) value).longValue());
                } else if (value instanceof Map<?, ?>) {
                    bstm.setMap(i, (Map<?, ?>) value);
                } else if (value instanceof Set<?>) {
                    bstm.setSet(i, (Set<?>) value);
                } else if (value instanceof Short) {
                    bstm.setShort(i, ((Short) value).shortValue());
                } else if (value instanceof Date) {
                    bstm.setTimestamp(i, (Date) value);
                } else if (value instanceof Token) {
                    bstm.setToken(i, (Token) value);
                } else if (value instanceof Token) {
                    bstm.setToken(i, (Token) value);
                } else if (value instanceof TupleValue) {
                    bstm.setTupleValue(i, (TupleValue) value);
                } else if (value instanceof UDTValue) {
                    bstm.setUDTValue(i, (UDTValue) value);
                } else if (value instanceof UUID) {
                    bstm.setUUID(i, (UUID) value);
                } else if (value instanceof BigInteger) {
                    bstm.setVarint(i, (BigInteger) value);
                } else if (value == null) {
                    bstm.setToNull(i);
                } else {
                    bstm.setString(i, value.toString());
                }
            }
        }
        return bstm;
    }

    /**
     * Bind values to a {@link PreparedStatement}.
     * 
     * @param stm
     * @param values
     * @return
     * @since 0.3.1
     */
    public static BoundStatement bindValues(PreparedStatement stm, Map<String, Object> values) {
        BoundStatement bstm = stm.bind();
        if (values != null && values.size() > 0) {
            for (Entry<String, Object> entry : values.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Boolean) {
                    bstm.setBool(key, ((Boolean) value).booleanValue());
                } else if (value instanceof Byte) {
                    bstm.setByte(key, ((Byte) value).byteValue());
                } else if (value instanceof byte[]) {
                    bstm.setBytes(key, ByteBuffer.wrap((byte[]) value));
                } else if (value instanceof ByteBuffer) {
                    bstm.setBytes(key, (ByteBuffer) value);
                } else if (value instanceof BigDecimal) {
                    bstm.setDecimal(key, (BigDecimal) value);
                } else if (value instanceof Double) {
                    bstm.setDouble(key, ((Double) value).doubleValue());
                } else if (value instanceof Float) {
                    bstm.setFloat(key, ((Float) value).floatValue());
                } else if (value instanceof InetAddress) {
                    bstm.setInet(key, (InetAddress) value);
                } else if (value instanceof Integer) {
                    bstm.setInt(key, ((Integer) value).intValue());
                } else if (value instanceof List<?>) {
                    bstm.setList(key, (List<?>) value);
                } else if (value instanceof Object[]) {
                    bstm.setList(key, Arrays.asList((Object[]) value));
                } else if (value instanceof Long) {
                    bstm.setLong(key, ((Long) value).longValue());
                } else if (value instanceof Map<?, ?>) {
                    bstm.setMap(key, (Map<?, ?>) value);
                } else if (value instanceof Set<?>) {
                    bstm.setSet(key, (Set<?>) value);
                } else if (value instanceof Short) {
                    bstm.setShort(key, ((Short) value).shortValue());
                } else if (value instanceof Date) {
                    bstm.setTimestamp(key, (Date) value);
                } else if (value instanceof Token) {
                    bstm.setToken(key, (Token) value);
                } else if (value instanceof Token) {
                    bstm.setToken(key, (Token) value);
                } else if (value instanceof TupleValue) {
                    bstm.setTupleValue(key, (TupleValue) value);
                } else if (value instanceof UDTValue) {
                    bstm.setUDTValue(key, (UDTValue) value);
                } else if (value instanceof UUID) {
                    bstm.setUUID(key, (UUID) value);
                } else if (value instanceof BigInteger) {
                    bstm.setVarint(key, (BigInteger) value);
                } else if (value == null) {
                    bstm.setToNull(key);
                } else {
                    bstm.setString(key, value.toString());
                }
            }
        }
        return bstm;
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @deprecated since 0.4.0.2 use {@link #execute(Session, String, Object...)}
     */
    public static void executeNonSelect(Session session, String cql, Object... bindValues) {
        executeNonSelect(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @since 0.3.0
     * @deprecated since 0.4.0.2 use {@link #execute(Session, String, Map)}
     */
    public static void executeNonSelect(Session session, String cql,
            Map<String, Object> bindValues) {
        executeNonSelect(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.2.2
     * @deprecated since 0.4.0.2 use {@link #execute(Session, String, ConsistencyLevel, Object...)}
     */
    public static void executeNonSelect(Session session, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        _executeNonSelect(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @since 0.3.0
     * @deprecated since 0.4.0.2 use {@link #execute(Session, String, ConsistencyLevel, Map)}
     */
    public static void executeNonSelect(Session session, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        _executeNonSelect(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @deprecated since 0.4.0.2 use {@link #execute(Session, PreparedStatement, Object...)}
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            Object... bindValues) {
        executeNonSelect(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @since 0.3.0
     * @deprecated since 0.4.0.2 use {@link #execute(Session, PreparedStatement, Map)}
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        executeNonSelect(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.2.2
     * @deprecated since 0.4.0.2 use
     *             {@link #execute(Session, PreparedStatement, ConsistencyLevel, Object...)}
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        _executeNonSelect(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.3.0
     * @deprecated since 0.4.0.2 use
     *             {@link #execute(Session, PreparedStatement, ConsistencyLevel, Map)}
     */
    public static void executeNonSelect(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        _executeNonSelect(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.2.6
     * @deprecated since 0.4.0.2
     */
    private static void _executeNonSelect(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        session.execute(bstm);
    }

    /**
     * Execute a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @since 0.3.0
     * @deprecated since 0.4.0.2
     */
    private static void _executeNonSelect(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        session.execute(bstm);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, String cql, Object... bindValues) {
        return execute(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSet execute(Session session, String cql, Map<String, Object> bindValues) {
        return execute(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.2
     */
    public static ResultSet execute(Session session, String cql, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
        return _execute(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSet execute(Session session, String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
        return _execute(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static ResultSet execute(Session session, PreparedStatement stm, Object... bindValues) {
        return execute(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSet execute(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        return execute(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.2
     */
    public static ResultSet execute(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _execute(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSet execute(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _execute(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public static ResultSet execute(Session session, Statement stm) {
        return execute(session, stm, null);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public static ResultSet execute(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        return _execute(session, stm, consistencyLevel);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.6
     */
    private static ResultSet _execute(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.execute(bstm);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    private static ResultSet _execute(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.execute(bstm);
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    private static ResultSet _execute(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.execute(stm);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, String cql, Object... bindValues) {
        return executeOne(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static Row executeOne(Session session, String cql, Map<String, Object> bindValues) {
        return executeOne(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.2
     */
    public static Row executeOne(Session session, String cql, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
        return _executeOne(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static Row executeOne(Session session, String cql, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
        return _executeOne(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     */
    public static Row executeOne(Session session, PreparedStatement stm, Object... bindValues) {
        return executeOne(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static Row executeOne(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        return executeOne(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.2
     */
    public static Row executeOne(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeOne(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static Row executeOne(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeOne(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public static Row executeOne(Session session, Statement stm) {
        return executeOne(session, stm, null);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public static Row executeOne(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        return _executeOne(session, stm, consistencyLevel);
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.6
     */
    private static Row _executeOne(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        ResultSet rs = _execute(session, stm, consistencyLevel, bindValues);
        return rs != null ? rs.one() : null;
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    private static Row _executeOne(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        ResultSet rs = _execute(session, stm, consistencyLevel, bindValues);
        return rs != null ? rs.one() : null;
    }

    /**
     * Execute a SELECT query and returns just one row.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    private static Row _executeOne(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        ResultSet rs = _execute(session, stm, consistencyLevel);
        return rs != null ? rs.one() : null;
    }

    /*----------------------------------------------------------------------*/
    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.2.3
     * @deprecated since 0.4.0, use {@link #executeAsync(Session, String, Object...)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, String cql,
            Object... bindValues) {
        return _executeNonSelectAsync(session, prepareStatement(session, cql), bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0, use {@link #executeAsync(Session, String, Map)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, String cql,
            Map<String, Object> bindValues) {
        return _executeNonSelectAsync(session, prepareStatement(session, cql), bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, String, ConsistencyLevel, Object...)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeNonSelectAsync(session, prepareStatement(session, cql), consistencyLevel,
                bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, String, ConsistencyLevel, Map)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeNonSelectAsync(session, prepareStatement(session, cql), consistencyLevel,
                bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.2.3
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, PreparedStatement, Object...)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, PreparedStatement stm,
            Object... bindValues) {
        return _executeNonSelectAsync(session, ensurePrepareStatement(session, stm), bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, PreparedStatement, Map)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        return _executeNonSelectAsync(session, ensurePrepareStatement(session, stm), bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, PreparedStatement, ConsistencyLevel, Object...)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeNonSelectAsync(session, ensurePrepareStatement(session, stm),
                consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0, use
     *             {@link #executeAsync(Session, PreparedStatement, ConsistencyLevel, Map)}
     */
    public static ResultSetFuture executeNonSelectAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeNonSelectAsync(session, ensurePrepareStatement(session, stm),
                consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.2.6
     * @deprecated since 0.4.0
     */
    private static ResultSetFuture _executeNonSelectAsync(Session session, PreparedStatement stm,
            Object... bindValues) {
        BoundStatement bstm = bindValues(stm, bindValues);
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0
     */
    private static ResultSetFuture _executeNonSelectAsync(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(stm, bindValues);
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.6
     * @deprecated since 0.4.0
     */
    private static ResultSetFuture _executeNonSelectAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        BoundStatement bstm = bindValues(stm, bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a non-SELECT query.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     * @deprecated since 0.4.0
     */
    private static ResultSetFuture _executeNonSelectAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(stm, bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static ResultSetFuture executeAsync(Session session, String cql, Object... bindValues) {
        return executeAsync(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSetFuture executeAsync(Session session, String cql,
            Map<String, Object> bindValues) {
        return executeAsync(session, cql, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static ResultSetFuture executeAsync(Session session, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeAsync(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSetFuture executeAsync(Session session, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeAsync(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static ResultSetFuture executeAsync(Session session, PreparedStatement stm,
            Object... bindValues) {
        return executeAsync(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSetFuture executeAsync(Session session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        return executeAsync(session, stm, (ConsistencyLevel) null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static ResultSetFuture executeAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeAsync(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static ResultSetFuture executeAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeAsync(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public static ResultSetFuture executeAsync(Session session, Statement stm) {
        return executeAsync(session, stm, (ConsistencyLevel) null);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public static ResultSetFuture executeAsync(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        return _executeAsync(session, stm, consistencyLevel);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.6
     */
    private static ResultSetFuture _executeAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    private static ResultSetFuture _executeAsync(Session session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(ensurePrepareStatement(session, stm), bindValues);
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bstm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bstm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(bstm);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link ResultSetFuture}.
     * 
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    private static ResultSetFuture _executeAsync(Session session, Statement stm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(stm);
    }

    /*----------------------------------------------------------------------*/
    /**
     * Build batch statement.
     * 
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static BatchStatement buildBatch(Statement... statements) {
        return buildBatch(null, statements);
    }

    /**
     * Build batch statement.
     * 
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static BatchStatement buildBatch(BatchStatement.Type batchType,
            Statement... statements) {
        if (statements == null || statements.length == 0) {
            throw new IllegalArgumentException("Statement list is null or empty!");
        }
        BatchStatement bStm = batchType != null ? new BatchStatement(batchType)
                : new BatchStatement();
        for (Statement stm : statements) {
            bStm.add(stm);
        }
        return bStm;
    }

    /**
     * Execute a batch statement.
     * 
     * @param session
     * @param bStm
     * @return
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #execute(Session, Statement)}
     */
    public static ResultSet executeBatch(Session session, BatchStatement bStm) {
        return executeBatch(session, bStm, null);
    }

    /**
     * Execute a batch statement.
     * 
     * @param session
     * @param bStm
     * @param consistencyLevel
     * @return
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #execute(Session, Statement, ConsistencyLevel)}
     */
    public static ResultSet executeBatch(Session session, BatchStatement bStm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bStm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bStm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.execute(bStm);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * @param session
     * @param bStm
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #executeBatch(Session, BatchStatement)}
     */
    public static void executeBatchNonSelect(Session session, BatchStatement bStm) {
        executeBatchNonSelect(session, bStm, null);
    }

    /**
     * Execute a non-select batch statement.
     * 
     * @param session
     * @param bStm
     * @param consistencyLevel
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use
     *             {@link #executeBatch(Session, BatchStatement, ConsistencyLevel)}
     */
    public static void executeBatchNonSelect(Session session, BatchStatement bStm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bStm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bStm.setConsistencyLevel(consistencyLevel);
            }
        }
        session.execute(bStm);
    }

    /**
     * Async-Execute a batch statement.
     * 
     * @param session
     * @param bStm
     * @return
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #executeAsync(Session, Statement)}
     */
    public static ResultSetFuture executeBatchAsync(Session session, BatchStatement bStm) {
        return executeBatchAsync(session, bStm, null);
    }

    /**
     * Async-Execute a batch statement.
     * 
     * @param session
     * @param bStm
     * @param consistencyLevel
     * @return
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #executeAsync(Session, Statement, ConsistencyLevel)}
     */
    public static ResultSetFuture executeBatchAsync(Session session, BatchStatement bStm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel == ConsistencyLevel.SERIAL
                    || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL) {
                bStm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                bStm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(bStm);
    }

    /**
     * Execute batches of statements.
     * 
     * @param session
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(Session session, Statement... statements) {
        return executeBatch(session, null, null, statements);
    }

    /**
     * Execute batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(Session session, ConsistencyLevel consistencyLevel,
            Statement... statements) {
        return executeBatch(session, consistencyLevel, null, statements);
    }

    /**
     * Execute batches of statements.
     * 
     * @param session
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(Session session, BatchStatement.Type batchType,
            Statement... statements) {
        return executeBatch(session, null, batchType, statements);
    }

    /**
     * Execute batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(Session session, ConsistencyLevel consistencyLevel,
            BatchStatement.Type batchType, Statement... statements) {
        return executeBatch(session, buildBatch(batchType, statements), consistencyLevel);
    }

    /**
     * Execute no-select batches of statements.
     * 
     * @param session
     * @param statements
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #executeBatch(Session, Statement...)}
     */
    public static void executeBatchNonSelect(Session session, Statement... statements) {
        executeBatchNonSelect(session, null, null, statements);
    }

    /**
     * Execute non-select batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param statements
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use {@link #executeBatch(Session, ConsistencyLevel, Statement...)}
     */
    public static void executeBatchNonSelect(Session session, ConsistencyLevel consistencyLevel,
            Statement... statements) {
        executeBatchNonSelect(session, consistencyLevel, null, statements);
    }

    /**
     * Execute non-select batches of statements.
     * 
     * @param session
     * @param batchType
     * @param statements
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use
     *             {@link #executeBatch(Session, BatchStatement.Type, Statement...)}
     */
    public static void executeBatchNonSelect(Session session, BatchStatement.Type batchType,
            Statement... statements) {
        executeBatchNonSelect(session, null, batchType, statements);
    }

    /**
     * Execute non-select batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @since 0.3.1
     * @deprecated since 0.4.0.2 use
     *             {@link #executeBatch(Session, ConsistencyLevel, BatchStatement.Type, Statement...)}
     */
    public static void executeBatchNonSelect(Session session, ConsistencyLevel consistencyLevel,
            BatchStatement.Type batchType, Statement... statements) {
        executeBatchNonSelect(session, buildBatch(batchType, statements), consistencyLevel);
    }

    /**
     * Async-Execute batches of statements.
     * 
     * @param session
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSetFuture executeBatchAsync(Session session, Statement... statements) {
        return executeBatchAsync(session, null, null, statements);
    }

    /**
     * Async-Execute batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSetFuture executeBatchAsync(Session session,
            ConsistencyLevel consistencyLevel, Statement... statements) {
        return executeBatchAsync(session, consistencyLevel, null, statements);
    }

    /**
     * Async-Execute batches of statements.
     * 
     * @param session
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSetFuture executeBatchAsync(Session session, BatchStatement.Type batchType,
            Statement... statements) {
        return executeBatchAsync(session, null, batchType, statements);
    }

    /**
     * Async-Execute batches of statements.
     * 
     * @param session
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSetFuture executeBatchAsync(Session session,
            ConsistencyLevel consistencyLevel, BatchStatement.Type batchType,
            Statement... statements) {
        return executeBatchAsync(session, buildBatch(batchType, statements), consistencyLevel);
    }
}