package com.github.ddth.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.token.Token;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;

/**
 * Cassandra utility class.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 * @since 0.2.0 renamed to CqlUtils
 */
public class CqlUtils {
    public final static int DEFAULT_CASSANDRA_PORT = 9042;

    /*----------------------------------------------------------------------*/

    /**
     * Prepare a CQL query.
     *
     * @param session
     * @param cql
     * @return
     * @since 0.2.0
     */
    public static PreparedStatement prepareStatement(CqlSession session, String cql) {
        return session.prepare(cql);
    }

    //    /**
    //     * Make sure the CQL is prepared by the correct Cluster.
    //     *
    //     * @since 0.2.5
    //     */
    //    private static PreparedStatement ensurePrepareStatement(CqlSession session, PreparedStatement pstm) {
    //        final String cql = pstm.getQuery();
    //        return prepareStatement(session, cql);
    //    }

    /**
     * @param builder
     * @param key
     * @param value
     * @return
     * @since 1.0.0
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static BoundStatementBuilder bindSet(BoundStatementBuilder builder, String key, Set value) {
        if (value == null || value.size() == 0) {
            return builder.setToNull(key);
        }
        Object el = value.iterator().next();
        return builder.setSet(key, value, el.getClass());
    }

    /**
     * @param builder
     * @param key
     * @param value
     * @return
     * @since 1.0.0
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static BoundStatementBuilder bindList(BoundStatementBuilder builder, String key, List value) {
        if (value == null || value.size() == 0) {
            return builder.setToNull(key);
        }
        Object el = value.iterator().next();
        return builder.setList(key, value, el.getClass());
    }

    /**
     * @param builder
     * @param key
     * @param value
     * @return
     * @since 1.0.0
     */
    private static BoundStatementBuilder bindArray(BoundStatementBuilder builder, String key, Object[] value) {
        if (value == null || value.length == 0) {
            return builder.setToNull(key);
        }
        return bindList(builder, key, Arrays.asList(value));
    }

    /**
     * @param builder
     * @param key
     * @param value
     * @return
     * @since 1.0.0
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static BoundStatementBuilder bindMap(BoundStatementBuilder builder, String key, Map value) {
        if (value == null || value.size() == 0) {
            return builder.setToNull(key);
        }
        Entry e = (Entry) value.entrySet().iterator().next();
        return builder.setMap(key, value, e.getKey().getClass(), e.getValue().getClass());
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
        BoundStatementBuilder builder = stm.boundStatementBuilder(values);
        return builder.build();
        // BoundStatement bstm = stm.bind();
        // if (values != null && values.length > 0) {
        // for (int i = 0; i < values.length; i++) {
        // Object value = values[i];
        // if (value == null) {
        // bstm.setToNull(i);
        // } else if (value instanceof Boolean) {
        // bstm.setBoolean(i, ((Boolean) value).booleanValue());
        // } else if (value instanceof Byte) {
        // bstm.setByte(i, ((Byte) value).byteValue());
        // } else if (value instanceof Double) {
        // bstm.setDouble(i, ((Double) value).doubleValue());
        // } else if (value instanceof Float) {
        // bstm.setFloat(i, ((Float) value).floatValue());
        // } else if (value instanceof Integer) {
        // bstm.setInt(i, ((Integer) value).intValue());
        // } else if (value instanceof Long) {
        // bstm.setLong(i, ((Long) value).longValue());
        // } else if (value instanceof Short) {
        // bstm.setShort(i, ((Short) value).shortValue());
        // } else if (value instanceof BigDecimal) {
        // bstm.setBigDecimal(i, (BigDecimal) value);
        // } else if (value instanceof BigInteger) {
        // bstm.setBigInteger(i, (BigInteger) value);
        // } else if (value instanceof UUID) {
        // bstm.setUuid(i, (UUID) value);
        // } else if (value instanceof InetAddress) {
        // bstm.setInetAddress(i, (InetAddress) value);
        // } else if (value instanceof Token) {
        // bstm.setToken(i, (Token) value);
        // } else if (value instanceof TupleValue) {
        // bstm.setTupleValue(i, (TupleValue) value);
        // } else if (value instanceof UdtValue) {
        // bstm.setUdtValue(i, (UdtValue) value);
        // } else if (value instanceof byte[]) {
        // bstm.setByteBuffer(i, ByteBuffer.wrap((byte[]) value));
        // } else if (value instanceof ByteBuffer) {
        // bstm.setByteBuffer(i, (ByteBuffer) value);
        // } else if (value instanceof LocalDate) {
        // bstm.setLocalDate(i, (LocalDate) value);
        // } else if (value instanceof LocalTime) {
        // bstm.setLocalTime(i, (LocalTime) value);
        // } else if (value instanceof LocalDateTime) {
        // LocalDateTime ldt = (LocalDateTime) value;
        // Instant v = Instant.ofEpochSecond(ldt.getSecond(), ldt.getNano());
        // bstm.setInstant(i, v);
        // } else if (value instanceof Date) {
        // bstm.setInstant(i, ((Date) value).toInstant());
        // } else if (value instanceof Set<?>) {
        // bindSet(i, value);
        // } else if (value instanceof Map<?, ?>) {
        // bindMap(i, value);
        // } else if (value instanceof List<?>) {
        // bindList(i, value);
        // } else if (value instanceof Object[]) {
        // bindArray(i, value);
        // } else {
        // bstm.setString(i, value.toString());
        // }
        // }
        // }
        // return bstm;
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
        BoundStatementBuilder builder = stm.boundStatementBuilder();
        if (values != null && values.size() > 0) {
            for (Entry<String, Object> entry : values.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value == null) {
                    builder = builder.setToNull(key);
                } else if (value instanceof Boolean) {
                    builder = builder.setBoolean(key, ((Boolean) value).booleanValue());
                } else if (value instanceof Byte) {
                    builder = builder.setByte(key, ((Byte) value).byteValue());
                } else if (value instanceof Double) {
                    builder = builder.setDouble(key, ((Double) value).doubleValue());
                } else if (value instanceof Float) {
                    builder = builder.setFloat(key, ((Float) value).floatValue());
                } else if (value instanceof Integer) {
                    builder = builder.setInt(key, ((Integer) value).intValue());
                } else if (value instanceof Long) {
                    builder = builder.setLong(key, ((Long) value).longValue());
                } else if (value instanceof Short) {
                    builder = builder.setShort(key, ((Short) value).shortValue());
                } else if (value instanceof BigDecimal) {
                    builder = builder.setBigDecimal(key, (BigDecimal) value);
                } else if (value instanceof BigInteger) {
                    builder = builder.setBigInteger(key, (BigInteger) value);
                } else if (value instanceof UUID) {
                    builder = builder.setUuid(key, (UUID) value);
                } else if (value instanceof InetAddress) {
                    builder = builder.setInetAddress(key, (InetAddress) value);
                } else if (value instanceof Token) {
                    builder = builder.setToken(key, (Token) value);
                } else if (value instanceof TupleValue) {
                    builder = builder.setTupleValue(key, (TupleValue) value);
                } else if (value instanceof UdtValue) {
                    builder = builder.setUdtValue(key, (UdtValue) value);
                } else if (value instanceof byte[]) {
                    builder = builder.setByteBuffer(key, ByteBuffer.wrap((byte[]) value));
                } else if (value instanceof ByteBuffer) {
                    builder = builder.setByteBuffer(key, (ByteBuffer) value);
                } else if (value instanceof LocalDate) {
                    builder = builder.setLocalDate(key, (LocalDate) value);
                } else if (value instanceof LocalTime) {
                    builder = builder.setLocalTime(key, (LocalTime) value);
                } else if (value instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) value;
                    Instant v = Instant.ofEpochSecond(ldt.getSecond(), ldt.getNano());
                    builder = builder.setInstant(key, v);
                } else if (value instanceof Date) {
                    builder = builder.setInstant(key, ((Date) value).toInstant());
                } else if (value instanceof Set<?>) {
                    builder = bindSet(builder, key, (Set<?>) value);
                } else if (value instanceof Map<?, ?>) {
                    builder = bindMap(builder, key, (Map<?, ?>) value);
                } else if (value instanceof List<?>) {
                    builder = bindList(builder, key, (List<?>) value);
                } else if (value instanceof Object[]) {
                    builder = bindArray(builder, key, (Object[]) value);
                } else {
                    builder = builder.setString(key, value.toString());
                }
            }
        }
        return builder.build();
    }

    /**
     * Execute a SELECT query and returns the {@link ResultSet}.
     *
     * @param session
     * @param cql
     * @param bindValues
     * @return
     */
    public static ResultSet execute(CqlSession session, String cql, Object... bindValues) {
        return execute(session, cql, null, bindValues);
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
    public static ResultSet execute(CqlSession session, String cql, Map<String, Object> bindValues) {
        return execute(session, cql, null, bindValues);
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
    public static ResultSet execute(CqlSession session, String cql, ConsistencyLevel consistencyLevel,
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
    public static ResultSet execute(CqlSession session, String cql, ConsistencyLevel consistencyLevel,
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
    public static ResultSet execute(CqlSession session, PreparedStatement stm, Object... bindValues) {
        return execute(session, stm, null, bindValues);
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
    public static ResultSet execute(CqlSession session, PreparedStatement stm, Map<String, Object> bindValues) {
        return execute(session, stm, null, bindValues);
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
    public static ResultSet execute(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
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
    public static ResultSet execute(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public static ResultSet execute(CqlSession session, Statement<?> stm) {
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
    public static ResultSet execute(CqlSession session, Statement<?> stm, ConsistencyLevel consistencyLevel) {
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
    private static ResultSet _execute(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
        BoundStatement bstm = bindValues(/*ensurePrepareStatement(session, stm)*/stm, bindValues);
        return _execute(session, bstm, consistencyLevel);
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
    private static ResultSet _execute(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(/*ensurePrepareStatement(session, stm)*/stm, bindValues);
        return _execute(session, bstm, consistencyLevel);
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
    private static ResultSet _execute(CqlSession session, Statement<?> stm, ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel.isSerial()) {
                stm = stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm = stm.setConsistencyLevel(consistencyLevel);
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
    public static Row executeOne(CqlSession session, String cql, Object... bindValues) {
        return executeOne(session, cql, null, bindValues);
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
    public static Row executeOne(CqlSession session, String cql, Map<String, Object> bindValues) {
        return executeOne(session, cql, null, bindValues);
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
    public static Row executeOne(CqlSession session, String cql, ConsistencyLevel consistencyLevel,
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
    public static Row executeOne(CqlSession session, String cql, ConsistencyLevel consistencyLevel,
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
    public static Row executeOne(CqlSession session, PreparedStatement stm, Object... bindValues) {
        return executeOne(session, stm, null, bindValues);
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
    public static Row executeOne(CqlSession session, PreparedStatement stm, Map<String, Object> bindValues) {
        return executeOne(session, stm, null, bindValues);
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
    public static Row executeOne(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
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
    public static Row executeOne(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    public static Row executeOne(CqlSession session, Statement<?> stm) {
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
    public static Row executeOne(CqlSession session, Statement<?> stm, ConsistencyLevel consistencyLevel) {
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
    private static Row _executeOne(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Object... bindValues) {
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
    private static Row _executeOne(CqlSession session, PreparedStatement stm, ConsistencyLevel consistencyLevel,
            Map<String, Object> bindValues) {
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
    private static Row _executeOne(CqlSession session, Statement<?> stm, ConsistencyLevel consistencyLevel) {
        ResultSet rs = _execute(session, stm, consistencyLevel);
        return rs != null ? rs.one() : null;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, String cql, Object... bindValues) {
        return executeAsync(session, cql, null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param cql
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, String cql,
            Map<String, Object> bindValues) {
        return executeAsync(session, cql, null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, String cql,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeAsync(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param cql
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, String cql,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeAsync(session, prepareStatement(session, cql), consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, PreparedStatement stm,
            Object... bindValues) {
        return executeAsync(session, stm, null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, PreparedStatement stm,
            Map<String, Object> bindValues) {
        return executeAsync(session, stm, null, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.3
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        return _executeAsync(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        return _executeAsync(session, stm, consistencyLevel, bindValues);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @return
     * @since 0.4.0.2
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, Statement<?> stm) {
        return executeAsync(session, stm, null);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    public static CompletionStage<AsyncResultSet> executeAsync(CqlSession session, Statement<?> stm,
            ConsistencyLevel consistencyLevel) {
        return _executeAsync(session, stm, consistencyLevel);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.2.6
     */
    private static CompletionStage<AsyncResultSet> _executeAsync(CqlSession session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Object... bindValues) {
        BoundStatement bstm = bindValues(/*ensurePrepareStatement(session, stm)*/stm, bindValues);
        return _executeAsync(session, bstm, consistencyLevel);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @param bindValues
     * @return
     * @since 0.3.0
     */
    private static CompletionStage<AsyncResultSet> _executeAsync(CqlSession session, PreparedStatement stm,
            ConsistencyLevel consistencyLevel, Map<String, Object> bindValues) {
        BoundStatement bstm = bindValues(/*ensurePrepareStatement(session, stm)*/stm, bindValues);
        return _executeAsync(session, bstm, consistencyLevel);
    }

    /**
     * Async-Executes a SELECT query and returns the {@link CompletionStage}.
     *
     * @param session
     * @param stm
     * @param consistencyLevel
     * @return
     * @since 0.4.0.2
     */
    private static CompletionStage<AsyncResultSet> _executeAsync(CqlSession session, Statement<?> stm,
            ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != null) {
            if (consistencyLevel.isSerial()) {
                stm = stm.setSerialConsistencyLevel(consistencyLevel);
            } else {
                stm = stm.setConsistencyLevel(consistencyLevel);
            }
        }
        return session.executeAsync(stm);
    }

    /*----------------------------------------------------------------------*/

    /**
     * Build {@link BatchStatement} from batch of statements.
     *
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static BatchStatement buildBatch(Statement<?>... statements) {
        return buildBatch(null, statements);
    }

    /**
     * Build {@link BatchStatement} from batch of statements.
     *
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static BatchStatement buildBatch(BatchType batchType, Statement<?>... statements) {
        if (statements == null || statements.length == 0) {
            throw new IllegalArgumentException("Statement list is null or empty!");
        }
        BatchStatement bStm = batchType != null ?
                BatchStatement.newInstance(batchType) :
                BatchStatement.newInstance(DefaultBatchType.LOGGED);
        for (Statement<?> stm : statements) {
            if (stm instanceof BatchableStatement<?>) {
                bStm = bStm.add((BatchableStatement<?>) stm);
            } else {
                throw new IllegalArgumentException("Statement is not batchable: " + stm);
            }
        }
        return bStm;
    }

    /**
     * Execute a batch of statements.
     *
     * @param session
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(CqlSession session, Statement<?>... statements) {
        return executeBatch(session, null, null, statements);
    }

    /**
     * Execute a batch of statements.
     *
     * @param session
     * @param consistencyLevel
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(CqlSession session, ConsistencyLevel consistencyLevel,
            Statement<?>... statements) {
        return executeBatch(session, consistencyLevel, null, statements);
    }

    /**
     * Execute a batch of statements.
     *
     * @param session
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(CqlSession session, BatchType batchType, Statement<?>... statements) {
        return executeBatch(session, null, batchType, statements);
    }

    /**
     * Execute a batch of statements.
     *
     * @param session
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static ResultSet executeBatch(CqlSession session, ConsistencyLevel consistencyLevel, BatchType batchType,
            Statement<?>... statements) {
        return execute(session, buildBatch(batchType, statements), consistencyLevel);
    }

    /**
     * Async-Execute a batch of statements.
     *
     * @param session
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static CompletionStage<AsyncResultSet> executeBatchAsync(CqlSession session, Statement<?>... statements) {
        return executeBatchAsync(session, null, null, statements);
    }

    /**
     * Async-Execute a batch of statements.
     *
     * @param session
     * @param consistencyLevel
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static CompletionStage<AsyncResultSet> executeBatchAsync(CqlSession session,
            ConsistencyLevel consistencyLevel, Statement<?>... statements) {
        return executeBatchAsync(session, consistencyLevel, null, statements);
    }

    /**
     * Async-Execute a batch of statements.
     *
     * @param session
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static CompletionStage<AsyncResultSet> executeBatchAsync(CqlSession session, BatchType batchType,
            Statement<?>... statements) {
        return executeBatchAsync(session, null, batchType, statements);
    }

    /**
     * Async-Execute a batch of statements.
     *
     * @param session
     * @param consistencyLevel
     * @param batchType
     * @param statements
     * @return
     * @since 0.3.1
     */
    public static CompletionStage<AsyncResultSet> executeBatchAsync(CqlSession session,
            ConsistencyLevel consistencyLevel, BatchType batchType, Statement<?>... statements) {
        return executeAsync(session, buildBatch(batchType, statements), consistencyLevel);
    }
}
