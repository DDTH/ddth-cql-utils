package com.github.ddth.cql.qnd;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryFutureCallbackResultSet;

public class QndAsync {

    static void initSchema(SessionManager sm) {
        sm.execute("DROP TABLE IF EXISTS test.tsc_metadata");
        sm.execute("CREATE TABLE test.tsc_metadata (m varchar, o text, PRIMARY KEY(m))");
        sm.execute(
                "UPDATE test.tsc_metadata SET o='{\"table\":\"tsc_col_counter\", \"counter_column\":true}' WHERE m='counter_1'");
        sm.execute(
                "UPDATE test.tsc_metadata SET o='{\"table\":\"tsc_col_bigint\", \"counter_column\":false}' WHERE m='counter_2'");

        sm.execute("DROP TABLE IF EXISTS test.tsc_tag");
        sm.execute(
                "CREATE TABLE test.tsc_tag(m varchar,k int,t bigint,tags map<varchar,varchar>,PRIMARY KEY((m, k), t))");

        sm.execute("DROP TABLE IF EXISTS test.tsc_col_counter");
        sm.execute(
                "CREATE TABLE test.tsc_col_counter(m varchar,k int,t bigint,v counter,PRIMARY KEY((m, k), t))");

        sm.execute("DROP TABLE IF EXISTS test.tsc_col_bigint");
        sm.execute(
                "CREATE TABLE test.tsc_col_bigint(m varchar,k int,t bigint,v bigint,PRIMARY KEY((m, k), t))");
    }

    static void testSync(SessionManager sm, String metric, ConsistencyLevel consistencyLevel,
            String cql, int numThreads, int numLoops) throws InterruptedException {
        long t1 = System.currentTimeMillis();
        PreparedStatement pstm = sm.prepareStatement(cql);
        pstm.setConsistencyLevel(consistencyLevel);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                Session session = sm.getSession();
                for (int j = 0; j < numLoops; j++) {
                    long t = System.currentTimeMillis();
                    int k = (int) (t / (1000));
                    BoundStatement stm = pstm.bind();
                    stm.setLong(0, 1);
                    stm.setString(1, metric);
                    stm.setInt(2, k);
                    stm.setLong(3, t);
                    session.execute(stm);
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long t2 = System.currentTimeMillis();
        long d = t2 - t1;
        int numItems = numThreads * numLoops;
        System.out.println("Sync - " + numThreads + " threads " + String.format("%,d", numLoops)
                + " loops/thread: " + String.format("%,d", numItems) + " writes in "
                + String.format("%,d", d) + " ms - "
                + String.format("%,1.1f", numItems * 1000.0 / d) + " writes/sec");
        Thread.sleep(1000);
    }

    static int maxJobs = 1200;

    synchronized static void submitResult(List<ResultSetFuture> resultList,
            ResultSetFuture result) {
        resultList.add(result);
        if (resultList.size() >= maxJobs) {
            for (ResultSetFuture rsf : resultList) {
                rsf.getUninterruptibly();
            }
            resultList.clear();
        }
    }

    static void testAsync(SessionManager sm, String metric, ConsistencyLevel consistencyLevel,
            String cql, int numThreads, int numLoops) throws InterruptedException {
        long t1 = System.currentTimeMillis();
        PreparedStatement pstm = sm.prepareStatement(cql);
        pstm.setConsistencyLevel(consistencyLevel);
        List<ResultSetFuture> result = new ArrayList<>();
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                Session session = sm.getSession();
                for (int j = 0; j < numLoops; j++) {
                    long t = System.currentTimeMillis();
                    int k = (int) (t / (1000));
                    BoundStatement stm = pstm.bind();
                    stm.setLong(0, 1);
                    stm.setString(1, metric);
                    stm.setInt(2, k);
                    stm.setLong(3, t);
                    submitResult(result, session.executeAsync(stm));
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        if (result.size() > 0) {
            for (ResultSetFuture rsf : result) {
                rsf.getUninterruptibly();
            }
            result.clear();
        }
        long t2 = System.currentTimeMillis();
        long d = t2 - t1;
        int numItems = numThreads * numLoops;
        System.out.println("Async - " + numThreads + " threads " + String.format("%,d", numLoops)
                + " loops/thread: " + String.format("%,d", numItems) + " writes in "
                + String.format("%,d", d) + " ms - "
                + String.format("%,1.1f", numItems * 1000.0 / d) + " writes/sec");
        Thread.sleep(10000);
    }

    static void testAsync2(SessionManager sm, String metric, ConsistencyLevel consistencyLevel,
            String cql, int numThreads, int numLoops) throws InterruptedException {
        long t1 = System.currentTimeMillis();
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < numLoops; j++) {
                    long t = System.currentTimeMillis();
                    int k = (int) (t / (1000));
                    try {
                        RetryFutureCallbackResultSet callback = new RetryFutureCallbackResultSet(sm,
                                1000, consistencyLevel, cql, 1L, metric, k, t) {
                            @Override
                            public void onSuccess(@Nullable ResultSet result) {
                            }

                            @Override
                            protected void onError(Throwable t) {
                                t.printStackTrace();
                            }
                        };
                        sm.executeAsync(callback, 1000, cql, consistencyLevel, 1L, metric, k, t);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long t2 = System.currentTimeMillis();
        long d = t2 - t1;
        int numItems = numThreads * numLoops;
        System.out.println("Async2 - " + numThreads + " threads " + String.format("%,d", numLoops)
                + " loops/thread: " + String.format("%,d", numItems) + " writes in "
                + String.format("%,d", d) + " ms - "
                + String.format("%,1.1f", numItems * 1000.0 / d) + " writes/sec");
        Thread.sleep(10000);
    }

    public static void main(String[] args) throws Exception {
        maxJobs = 1024;
        int numTheads = 8;
        int numLoops = 10_000;
        // String cql = "UPDATE test.tsc_col_bigint SET v=? WHERE m=? AND k=? AND t=?";
        String cql = "UPDATE test.tsc_col_counter SET v=v+? WHERE m=? AND k=? AND t=?";
        try (SessionManager sm = new SessionManager()) {
            sm.setDefaultHostsAndPorts("localhost").setDefaultUsername("test")
                    .setDefaultPassword("test");
            sm.setMaxAsyncJobs(maxJobs * 2);
            sm.init();

            LoadBalancingPolicy lbPolicy = sm.getCluster().getConfiguration().getPolicies()
                    .getLoadBalancingPolicy();
            Set<Host> allHosts = sm.getCluster().getMetadata().getAllHosts();
            for (Host host : allHosts) {
                System.out.println(host + " - " + host.getDatacenter() + " - "
                        + host.getCassandraVersion() + " - " + lbPolicy.distance(host));
            }

            initSchema(sm);

            testSync(sm, "counter1", ConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
            testSync(sm, "counter2", ConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
            testAsync(sm, "counter3", ConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
            testAsync2(sm, "counter4", ConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
        }
    }

}
