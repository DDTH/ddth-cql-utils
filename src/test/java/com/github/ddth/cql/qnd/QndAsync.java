package com.github.ddth.cql.qnd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryCallbackResultSet;

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
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                CqlSession session = sm.getSession();
                for (int j = 0; j < numLoops; j++) {
                    long t = System.currentTimeMillis();
                    int k = (int) (t / (1000));
                    BoundStatement stm = pstm.bind().setConsistencyLevel(consistencyLevel)
                            .setLong(0, 1).setString(1, metric).setInt(2, k).setLong(3, t);
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

    synchronized static void submitResult(List<CompletionStage<AsyncResultSet>> resultList,
            CompletionStage<AsyncResultSet> result) {
        resultList.add(result);
        int n = resultList.size();
        if (n >= maxJobs) {
            long t = System.currentTimeMillis();
            System.out.print("\tFinishing up [" + n + "] async jobs...");
            for (CompletionStage<AsyncResultSet> el : resultList) {
                try {
                    el.toCompletableFuture().get().one();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            resultList.clear();
            System.out.println((System.currentTimeMillis() - t) + "ms");
        }
    }

    static void testAsync(SessionManager sm, String metric, ConsistencyLevel consistencyLevel,
            String cql, int numThreads, int numLoops) throws InterruptedException {
        long t1 = System.currentTimeMillis();
        PreparedStatement pstm = sm.prepareStatement(cql);
        List<CompletionStage<AsyncResultSet>> result = new ArrayList<>();
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                CqlSession session = sm.getSession();
                for (int j = 0; j < numLoops; j++) {
                    long t = System.currentTimeMillis();
                    int k = (int) (t / (1000));
                    BoundStatement stm = pstm.bind().setConsistencyLevel(consistencyLevel)
                            .setLong(0, 1).setString(1, metric).setInt(2, k).setLong(3, t);
                    submitResult(result, session.executeAsync(stm));
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        if (result.size() > 0) {
            for (CompletionStage<AsyncResultSet> el : result) {
                try {
                    el.toCompletableFuture().get().one();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
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
                        RetryCallbackResultSet callback = new RetryCallbackResultSet(sm, 1000,
                                consistencyLevel, cql, 1L, metric, k, t) {
                            @Override
                            public void onSuccess(AsyncResultSet result) {
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

    static int maxJobs = 1;

    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        maxJobs = 512;
        int numTheads = 8;
        int numLoops = 10_000;
        // String cql = "UPDATE test.tsc_col_bigint SET v=? WHERE m=? AND k=? AND t=?";
        String cql = "UPDATE test.tsc_col_counter SET v=v+? WHERE m=? AND k=? AND t=?";
        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.setMaxAsyncJobs(maxJobs);
            sm.init();

            initSchema(sm);

            testSync(sm, "counter1", DefaultConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
            // testSync(sm, "counter2", DefaultConsistencyLevel.LOCAL_ONE, cql, numTheads,
            // numLoops);
            testAsync(sm, "counter3", DefaultConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
            testAsync2(sm, "counter4", DefaultConsistencyLevel.LOCAL_ONE, cql, numTheads, numLoops);
        }
    }
}
