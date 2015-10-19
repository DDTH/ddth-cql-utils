package com.github.ddth.cql.qnd;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;

public class QndPreparedStatement {

    public static void main(String[] args) throws Exception {
        final Cluster cluster = CqlUtils.newCluster("localhost", "tsc", "tsc");
        cluster.getConfiguration().getProtocolOptions().setCompression(Compression.SNAPPY);
        {
            PoolingOptions pOptions = cluster.getConfiguration().getPoolingOptions();
            // pOptions.setConnectionsPerHost(HostDistance.LOCAL, 16, 256);
            // pOptions.setConnectionsPerHost(HostDistance.REMOTE, 4, 16);
        }
        for (Host host : cluster.getMetadata().getAllHosts()) {
            System.out.println(host);
            System.out.println(cluster.getConfiguration().getPolicies().getLoadBalancingPolicy()
                    .distance(host));
        }

        final Session session = CqlUtils.newSession(cluster, "tsc_demo");
        System.out.println(session);

        // final PreparedStatement pstm = CqlUtils.prepareStatement(session,
        // "UPDATE tsc_demo SET v=v+1 WHERE c=? AND t=?");
        final PreparedStatement pstm = CqlUtils.prepareStatement(session,
                "UPDATE tsc_demo2 SET v=1 WHERE c=? AND t=?");

        final BlockingQueue<ResultSetFuture> FUTURES = new LinkedBlockingQueue<ResultSetFuture>();
        final AtomicLong COUNTER = new AtomicLong(0);
        for (final ConsistencyLevel cl : new ConsistencyLevel[] { ConsistencyLevel.ALL,
                ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.ANY }) {
            final long N = 32000;
            int NUM_THREADS = 64;
            Thread[] THREADS = new Thread[NUM_THREADS];

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < THREADS.length; i++) {
                Thread t = new Thread() {
                    public void run() {
                        for (int i = 0; i < N; i++) {
                            BoundStatement bstm = pstm.bind("counter_1", COUNTER.incrementAndGet());
                            bstm.setConsistencyLevel(cl);
                            // session.execute(bstm);
                            ResultSetFuture rsf = session.executeAsync(bstm);
                            // if (rsf != null) {
                            // FUTURES.add(rsf);
                            // } else {
                            // throw new NullPointerException();
                            // }
                            // rsf.getUninterruptibly();
                        }
                    }
                };
                THREADS[i] = t;
                t.start();
            }
            for (Thread t : THREADS) {
                t.join();
            }
            long t2 = System.currentTimeMillis();
            System.out.println("====================");
            System.out.println(cl + ": " + (t2 - t1) + " - " + COUNTER);
            System.out.println(N * THREADS.length * 1000.0 / (t2 - t1));
        }

        System.out.println("Size: " + FUTURES.size());
        while (FUTURES.size() > 0) {
            // Wait for the other write requests to terminate
            ResultSetFuture future = FUTURES.poll();
            future.get();
        }

        session.closeAsync().get();
        cluster.closeAsync().get();
    }
}
