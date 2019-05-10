package com.github.ddth.cql.qnd.cassandra;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.Callback;

/**
 * Async-insert to table, limit number of async-jobs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 */
public class QndAsync2 {

    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    }

    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            sm.execute(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");

            sm.execute("DROP TABLE IF EXISTS test.tbl_test");
            sm.execute("CREATE TABLE test.tbl_test (id text, name text, PRIMARY KEY(id))");

            int NUM_ROWS = 100000;
            String[] idList = new String[NUM_ROWS];
            String[] nameList = new String[NUM_ROWS];

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < NUM_ROWS; i++) {
                idList[i] = RandomStringUtils.randomNumeric(16);
                nameList[i] = RandomStringUtils.randomAlphanumeric(64);
            }
            long t2 = System.currentTimeMillis();
            PreparedStatement stm = sm
                    .prepareStatement("INSERT INTO test.tbl_test (id, name) VALUES (?, ?)");
            AtomicLong counterSuccess = new AtomicLong(), counterError = new AtomicLong();
            Callback<AsyncResultSet> callback = new Callback<AsyncResultSet>() {
                @Override
                public void onSuccess(AsyncResultSet result) {
                    counterSuccess.incrementAndGet();
                }

                @Override
                public void onFailure(Throwable t) {
                    counterError.incrementAndGet();
                }
            };
            for (int i = 0; i < NUM_ROWS; i++) {
                String id = idList[i];
                String name = nameList[i];
                sm.executeAsync(callback, stm, id, name);
            }
            long t3 = System.currentTimeMillis();
            long d1 = t2 - t1, d2 = t3 - t2;
            double r1 = Math.round(NUM_ROWS * 10000.0 / d1) / 10.0,
                    r2 = Math.round(NUM_ROWS * 10000.0 / d2) / 10.0;
            System.out.println(
                    "Generate [" + NUM_ROWS + "] entries in " + d1 + " ms; " + r1 + " items/s");
            System.out.println(
                    "Insert   [" + NUM_ROWS + "] entries in " + d2 + " ms; " + r2 + " items/s");

            System.out.println("Success: " + counterSuccess);
            System.out.println("Error  : " + counterError);

            long numRows = sm.executeOne("SELECT count(*) FROM test.tbl_test").getLong(0);
            System.out.println("Num rows: " + numRows);
            for (int i = 0; i < 2; i++) {
                if (numRows < NUM_ROWS) {
                    Thread.sleep(5000);
                    numRows = sm.executeOne("SELECT count(*) FROM test.tbl_test").getLong(0);
                    System.out.println("Num rows: " + numRows);
                } else {
                    break;
                }
            }
            if (numRows < NUM_ROWS) {
                Thread.sleep(5000);
            }
        }
    }
}
