package dse;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.github.ddth.cql.DseSessionManager;
import com.google.common.util.concurrent.FutureCallback;

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
        try (DseSessionManager sm = new DseSessionManager()) {
            sm.setDefaultHostsAndPorts("localhost").setDefaultUsername("test")
                    .setDefaultPassword("test").setDefaultKeyspace(null);
            sm.init();

            sm.executeNonSelect(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");

            sm.executeNonSelect("DROP TABLE IF EXISTS test.tbl_test");
            sm.executeNonSelect("CREATE TABLE test.tbl_test (id text, name text, PRIMARY KEY(id))");

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
            FutureCallback<ResultSet> future = new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet result) {
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
                sm.executeAsync(future, stm, id, name);
            }
            long t3 = System.currentTimeMillis();
            System.out.println("Generated [" + NUM_ROWS + "] entries in " + (t2 - t1) + " ms.");
            System.out.println("Inserted  [" + NUM_ROWS + "] entries in " + (t3 - t2) + " ms.");

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