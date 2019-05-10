package com.github.ddth.cql.qnd;

import java.util.Iterator;
import java.util.Random;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * See what happens when host(s) is/are down!
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 */
public class QndHostDown {
    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    }

    public static void main(String[] args) throws Exception {
        Random r = new Random(System.currentTimeMillis());
        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            // initialize data
            sm.execute(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");
            sm.execute("DROP TABLE IF EXISTS test.tbl_test");
            sm.execute("CREATE TABLE test.tbl_test (id text, name text, PRIMARY KEY(id))");

            long timestamp = 0;
            while (true) {
                try {
                    CqlSession session = sm.getSession(false);
                    CqlUtils.execute(session, "UPDATE test.tbl_test SET name=? WHERE id='key'",
                            String.valueOf(r.nextInt()));
                    ResultSet rs = CqlUtils.execute(session, "SELECT * FROM test.tbl_test");
                    Iterator<Row> it = rs.iterator();
                    while (it.hasNext()) {
                        Row row = it.next();
                        System.out.println("Row: " + row);
                    }

                    timestamp = System.currentTimeMillis();
                    System.out.println("Session: " + session);
                    // session.close();
                    Thread.sleep(r.nextInt(2000));
                } catch (Throwable e) {
                    System.out.println("ERROR/" + (System.currentTimeMillis() - timestamp) + ": "
                            + e.getMessage());
                    Thread.sleep(r.nextInt(5000));
                }
                System.out.println();
            }
        }
    }

}
