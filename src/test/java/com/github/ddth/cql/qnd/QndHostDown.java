package com.github.ddth.cql.qnd;

import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * See what happens when host(s) is/are down!
 * 
 * @author btnguyen
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
        try (SessionManager sm = new SessionManager()) {
            sm.setDefaultHostsAndPorts("localhost").setDefaultUsername("test")
                    .setDefaultPassword("test").setDefaultKeyspace(null);
            sm.init();

            // initialize data
            sm.executeNonSelect(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");
            sm.executeNonSelect("DROP TABLE IF EXISTS test.tbl_test");
            sm.executeNonSelect("CREATE TABLE test.tbl_test (id text, name text, PRIMARY KEY(id))");

            long timestamp = 0;
            while (true) {
                Session session = sm.getSession("localhost", "test", "test", null, false);
                try {
                    ResultSet rs = CqlUtils.execute(session, "SELECT * FROM test.tbl_test");
                    Iterator<Row> it = rs.iterator();
                    while (it.hasNext()) {
                        Row row = it.next();
                        System.out.println(row);
                    }

                    timestamp = System.currentTimeMillis();
                    System.out.println("Session: " + session);
                    session.close();
                    Thread.sleep(2000);
                } catch (Throwable e) {
                    System.out.println("ERROR/" + (System.currentTimeMillis() - timestamp) + ": "
                            + e.getMessage());
                    // e.printStackTrace();
                    Thread.sleep(5000);
                }

                System.out.println();
            }
        }
    }

}
