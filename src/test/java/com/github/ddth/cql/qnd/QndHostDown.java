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

    public static void main(String[] args) throws Exception {
        try (SessionManager sm = new SessionManager()) {
            sm.init();

            long timestamp = 0;
            while (true) {
                Session session = sm.getSession("localhost", "demo", "demo", "demo", false);
                try {
                    ResultSet rs = CqlUtils.execute(session, "SELECT * FROM tbldemo");
                    Iterator<Row> it = rs.iterator();
                    while (it.hasNext()) {
                        Row row = it.next();
                        System.out.println(row);
                    }

                    timestamp = System.currentTimeMillis();
                    System.out.println("Ping: " + session);
                    session.close();
                    Thread.sleep(2000);
                } catch (Throwable e) {
                    System.out.println("ERROR/" + (System.currentTimeMillis() - timestamp) + ": "
                            + e.getMessage());
                    // e.printStackTrace();
                    Thread.sleep(5000);
                }
            }
        }
    }

}
