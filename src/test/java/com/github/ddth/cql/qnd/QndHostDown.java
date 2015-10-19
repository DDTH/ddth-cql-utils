package com.github.ddth.cql.qnd;

import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;

public class QndHostDown {

    public static void main(String[] args) throws Exception {
        Cluster cluster = CqlUtils.newCluster("localhost:9042", null, null);
        try {
            Session session = CqlUtils.newSession(cluster, "demo");
            while (true) {
                try {
                    ResultSet rs = CqlUtils.execute(session, "SELECT * FROM tbldemo");
                    Iterator<Row> it = rs.iterator();
                    while (it.hasNext()) {
                        Row row = it.next();
                        System.out.println(row);
                    }

                    System.out.println("Ping");
                    Thread.sleep(5000);
                } catch (Throwable e) {
                    e.printStackTrace();
                    Thread.sleep(5000);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
