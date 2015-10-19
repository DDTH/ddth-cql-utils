package com.github.ddth.cql.qnd;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

public class QndPreparedStatementCrossSession {

    public static void main(String[] args) throws Exception {
        SessionManager sm = new SessionManager();
        try {
            Session session = sm.getSession("localhost", "tsc", "tsc", "tsc_demo");

            System.out.println(session);

            final PreparedStatement pstm = CqlUtils.prepareStatement(session,
                    "UPDATE tsc_demo SET v=v+1 WHERE c=? AND t=?");

            Thread.sleep(10000);

            CqlUtils.execute(session, pstm, "counter_1", 1L);

            Thread.sleep(10000);

            session = sm.getSession("10.30.55.41:9042,10.30.55.42:9042,10.30.55.43:9042", "tsc",
                    "tsc", "tsc_demo");
            System.out.println(session);
            CqlUtils.execute(session, pstm, "counter_1", 1L);
        } finally {
            sm.destroy();
        }
    }
}
