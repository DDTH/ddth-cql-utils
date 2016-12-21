package com.github.ddth.cql.qnd;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * CqlUtils should correctly re-prepare the statement.
 * 
 * @author btnguyen
 */
public class QndPreparedStatementCrossSessions2 {

    public static void main(String[] args) throws Exception {
        try (SessionManager sm = new SessionManager()) {
            sm.init();

            Session session = sm.getSession("10.30.55.41:9042,10.30.55.42:9042,10.30.55.43:9042",
                    "tsc", "tsc", "tsc_demo", false);
            System.out.println("Session: " + session);

            final PreparedStatement pstm = CqlUtils.prepareStatement(session,
                    "UPDATE tsc_demo SET v=v+1 WHERE c=? AND t=?");

            CqlUtils.execute(session, pstm, "counter_1", 1L);

            session = sm.getSession("10.30.55.43:9042,10.30.55.41:9042,10.30.55.42:9042", "tsc",
                    "tsc", "tsc_demo", false);
            System.out.println("Session: " + session);

            CqlUtils.execute(session, pstm, "counter_1", 1L);
        }
    }
}
