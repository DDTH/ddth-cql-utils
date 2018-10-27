package com.github.ddth.cql.qnd;

import java.util.Iterator;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * Statement that was prepared for a session can be used by another session from
 * the same cluster.
 * 
 * <p>
 * Schema:
 * </p>
 * 
 * <pre>
 * CREATE TABLE tbldemo (id UUID, data TEXT, PRIMARY KEY (id)) WITH COMPACT STORAGE;
 * </pre>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 */
public class QndPreparedStatementCrossSessions {

    public static void main(String[] args) throws Exception {
        try (SessionManager sm = new SessionManager()) {
            sm.init();

            Session session = sm.getSession("localhost", "test", "test", null, true);
            System.out.println("Session: " + session);

            final PreparedStatement pstm = CqlUtils.prepareStatement(session,
                    "SELECT * FROM test.tbl_test WHERE id=?");

            {
                ResultSet rs = CqlUtils.execute(session, pstm,
                        "62c36092-82a1-3a00-93d1-46196ee77204");
                Iterator<Row> it = rs.iterator();
                while (it.hasNext()) {
                    Row row = it.next();
                    System.out.println(row);
                }
            }

            session = sm.getSession("localhost", "test", "test", null, true);
            System.out.println(session);

            {
                ResultSet rs = CqlUtils.execute(session, pstm,
                        "444c3a8a-25fd-431c-b73e-14ef8a9e22fc");
                Iterator<Row> it = rs.iterator();
                while (it.hasNext()) {
                    Row row = it.next();
                    System.out.println(row);
                }
            }
        }
    }
}
