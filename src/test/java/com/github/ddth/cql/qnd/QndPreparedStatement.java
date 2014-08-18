package com.github.ddth.cql.qnd;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.ddth.cql.SessionManager;

public class QndPreparedStatement {

    public static void main(String[] args) {
        SessionManager sm = new SessionManager();
        sm.init();

        Session session1 = sm.getSession("localhost", null, null, "demo");
        System.out.println(session1);

        Session session2 = sm.getSession("localhost", null, null, "demo");
        System.out.println(session2);

        Session session3 = sm.getSession("localhost", null, null, "demo");
        System.out.println(session3);

        PreparedStatement pstm1 = session1.prepare("select * from tbl1");
        PreparedStatement pstm2 = session1.prepare("select * from tbl1");
        PreparedStatement pstm3 = session1.prepare("select * from tbl1");

        sm.destroy();
    }

}
