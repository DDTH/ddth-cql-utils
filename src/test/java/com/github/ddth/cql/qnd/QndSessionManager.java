package com.github.ddth.cql.qnd;

import com.datastax.driver.core.Session;
import com.github.ddth.cql.SessionManager;

public class QndSessionManager {

    public static void main(String[] args) {
        SessionManager sm = new SessionManager();
        sm.init();

        Session session1 = sm.getSession("localhost", null, null, "demo");
        System.out.println(session1);

        Session session2 = sm.getSession("localhost", null, null, "demo");
        System.out.println(session2);

        sm.destroy();
    }

}
