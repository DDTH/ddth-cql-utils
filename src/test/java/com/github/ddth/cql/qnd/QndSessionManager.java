package com.github.ddth.cql.qnd;

import com.datastax.driver.core.Session;
import com.github.ddth.cql.SessionManager;

public class QndSessionManager {

    public static void main(String[] args) throws Exception {
        try (SessionManager sm = new SessionManager()) {
            sm.init();

            Session session1 = sm.getSession("localhost", "demo", "demo", "demo", false);
            System.out.println("Session 1 (forceNew = false / close = false): " + session1);

            Session session2 = sm.getSession("localhost", "demo", "demo", "demo", false);
            System.out.println("Session 2 (forceNew = false / close = false): " + session2);
        }

        try (SessionManager sm = new SessionManager()) {
            sm.init();

            Session session1 = sm.getSession("localhost", "demo", "demo", "demo", false);
            System.out.println("Session 1 (forceNew = false / close = true): " + session1);
            session1.close();

            Session session2 = sm.getSession("localhost", "demo", "demo", "demo", false);
            System.out.println("Session 2 (forceNew = false / close = true): " + session2);
            session2.close();
        }

        try (SessionManager sm = new SessionManager()) {
            sm.init();

            Session session1 = sm.getSession("localhost", "demo", "demo", "demo", true);
            System.out.println("Session 1 (forceNew = true / close = false): " + session1);

            Session session2 = sm.getSession("localhost", "demo", "demo", "demo", true);
            System.out.println("Session 2 (forceNew = true / close = false): " + session2);
        }
    }

}
