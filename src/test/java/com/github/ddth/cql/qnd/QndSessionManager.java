package com.github.ddth.cql.qnd;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.github.ddth.cql.SessionManager;

public class QndSessionManager {

    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");

        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            CqlSession session1 = sm.getSession(false);
            System.out.println("Session 1 (forceNew = false / close = false): " + session1);

            CqlSession session2 = sm.getSession(false);
            System.out.println("Session 2 (forceNew = false / close = false): " + session2);
        }

        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            CqlSession session1 = sm.getSession(false);
            System.out.println("Session 1 (forceNew = false / close = true): " + session1);
            session1.close();

            CqlSession session2 = sm.getSession(false);
            System.out.println("Session 2 (forceNew = false / close = true): " + session2);
            session2.close();
        }

        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            CqlSession session1 = sm.getSession(true);
            System.out.println("Session 1 (forceNew = true / close = false): " + session1);

            CqlSession session2 = sm.getSession(true);
            System.out.println("Session 2 (forceNew = true / close = false): " + session2);
        }
    }

}
