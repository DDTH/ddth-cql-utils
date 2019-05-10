package com.github.ddth.cql.qnd;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;

/**
 * CqlUtils should correctly re-prepare the statement.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 */
public class QndPreparedStatementCrossSessions2 {

    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        try (SessionManager sm = new SessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();

            sm.execute("DROP TABLE IF EXISTS test.tbl_test");
            sm.execute("CREATE TABLE test.tbl_test (id varchar, name text, PRIMARY KEY(id))");
            sm.execute("INSERT INTO test.tbl_test (id, name) VALUES (?, ?)", "1",
                    "Nguyễn Bá Thành");
            sm.execute("INSERT INTO test.tbl_test (id, name) VALUES (?, ?)", "2",
                    "Nguyen Ba Thanh");

            CqlSession session = sm.getSession(false);
            System.out.println("Session: " + session);

            final PreparedStatement pstm = CqlUtils.prepareStatement(session,
                    "UPDATE test.tbl_test SET name=? WHERE id=?");
            CqlUtils.execute(session, pstm, "NBThành", "1");

            session = sm.getSession(false);
            System.out.println("Session: " + session);
            CqlUtils.execute(session, pstm, "NBThanh", "2");
        }
    }
}
