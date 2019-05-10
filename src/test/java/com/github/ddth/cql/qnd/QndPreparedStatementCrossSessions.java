package com.github.ddth.cql.qnd;

import java.util.Iterator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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

            CqlSession session1 = sm.getSession(true);
            System.out.println("Session: " + session1);
            final PreparedStatement pstm = CqlUtils.prepareStatement(session1,
                    "SELECT * FROM test.tbl_test WHERE id=?");
            {
                // BoundStatement bstm = pstm.bind("1");
                // ResultSet rs = session1.execute(bstm);
                ResultSet rs = CqlUtils.execute(session1, pstm, "1");
                Iterator<Row> it = rs.iterator();
                while (it.hasNext()) {
                    Row row = it.next();
                    System.out.println("\tRow data: " + row.getString("name"));
                }
            }

            CqlSession session2 = sm.getSession(true);
            System.out.println("Session: " + session2);
            {
                // BoundStatement bstm = pstm.bind("2");
                // ResultSet rs = session2.execute(bstm);
                ResultSet rs = CqlUtils.execute(session2, pstm, "2");
                Iterator<Row> it = rs.iterator();
                while (it.hasNext()) {
                    Row row = it.next();
                    System.out.println("\tRow data: " + row.getString("name"));
                }
            }
        }
    }
}
