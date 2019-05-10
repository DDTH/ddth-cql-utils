package com.github.ddth.cql.qnd;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class QndCollection {

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader
                .programmaticBuilder();
        configBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER,
                "datacenter1");
        CqlSessionBuilder builder = CqlSession.builder();
        builder.withConfigLoader(configBuilder.build())
                .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
        try (CqlSession session = builder.build()) {
            ResultSet rs;

            rs = session.execute("DROP TABLE IF EXISTS test.tbl_collection");
            System.out.println("DROP TABLE: " + rs.one());

            rs = session.execute(
                    "CREATE TABLE test.tbl_collection (k VARCHAR, s SET<VARCHAR>, l LIST<INT>, m MAP<VARCHAR,INT>, PRIMARY KEY (k))");
            System.out.println("CREATE TABLE: " + rs.one());
            {
                Object value = new HashSet<String>() {
                    {
                        add("one");
                        add("2");
                        add("3.4");
                        add("true");
                    }
                };
                String cql = "INSERT INTO test.tbl_collection (k, s) VALUES (?, ?)";
                PreparedStatement pstm = session.prepare(cql);
                BoundStatement bstm = pstm.bind("key-1", value);
                rs = session.execute(bstm);
                System.out.println("Insert [key-1]: " + rs.one());
            }
            {
                Object value = new ArrayList<Integer>() {
                    {
                        add(1);
                        add(2);
                        add(3);
                    }
                };
                String cql = "INSERT INTO test.tbl_collection (k, l) VALUES (?, ?)";
                PreparedStatement pstm = session.prepare(cql);
                BoundStatement bstm = pstm.bind("key-2", value);
                rs = session.execute(bstm);
                System.out.println("Insert [key-2]: " + rs.one());
            }
            {
                Object value = new HashMap<String, Integer>() {
                    {
                        put("One", 1);
                        put("two", 2);
                        put("three", 3);
                    }
                };
                String cql = "INSERT INTO test.tbl_collection (k, m) VALUES (?, ?)";
                PreparedStatement pstm = session.prepare(cql);
                BoundStatement bstm = pstm.bind("key-3", value);
                rs = session.execute(bstm);
                System.out.println("Insert [key-3]: " + rs.one());
            }
            {
                Object valueS = new HashSet<String>() {
                    {
                        add("one");
                        add("2");
                        add("3.4");
                        add("true");
                    }
                };
                Object valueL = new ArrayList<Integer>() {
                    {
                        add(1);
                        add(2);
                        add(3);
                    }
                };
                Object valueM = new HashMap<String, Integer>() {
                    {
                        put("One", 1);
                        put("two", 2);
                        put("three", 3);
                    }
                };
                String cql = "INSERT INTO test.tbl_collection (k, s, l, m) VALUES (?, ?, ?, ?)";
                PreparedStatement pstm = session.prepare(cql);
                BoundStatement bstm = pstm.bind("key-4", valueS, valueL, valueM);
                rs = session.execute(bstm);
                System.out.println("Insert [key-4]: " + rs.one());
            }
            {
                String cql = "INSERT INTO test.tbl_collection (k, s, l, m) VALUES (?, ?, ?, ?)";
                PreparedStatement pstm = session.prepare(cql);
                BoundStatement bstm = pstm.bind("key-4");
                bstm = bstm.unset(1);
                bstm = bstm.setToNull(2);
                rs = session.execute(bstm);
                System.out.println("Update [key-4]: " + rs.one());
            }
        }
    }
}
