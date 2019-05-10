package com.github.ddth.cql.qnd;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class QndAsyncRaw {

    public static void main(String[] args) throws Exception {
        ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader
                .programmaticBuilder();
        configBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER,
                "datacenter1");
        CqlSessionBuilder builder = CqlSession.builder();
        builder.withConfigLoader(configBuilder.build())
                .addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
        try (CqlSession session = builder.build()) {
            session.execute("DROP TABLE IF EXISTS test.tbl_async");
            session.execute("CREATE TABLE test.tbl_async (k VARCHAR, v VARCHAR, PRIMARY KEY (k))");

            String cql = "INSERT INTO test.tbl_async (k, v) VALUES (?, ?)";
            PreparedStatement pstm = session.prepare(cql);
            BoundStatement bstm = pstm.bind("key1", "value1");
            CompletionStage<AsyncResultSet> rsf = session.executeAsync(bstm);
            rsf.whenComplete((rs, err) -> {
                System.out.println(rs.wasApplied());
                System.out.println(err);
            });
            System.out.println("DONE.");
        }
    }
}
