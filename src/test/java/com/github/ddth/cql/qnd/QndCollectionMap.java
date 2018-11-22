package com.github.ddth.cql.qnd;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.github.ddth.cql.SessionManager;

public class QndCollectionMap {

    public static void main(String[] args) throws Exception {
        try (SessionManager sm = new SessionManager()) {
            sm.setDefaultHostsAndPorts("localhost").setDefaultUsername("test")
                    .setDefaultPassword("test").setDefaultKeyspace("test");
            sm.init();

            sm.execute("DROP TABLE IF EXISTS tbl_cmap");
            sm.execute(
                    "CREATE TABLE tbl_cmap (k varchar, v map<varchar,varchar>, PRIMARY KEY (k))");

            Map<String, String> value = new HashMap<>();
            {
                value.put("key", "value");
            }
            sm.execute("UPDATE tbl_cmap SET v=? WHERE k='1'", (Object) value);
            Row row = sm.executeOne("SELECT k,v FROM tbl_cmap WHERE k='1'");
            System.out.println(row.getMap("v", String.class, String.class));
        }
    }

}
