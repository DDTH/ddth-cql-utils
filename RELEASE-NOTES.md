ddth-cql-utils release notes
============================

0.2.0 - 2014-10-08
------------------
- Renamed `CassandraUtils` to `CqlUtils`!!!
- Use `CqlUtils.prepareStatement(Session, String)` to reduce number of the annoying message `Re-preparing already prepared query`.
- Bump version of `cassandra-driver-core` to `2.1.1`.


0.1.1 - 2014-08-18
------------------
- `SessionManager.getSession(...)` now throws `com.datastax.driver.core.exceptions.NoHostAvailableException` and `com.datastax.driver.core.exceptions.AuthenticationException`


0.1.0 - 2014-08-18
------------------
- First release.
