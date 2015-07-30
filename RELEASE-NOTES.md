ddth-cql-utils release notes
============================

0.2.2 - 2015-07-30
------------------

- `CqlUtils`: support consistency level per query.
- Overload method `SessionManager.getSession()` with new parameter to force creating new Cassandra session instance.


0.2.1 - 2015-04-10
------------------

- Enhancement: rebuild `Cluster` when `java.lang.IllegalStateException` occurred.


0.2.0.1 - 2014-10-09
--------------------

- Update POM parent to `com.github.ddth:ddth-parent:2`


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
