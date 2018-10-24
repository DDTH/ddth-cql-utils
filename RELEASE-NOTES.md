# ddth-cql-utils release notes

## 0.4.0.1 - 2018-10-24

- New class `DseUtils`: try to fix the bug that DSE driver is loaded unnecessarily.


## 0.4.0 - 2018-10-21

- Add support for DataStax Enterprise: new class `DseSessionManager`
- Upgrade to `cassandra-driver-core:3.6.0` and `dse-java-driver-core:1.7.0`, both are marked as optional
- `CqlUtils`:
  - New methods `newDseCluster(...)` and `newDseSession(...)`
  - Methods `executeNonSelectAsync(...)` are now deprecated 
- `SessionManager`:
  - Add `defaultHostsAndPorts`, `defaultUsername`, `defaultPassword`, `defaultKeyspace`
  - New method `public Configuration getConfiguration()`
  - New methods to get default Cluster (`getCluster()`) and default Session (`getSession(...)`)
  - Many new methods to execute CQL/statement using the default Session 
  - Async-execution with callback, limit maximum number of async-jobs (new class `ExceedMaxAsyncJobsException`)


## 0.3.1 - 2017-06-12

- `CqlUtils`: new method(s) for batch queries.
- `CqlUtils`: rename methods `_bindValues(...)` to `bindValues(...)` and make them public.
- Upgrade DataStax's driver to `v3.2.0`, works with Guava v16.0.1+!


## 0.3.0 - 2016-12-21

- Bump to `com.github.ddth:ddth-parent:6`, now requires Java 8+.
- Upgrade DataStax's driver to `v3.1.2`.
- `CqlUtils`: Improve PreparedStatement caching.
- `CqlUtils`: Support named parameters binding.
- `SessionManager`:  


## 0.2.6 - 2015-10-18

- Fix bug: `You may have used a PreparedStatement that was created with another Cluster instance`.


## 0.2.5 - 2015-09-02

- New class `BeanPoolingOptions`.


## 0.2.4 - 2015-09-01

- Set default pooling options: 2/4 connections per local host, 1/2 connections per remote host.
- Class `SessionManager`: new members `poolingOptions`, `reconnectionPolicy` and `retryPolicy`.


## 0.2.3 - 2015-07-31

- `CqlUtils`: new method(s) for async queries.


## 0.2.2 - 2015-07-30

- `CqlUtils`: support consistency level per query.
- Overload method `SessionManager.getSession()` with new parameter to force creating new Cassandra session instance.


## 0.2.1 - 2015-04-10

- Enhancement: rebuild `Cluster` when `java.lang.IllegalStateException` occurred.


## 0.2.0.1 - 2014-10-09

- Update POM parent to `com.github.ddth:ddth-parent:2`


## 0.2.0 - 2014-10-08

- Renamed `CassandraUtils` to `CqlUtils`!!!
- Use `CqlUtils.prepareStatement(Session, String)` to reduce number of the annoying message `Re-preparing already prepared query`.
- Bump version of `cassandra-driver-core` to `2.1.1`.


## 0.1.1 - 2014-08-18

- `SessionManager.getSession(...)` now throws `com.datastax.driver.core.exceptions.NoHostAvailableException` and `com.datastax.driver.core.exceptions.AuthenticationException`


## 0.1.0 - 2014-08-18

- First release.
