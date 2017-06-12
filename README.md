# ddth-cql-utils

Wrap around [DataStax's Cassandra Java Driver](http://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html)
and simplify the usage of CQL with helper class.

Project home:
[https://github.com/DDTH/ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils)

**`ddth-cql-utils` requires Java 8+ since v0.3.0**


## Installation

Latest release version: `0.3.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-cql-utils</artifactId>
	<version>0.3.1</version>
</dependency>
```


## Usage

Manage Cassandra `Cluster` and `Session` with `SessionManager`:

```java
// obtain the session manager
SessionManager sm = new SessionManager();
// customize driver's options via SessionManager.setXXX() methods
// remember to initialize the SessionManager
sm.init(); 

// obtain a Datastax Cluster instance
Cluster cluster = sm.getCluster("host1:port1,host2,host3:port3...", username, password);
// perform business tasks with the obtained Cluster instance

/*
 * Notes:
 * - Use one Cluster instance per Cassandra physical cluster throughout application's life. SessionManager caches Cluster instances so
 *   it's safe to call SessionManager.getCluster(...) multiple times with the same parameters.
 * - Close Cluster instances only when absolutely needed, SessionManager will close open Clusters during SessionManager.destroy() call.
 */

// obtain a Datastax Session instance
Session session = sm.getSession("host1:port1,host2,host3:port3...", username, password, keyspace);
// perform business tasks with the obtained Session instance

/*
 * Notes:
 * - Use one Session instance per keyspace throughout application's life. SessionManager caches Session instances so
 *   it's safe to call SessionManager.getSession(...) multiple times with the same parameters.
 * - Close Session instances only when absolutely needed, SessionManager will close open Sessions during SessionManager.destroy() call.
 */

// destroy the session manager at the end of application's life
sm.destroy();
```

Working with CQL is easy with helper class `CqlUtils`:

```java
// prepare a statement
PreparedStatement pstm = CqlUtils.prepareStatement(session, "INSERT INTO table (col1, col2) VALUES (?, ?)");

// prepare & execute a non-select query
CqlUtils.executeNonSelect(session, "INSERT INTO table (col1, col2) VALUES (?, ?)", value1, value2);
// or:
CqlUtils.executeNonSelect(session, pstm, value1, value2);

// named parameters are also supported (see: https://docs.datastax.com/en/developer/java-driver/3.1/manual/statements/prepared/)
PreparedStatement pstm = CqlUtils.prepareStatement(session, "INSERT INTO product (sku, description) VALUES (:sku, :desc)");
Map<String, Object> params = new HashMap<>();
params.put("sku", "324378");
params.put("desc", "LCD screen");
CqlUtils.executeNonSelect(session, pstm, params);
// or:
CqlUtils.executeNonSelect(session, "INSERT INTO product (sku, description) VALUES (:sku, :desc)", params);

// execute a select query is as simple as
ResultSet rs = CqlUtils.execute(session, pstm, value1, value2, value3...);

// consistency level can be specified for individual query
ResultSet rs = CqlUtils.execute(session, pstm, ConsistencyLevel.LOCAL_ONE, params);

// bind values to prepared statement manually: bind by index
Statement stm1 = CqlUtils.bindValues(pStm1, value1, value2, value3...);
// or bind by name
Map<String, Object> valuesAndKeys = ...;
Statement stm2 = CqlUtils.bindValues(pStm2, valuesAndKeys);

// execute a batch of statements
ResultSet rs = CqlUtils.executeBatch(session, stm1, stm2...);
```

### Notes

* As of v3.2.0, DataStax's driver requires Guava v16.0.1!

- Initialize `SessionManager` by calling `SessionManager.init()` before use.
- There is no need to close `Cluster` or `Session`. Use one `Cluster` instance per Cassandra physical cluster
  throughout application's life. Use one `Session` instance per keyspace throughout application's life.
- `SessionManager` caches opened `Cluster`s and `Session`s. It is safe to call `SessionManager.getCluster(...)` and
  `SessionManager.getSession(...)` multiple times.
- If you use compression, include appropriate jar files in classpath. See: https://docs.datastax.com/en/developer/java-driver/3.2/manual/compression/


## Credits

- [Datastax](http://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html) is the underlying Cassandra library. 


## License

See LICENSE.txt for details. Copyright (c) 2014-2017 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
