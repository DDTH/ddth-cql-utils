# ddth-cql-utils

Wrap around [DataStax's Cassandra Java Driver](http://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html)
and simplify the usage of CQL.

Project home:
[https://github.com/DDTH/ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils)

**`ddth-cql-utils` requires Java 11+ since v1.0.0, for Java 8, use v0.x.y**


## Installation

Latest release version: `1.0.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-cql-utils</artifactId>
	<version>1.0.0</version>
</dependency>
```

`ddth-cql-utils` supports [Datastax Java Driver for Apache Cassandra](https://docs.datastax.com/en/developer/java-driver/4.0/) as well as [DataStax Enterprise Java driver](https://docs.datastax.com/en/developer/java-driver-dse/2.0/). [`ddth-cql-utils' pom`](pom.xml) marks those drivers "optional", you need to include the dependenc for those drivers in your application.


## Usage

(See [working example code](src/test/java/com/github/ddth/cql/qnd))

**Overall flow**

```java
/* Create and initialize `SessionManager` */
SessionManager sm = new SessionManager();

//// Optional: customize the SessionManager
// limit maximun number of async-jobs
sm.setMaxAsyncJobs(1024);
// default values to connect to Cassandra cluster
sm.setDefaultHostsAndPorts("host1:9042,host2,host3:19042").setDefaultKeyspace("test");
// override some configurations
sm.setConfigLoader(DriverConfigLoader.fromFile("my-application.conf"));

// remember to initialize the SessionManager
sm.init(); 

/*Obtain `CqlSession` and use it*/
// obtain a CqlSession instance, specifying contact points and keyspace
CqlSession session = sm.getSession("host1:port1,host2,host3:port3", keyspace);
// obtain a CqlSession instance, using defaultHostsAndPorts and defaultKeyspace
CqlSession defaultSession = sm.getSession();

/*
CqlSession instance is now ready to use.
See [atastax Java Driver for Apache Cassandra (https://docs.datastax.com/en/developer/java-driver/4.0/) for usage manual.
*/

/* Before application exists: cleanup */
sm.close(); //or sm.destroy();
```

**Performance Practices**

- Use one `CqlSession` instance per keyspace throughout application life.
Close `CqlSession` only when absolutely needed! `SessionManager` will close
open `CqlSession` instances when `SessionManager` is closed.
- `SessionManager` caches `CqlSession` instances,
so it's safe to call `SessionManager.getSession(...)` multiple times with the same parameters.
- If application works with multiple keyspaces,
it is recommended to create a `CqlSession` with no default keyspace
(e.g. `CqlSession session = sm.getSession("hostsAndPorts", null)`) and prefix table name
with keyspace name in all queries (e.g. `session.execute("SELECT count(*) FROM my_keyspace.my_table")`).

**Execute CQL Queries with SessionManager**

Although CQL queries can be executed using obtained `CqlSession` instance, it is recommended to execute CQL using `SessionManager`.

> `SessionManager` uses the `CqlSession` instance initialized with `defaultHostsAndPorts` and `defaultInstance` to execute CQL queries.

Prepare and execute CQL:

```java
PreparedStatement pstm = sm.prepareStatement("INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)");
sm.execute(pstm, value1, value2);

// or shorthand version
sm.execute("INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)", value1, value2);
```

Using [named parameters](https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/statements/prepared/):

```java
Map<String, Object> params = new HashMap<>();
params.put("sku", "324378");
params.put("desc", "LCD screen");

PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)");
sm.execute(pstm, params);

// or shorthand version:
sm.execute("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)", params);
```

Execute SELECT query and process result:

```java
// execute a SELECT query and get back a ResultSet
ResultSet rs = sm.execute("SELECT sku, description FROM my_keyspace.product", value1, value2, value3...);

// consistency level can be specified for individual query
ResultSet rs = sm.execute("SELECT sku, description FROM my_keyspace.product", ConsistencyLevel.LOCAL_ONE, params);

// loop through the result
for (Row row : rs) {
    System.out.println(row);
}
```

Bind values manually and execute batch:

```java
// bind by index
PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (?, ?)");
Statement stm1 = sm.bindValues(pstm, value1, value2);

// or bind by name
PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :description)");
Map<String, Object> valuesAndKeys = ...;
Statement stm2 = sm.bindValues(pstm, valuesAndKeys);

// execute a batch of statements
ResultSet rs = sm.executeBatch(stm1, stm2);
```

Execute query asynchronously (more information at [Datastax Manual Page](https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/async/)):

```java
// execute a SELECT query asynchronously
String cql = "SELECT * from keyspace.products WHERE sku=?";
CqlSession session = sm.getSession(...);
CompletionStage<AsyncResultSet> stage = CqlUtils.executeAsync(session, cql, "12345");
stage.whenComplete((resultSet, error) -> {
    if (error != null) {
        error.printStackTrace();
    } else {
        System.out.println("First row: " + resultSet.one());
    }
});

// or using callback to process result
Callback<AsyncResultSet> callback = new Callback<>() {
    public void onSuccess(AsyncResultSet result) {
        System.out.println("First row: " + resultSet.one());
    }
    public void void onFailure(Throwable t) {
        t.printStackTrace();
    }
};
sm.executeAsync(callback, cql, "12345");
```

> `SessionManager` limits the number of asyn-jobs that can be executed concurrently
> (default value is `100`, it can be changed by calling `SessionManager.setMaxAsyncJobs(int)` _**before**_ `SessionManager.init()` is called).
>
> If number of async-jobs exceeds this value, `ExceedMaxAsyncJobsException` is passed to `Callback.onFailure()`.

**Work with DataStax Enterprise Server**

If application uses Cassandra-only features, `CqlSession` can work with DSE Server.
However, `DseSession` is required to work with DSE-features (such as graph, analytics or search).
`ddth-cql-utils` offers `DseSessionManager` to manage `DseSession` instance. Its usage is similar to `SessionManager`:

```java
DseSessionManager sm = new DseSessionManager();

//// Optional: customize the SessionManager
// limit maximun number of async-jobs
sm.setMaxAsyncJobs(1024);
// default values to connect to Cassandra cluster
sm.setDefaultHostsAndPorts("host1:9042,host2,host3:19042").setDefaultKeyspace("test");
// override some configurations
sm.setConfigLoader(DseDriverConfigLoader.fromFile("my-dse-application.conf"));

// remember to initialize the SessionManager
sm.init(); 

/*Obtain `DseSession` and use it*/
// obtain a DseSession instance, specifying contact points and keyspace
DseSession session = sm.getSession("host1:port1,host2,host3:port3", keyspace);
// obtain a DseSession instance, using defaultHostsAndPorts and defaultKeyspace
DseSession defaultSession = sm.getSession();

/*
DseSession instance is now ready to use.
See Datastax Enterprise Java Driver (https://docs.datastax.com/en/developer/java-driver-dse/2.0/) for usage manual.
*/

/* Before application exists: cleanup */
sm.close(); //or sm.destroy();
```

> `DseSessionManager` extends `SessionManager` and can be used as a drop-in replacement, with a few differences:
> - `DseSessionManager.getSession(...)` returns `DseSession` instead of `CqlSession`. Again, `DseSession` extends `CqlSession` and can be used as a drop-in replacement.
> - Use `DseDriverConfigLoader.fromXXX(...)` to load configurations for `DseSessionManager.setConfigLoader(DriverConfigLoader)`.

## Credits

- [Datastax](https://docs.datastax.com/en/driver-matrix/doc/driver_matrix/javaDrivers.html) is the underlying Cassandra library. 


## License

See LICENSE.txt for details. Copyright (c) 2014-2019 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
