# ddth-cql-utils

Wrap around [DataStax's Cassandra Java Driver](http://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html)
and simplify the usage of CQL.

Project home:
[https://github.com/DDTH/ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils)

**`ddth-cql-utils` requires Java 8+ since v0.3.0**


## Installation

Latest release version: `0.4.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-cql-utils</artifactId>
	<version>0.4.0</version>
</dependency>
```

`ddth-cql-utils` supports [Datastax Java Driver for Apache Cassandra](https://docs.datastax.com/en/developer/java-driver/3.6/) as well as [DataStax Enterprise Java driver](https://docs.datastax.com/en/developer/java-driver-dse/1.7/). [`ddth-cql-utils' pom`](pom.xml) marks those drivers "optional", you need to include the dependencies for those drivers in your application.

Datastax Java Driver for Apache Cassandra:

```xml
<dependency>
    <groupId>com.datastax.cassandra</groupId>
    <artifactId>cassandra-driver-core</artifactId>
    <version>${version}</version>
</dependency>
```

DataStax Enterprise Java driver:

```xml
<dependency>
    <groupId>com.datastax.dse</groupId>
    <artifactId>dse-java-driver-core</artifactId>
    <version>${version}</version>
</dependency>
```


## Usage

**Create and initialize `SessionManager`:**

```java
SessionManager sm = new SessionManager();

/*
 * Optional: customize the SessionManager via SessionManager.setXXX() methods
 *
 * // limit maximun number of async-jobs
 * sm.setMaxAsyncJobs(10244);
 *
 * // value for the default Cluster/Session
 * sm.setDefaultHostsAndPorts("host1:9042,host2,host3:19042")
 *   .setDefaultUsername("cassandra")
 *   .setDefaultPassword("secret")
 *   .setDefaultKeyspace(null);
 *
 * Other settings: call SessionManager.setXXX()
 */

// remember to initialize the SessionManager
sm.init(); 
```

**Manage Cassandra `Cluster` and `Session` with `SessionManager`**

```java
// obtain a Cluster instance
Cluster cluster = sm.getCluster("host1:port1,host2,host3:port3...", username, password);
// defaultHostsAndPorts, defaultUsername and defaultPassword are used
Cluster defaultCluster = sm.getCluster();

// obtain a Session instance
Session session = sm.getSession("host1:port1,host2,host3:port3...", username, password, keyspace);
// defaultHostsAndPorts, defaultUsername, defaultPassword and defaultKeyspace are used
Session defaultSession = sm.getSession();
```

Notes:

- Use one `Cluster` instance per Cassandra physical cluster throughout application's life. Close `Cluster` instances only when absolutely needed! `SessionManager` will close open `Cluster`s when `SessionManager.destroy()` is called.
- `SessionManager` caches `Cluster` instances, so it's safe to call `SessionManager.getCluster(...)` multiple times with the same parameters.
- Use one `Session` instance per keyspace throughout application's life. Close `Session` instances only when absolutely needed! `SessionManager` will close open `Session`s when `SessionManager.destroy()` is called.
- `SessionManager` caches `Session` instances, so it's safe to call `SessionManager.getSession(...)` multiple times with the same parameters.
- If application works with multiple keyspaces, it is recommended to create a `Session` with no associated keyspace (`Session session = sm.getSession("hostsAndPorts", "username", "password", null)`) and prefix table name with keyspace name in all queries (e.g. `session.execute("SELECT count(*) FROM my_keyspace.my_table")`).

**Working with CQL is easy with helper class `CqlUtils`**

```java
// prepare a statement
PreparedStatement pstm = CqlUtils.prepareStatement(session, "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)");
// execute a non-select query
CqlUtils.executeNonSelect(session, pstm, value1, value2);

// or shorthand for both
CqlUtils.executeNonSelect(session, "INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)", value1, value2);

// named parameters are also supported (see: https://docs.datastax.com/en/developer/java-driver/3.6/manual/statements/prepared/)
PreparedStatement pstm = CqlUtils.prepareStatement(session, "INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)");
Map<String, Object> params = new HashMap<>();
params.put("sku", "324378");
params.put("desc", "LCD screen");
CqlUtils.executeNonSelect(session, pstm, params);
// or:
CqlUtils.executeNonSelect(session, "INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)", params);
```

Note: Since `v0.4.0` running queries against the default `Cluster/Session` via `SessionManager` is preferred. The code snippet above can be rewritten as the following:

```java
// prepare a statement
PreparedStatement pstm = sm.prepareStatement("INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)");
// execute a non-select query
sm.executeNonSelect(pstm, value1, value2);

// or shorthand for both
sm.executeNonSelect("INSERT INTO keyspace.table (col1, col2) VALUES (?, ?)", value1, value2);

// named parameters are also supported (see: https://docs.datastax.com/en/developer/java-driver/3.6/manual/statements/prepared/)
PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)");
Map<String, Object> params = new HashMap<>();
params.put("sku", "324378");
params.put("desc", "LCD screen");
sm.executeNonSelect(pstm, params);
// or:
sm.executeNonSelect("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :desc)", params);
```

Execute SELECT query and process result:

```java
// execute a SELECT query and get back a ResultSet
ResultSet rs = sm.execute("SELECT sku, description FROM my_keyspace.product", value1, value2, value3...);

// consistency level can be specified for individual query
ResultSet rs = sm.execute("SELECT sku, description FROM my_keyspace.product", ConsistencyLevel.LOCAL_ONE, params);

for (Row row : rs) {
    System.out.println(row);
}
```

Bind values manually and execute batch:

```java
// bind by index
PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (?, ?)");
Statement stm1 = CqlUtils.bindValues(pstm, value1, value2);

// or bind by name
PreparedStatement pstm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (:sku, :description)");
Statement stm1 = CqlUtils.bindValues(pstm, value1, value2);

Map<String, Object> valuesAndKeys = ...;
Statement stm2 = CqlUtils.bindValues(pstm, valuesAndKeys);

// execute a batch of statements
ResultSet rs = sm.executeBatch(stm1, stm2...);
```

Execute query asynchronously (see [https://docs.datastax.com/en/developer/java-driver/3.6/manual/async/](https://docs.datastax.com/en/developer/java-driver/3.6/manual/async/)):

```java
import com.google.common.util.concurrent.*;

// Use transform with an AsyncFunction to chain an async operation after another:
ListenableFuture<ResultSet> resultSet = Futures.transform(session,
    new AsyncFunction<Session, ResultSet>() {
        public ListenableFuture<ResultSet> apply(Session session) throws Exception {
            return CqlUtils.executeAsync(session, "SELECT release_version FROM system.local");
        }
    });

// Use transform with a simple Function to apply a synchronous computation on the result:
ListenableFuture<String> version = Futures.transform(resultSet,
    new Function<ResultSet, String>() {
        public String apply(ResultSet rs) {
            return rs.one().getString("release_version");
        }
    });

// Use a callback to perform an action once the future is complete:
Futures.addCallback(version, new FutureCallback<String>() {
    public void onSuccess(String version) {
        System.out.printf("Cassandra version: %s%n", version);
    }

    public void onFailure(Throwable t) {
        System.out.printf("Failed to retrieve the version: %s%n",
            t.getMessage());
    }
}, myCustomExecutor);
```

Or, a simpler way with `SessionManager`:

```java
import com.google.common.util.concurrent.*;

FutureCallback<ResultSet> future = new FutureCallback<ResultSet>() {
    @Override
    public void onSuccess(ResultSet result) {
        for (Row row : result) {
            System.out.println(row);
        }
    }
    
    @Override
    public void onFailure(Throwable t) {
        t.printStacktrace();
    }
};
PreparedStatement stm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (?, ?)");
sm.executeAsync(future, stm, sku, desc);
```

`SessionManager` limits the number of asyn-jobs that can be executed at a time (default value is `100`, set via `SessionManager.setMaxAsyncJobs(int)` _before_ `SessionManager.init()` is called).
If number of async-jobs exceeds this value, `ExceedMaxAsyncJobsException` is thrown. The query can be retries within the callback, like this:


```java
import com.google.common.util.concurrent.*;

PreparedStatement stm = sm.prepareStatement("INSERT INTO my_keyspace.product (sku, description) VALUES (?, ?)");
FutureCallback<ResultSet> future = new FutureCallback<ResultSet>() {
    @Override
    public void onSuccess(ResultSet result) {
        for (Row row : result) {
            System.out.println(row);
        }
    }
    
    @Override
    public void onFailure(Throwable t) {
        if ( t instanceof ExceedMaxAsyncJobsException ) {
            this.sm.executeAsync(this, this.stm, this.sku, this.desc);
        } else {
            t.printStacktrace();
        }
    }
    
    private SessionManager sm;
    private PreparedStatement stm;
    private String sku, desc;
    public new FutureCallback<ResultSet> init(SessionManager sm, PreparedStatement stm, String sku, String desc) {
        this.sm = sm;
        this.stm = stm;
        this.sku = sku;
        this.desc = desc;
        return this;
    }
}.init(sm, stm, sku, desc);
sm.executeAsync(future, stm, sku, desc);
```

**Work with DataStax Enterprise Server**

If you use Cassandra-only features, `SessionManager` can work with DSE Server without problem. However, when you need DSE-only feature (such as graph, analytics or search), you need to use `DseSessionManager`.

`DseSessionManager` can be a drop-in replacement for `SessionManager` (see [https://docs.datastax.com/en/developer/java-driver-dse/1.7/faq/](https://docs.datastax.com/en/developer/java-driver-dse/1.7/faq/)), with a few differences:

- `DseSessionManager.getCluster(...)` return `DseCluster` which is a sub-class of `Cluster`.
- `DseSessionManager.getSession(...)` return `DseSession` which is a sub-class of `Session`.
- DSE allows a user to connect as another user or role (["proxy authentication"](https://docs.datastax.com/en/developer/java-driver-dse/1.7/manual/auth/)). Two overload methods `getCluster(...)` and `getSession(...)` are introduced in `DseSessionManager` to support this feature.


### Notes

- DataStax's driver requires `Guava` with minumum version `16.0.1+`, `19.0+` is recommended for better performance
- Initialize `SessionManager` by calling `SessionManager.init()` before use.
- There is no need to close `Cluster` or `Session`:
  - Use one `Cluster` instance per Cassandra physical cluster throughout application's life.
  - Use one `Session` instance per keyspace throughout application's life. However, is it recommended to create just one `Session`
    instance with no associated keyspace and use `keyspace_name.table_name` format in all queries.
- `SessionManager` caches opened `Cluster`s and `Session`s. It is safe to call `SessionManager.getCluster(...)` and
  `SessionManager.getSession(...)` multiple times (e.g. `SELECT count(*) FROM my_keyspace.my_tabble`).
- If you use compression, include appropriate jar files in classpath. See: [https://docs.datastax.com/en/developer/java-driver/3.6/manual/compression/](https://docs.datastax.com/en/developer/java-driver/3.6/manual/compression/)


## Credits

- [Datastax](http://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html) is the underlying Cassandra library. 


## License

See LICENSE.txt for details. Copyright (c) 2014-2018 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
