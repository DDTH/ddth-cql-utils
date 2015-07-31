ddth-cql-utils
==============

DDTH's CQL Utilities: simplify CQL's usage.

Project home:
[https://github.com/DDTH/ddth-cql-utils](https://github.com/DDTH/ddth-cql-utils)

OSGi environment: `ddth-cql-utils` is packaged as an OSGi bundle.


## Installation #

Latest release version: `0.2.3`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-cql-utils</artifactId>
	<version>0.2.3</version>
</dependency>
```


## Usage ##

```java
// obtain the session manager
SessionManager sm = new SessionManager();
sm.init();

// obtain a Datastax Session instance
Session session = sm.getSession("host1:port1,host2,host3:port3", username, password, keyspace);

//perform some queries
CassandraUtils.executeNonSelect(session, "INSERT INTO table (col1, col2) VALUES ('value1', 2)");

ResultSet rs = CassandraUtils.executeSelect(session, "SELECT * FROM table WHERE col1=? OR col2=?", param1, param2);

sm.destroy(); //destroy the session manager when done
```


## Credits ##

- [Datastax](http://www.datastax.com/download#dl-datastax-drivers) is the underlying Cassandra library. 


## License ##

See LICENSE.txt for details. Copyright (c) 2014-2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
