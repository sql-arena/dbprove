# PostgreSQL connector

Currently, this connector uses `libpq` - which is dog slow. However, it is the library that is most commonly 
used to access PostgreSQL (it is the underpinnings of most publicly shipped JDBC and ODBC drivers).

`libpq` lacks a separate, standalone repo - which is sad. This means that we need to `vcpkg` all of the `PostgreSQL`
which is a rather beefy library

Note that when benchmarking, you will need to change the default configuration of PostgreSQL, the reference script is
in `configure.sql`. For testing the connector, use `configure_test.sql`

