# Phase 1- Initial, public drop of `dbprove`


## Supported Data Types
Support for the following data types will be added in phase 1:

- 64-bit `DOUBLE` with all its aliases
- `VARCHAR` and `TEXT`
- `BIGINT`, `INT`, `SMALLINT`, `TINYINT`
- `DECIMAL`

## Generator Library
The following datasets and the utility classes needed to generate them will be
implemented:

- TPC-H SF1
- The "evil" dataset

### TPC-H
We will use small, fast generated datasets. The goal of DBProve is not to benchmark
the speed of TPC-H (at least not initially) it is to check if the plans and rewrites
needed for TPC-H are present in the tested database.

### The Evil Dataset
This database contains ... one... Billion... Rows...

These rows represent uniform, zipfian and skewed variants of supported data types.
This allows us to test estimation quality of the database.

The dataset also contains the interesting outliers for each data type, such as:

- Min/Max values of each type
- NaN / Inf for DOUBLE and other "odd" values for the types
- Powers of two integers and string lengths
- Zero, empty strings, null values of each type
- UTF8 sequences of length 1-4 bytes, including strings mixing these
- Large strings
- Various interesting, regex patterns
- Skippable values in ranges that will be implicitly ordered when the data is sorted
  by the unique `k` column
- Self joinable foreign keys that point at `k` with various distributions to test estimation
- Etc

These values allow us to validate if the types behave correct in terms of IEEE
standards and `DECIMAL` arithmetic and to test edge caess of optimiser behaviour.

# SQL library
The SQL Library will have support for the following engines:


| Driver     | Third Party Source            | Linking | Engines Enabled                                                                           |
|------------|-------------------------------|---------|-------------------------------------------------------------------------------------------|
| mysql      | libmariadb                    | Static  | MariaDB, MySQL, Starrocks                                                                 |
| libpq      | PostgreSQL source             | StatiC  | PostreSQL, Yellowbrick, Redshift                                                          |
| msodbc     | Installed ODBC<br/>+ unixODBC | DlOpen  | Most Microsoft Products                                                                   |
| clickhouse | clickhouse-cpp                | Static  | ClickHouse                                                                                |
| SQLite     | SQLIte source                 | Static  | SQLite                                                                                    |
| DuckDB     | DuckDB source                 | Static  | DuckDB                                                                                    | 
| Utopia     | None                          | Static  | A special Driver that returns theoretically optimal values (controlled via configuration) |

Drivers can be updated without changing the basic interface that the Theorem consumes.

Driver support implements a thin wrapper on top of whatever native driver the 
target database supports. Drivers use a zero copy abstraction to iterate over rows
that are returned from the driver

## Driver Interfaces Supported
For the initial drop of code, we suport the smallest interfaces neded for the theorems
we will run.

| Interface             | What is it?                                                                               |
|-----------------------|-------------------------------------------------------------------------------------------|
| `bulkLoad`            | Use fast load interface to move CSV into database.                                        |
| `execute`             | Run a SQL command                                                                         |
| `fetchAll`            | Runs a SQL command and returns a `Result` object that can be iterated                     |
| `translateDialectDdl` | Given a DDL statement, turn this into a into the driver/engine specific syntax            |
| `version`             | Returns a version string of the engine we are talking to                                  |
| `engine`              | Returns the engine we are talking to (Ex: MySQL or MariaDb when using their common driver |

These interfaces are defined in `connection_base.h`. Every driver must inherit this
interface.

# Runner Library
The runner library will support these operations:

- `serial` - execute a list of queries in sequence
- `single` - execute one query
- `parallelTogether` - have `n` threads coordinate on execution a set of queries
- `parallelApart` - have `n` threads each execute the same set of queries

The following measurements will be support

- **Runtimes** -  Microsecond precision measure of query runtimes
- **Avg / stddev** - For sets of queries, report avg / stdev
- **Percentile** - for set of queries, report 0.1, 1, 10, 20, ... 90, 99, 99.9 percentile runtimes
- **Result validation** - Check that the query matches the results returned by `utopia`. 
  nicely report back what the differences are if it does not

# Theorems
These theorems will be supported:

| Theorem Category | Theorem                                                                                                |
|------------------|--------------------------------------------------------------------------------------------------------|
| LOAD             | Load TPC-H (measure rough speed)                                                                       |
| CORRECT          | Check that the 22 TPC-H queries returns correct results                                                |
| REWRITE          | Validate (via runtimes) that transitive closure of pushdown works                                      |
| EE               | Validate (via runtimes) that bloom filters work                                                        |
| EE               | Join scale curve: Validate that non spilling join scale curve is linear                                |
| CLIENT           | Fetch Latency - check how many ms it takes to fetch empty result sets that do not need to touch tables |
| CLIENT           | Full roundtrip touching minimalist table                                                               |
| SE               | Haystack access - check how fast a single row can be fished out of a perfectly sorted dataset          |


In a later phase, we will support a generic plan AST parser than can create a common
query plan AST from any of the support engines. When that feature is present, we can 
do a deeper level validation to more interesting facts about rewrites and plan shapes.

# Docs
The methods in all libraries will be documented with Doxygen doc strings.

All public facing method will be documented and `README.md` files provided
describing how to extend each of the library that make up `dbprove`.

For the initial drop, we will *not* auto generate documentation to websites.


