# Phase 1- Initial `dbprove` - validating optimisers and data types
The goal of this phase is to quantify the power of the following query planners and query rewrites:

- Postgres 17
- Yellowbrick latest
- DuckDb

Additionally, we wish to explore areas where Yellowbrick has gone to great lengths to implement correctness of types and 
edge cases.

The minimal feature set to support these outcomes is as follows:

## Supported Data Types
Support for the following data types and their aliases will be added in phase 1:

- 64-bit `DOUBLE` 
- `VARCHAR` and `TEXT`
- `BIGINT`, `INT`, `SMALLINT`, `TINYINT`
- `DECIMAL` 
- `FLOAT4` and `FLOAT8` along with their aliases

For now, we will explicitly move `CHAR` out of scope. We can revisit that type later.

## Generator Lib
The following datasets and the utility classes needed to generate datasets will be
implemented:

- TPC-H SF1
- The "evil" dataset

### TPC-H
We will use small, fast generated datasets so many theorems can be proven quickly. Initially, the goal of `dbprove` is 
not to benchmark the speed of TPC-H, it is to check if the plans and rewrites needed for TPC-H are present in the tested 
database.

### The Evil Table
This table contains ... one... Billion... Rows...

These rows represent uniform, zipfian, bi-modal and extremely skewed variants of supported data types.
This allows us to test estimation quality of database optimisers.

The dataset also contains the interesting outliers for each data type, such as:

- Min/Max values of each type
- NaN / Inf for `FLOAT` and other "odd" values for the types
- Powers of two integers and string lengths
- Zero, empty strings, null values of each type
- UTF8 sequences of length 1-4 bytes, including strings mixing these
- Hex binary sequences that cast to invalid UTF8 strings
- Large strings
- Various interesting, regex patterns
- Skippable values in ranges that will be implicitly ordered when the data is sorted
  by the unique `k` column
- Self-joinable foreign keys that point at `k` with various distributions to test estimation
- Etc

These values allow us to validate if the types behave correctly in terms of IEEE
standards and arithmetic edge cases. It also allows foundational testing of optimiser quality.

The `evil` table will be expanded over time so that all theorems that need artificial data can rely on a single 
test dataset which eases future theorem creation

# SQL library
The SQL Library will have support for the following drivers and their associated engines:


| Driver     | Third Party Source            | Linking | Engines Enabled                                                                           |
|------------|-------------------------------|---------|-------------------------------------------------------------------------------------------|
| libpq      | PostgreSQL source             | StatiC  | PostreSQL, Yellowbrick, Redshift                                                          |
| DuckDB     | DuckDB source                 | Static  | DuckDB                                                                                    | 
| Utopia     | None                          | Static  | A special Driver that returns theoretically optimal values (controlled via configuration) |

Drivers can be updated without changing the basic interface that the Theorem consumes. The initial phase of `dbprove`
establishes the basic API that `sql lib` provides to the world.

Driver support implements a thin wrapper on top of whatever native driver the 
target database supports. Drivers use a zero copy abstraction to iterate over rows
that are returned from the driver.

THe initial drivers are chosen to evaluate the behaviour of DuckDb vs Postgres vs Yellowbrick. Particular care will
be taken to handle plan and expression comparing.

## Driver Interfaces Supported
We will support the smallest interfaces needed for phase 1 theorems. The interfaces are

| Interface             | What is it?                                                                                |
|-----------------------|--------------------------------------------------------------------------------------------|
| `bulkLoad`            | Use fast load interface to move CSV into database. Fall back to `INSERT` if unavailable.   |
| `execute`             | Run a SQL command                                                                          |
| `fetchAll`            | Runs a SQL command and returns a `Result` object that can be iterated                      |
| `translateDialectDdl` | Given a DDL statement, turn this into a into the driver/engine specific syntax             |
| `version`             | Returns a version string of the engine we are talking to                                   |
| `engine`              | Returns the engine we are talking to (Ex: MySQL or MariaDb when using their common driver) |
| `explain`             | Returns a unified AST used to compare plans across engines                                 |


These interfaces are defined in `connection_base.h`. Every driver must inherit this
interface.

### Driver Exceptions
Drivers must throw typed exceptions that inherit from `sql::Exception`. The exact exceptions we will need to throw
will be fleshed out during development and discussed as needed with the contributors. 

Drivers are never allowed to throw an exception directly from the driver infrastructure, it must always translate the
driver thrown exception to a more generic, cross driver subclass of `sql::Exception`.

For example, `libpq` (the connector for PostgreSQL) may send this error code back to the caller:

```42601 syntax_error```

This error must be handled in the driver and the following must occur to the outside caller:

```c++
throw sql::SyntaxErrorException(...);
```

# Runner Library
The runner library will support these operations:

- `serial` - execute a list of queries in sequence and generate aggregated instrumentation
- `single` - execute and measure one query
- `parallelTogether` - have `n` threads coordinate on the execution a set of queries
- `parallelApart` - have `n` threads each execute the same set of queries
- `analyse` - Run the query, collect the plan and provide details plan instrumentation

The following measurements will be support

- **Runtimes** -  Microsecond precision measures of query runtimes
- **Avg / stddev** - For sets of queries, report avg / stddev
- **Percentile** - for set of queries, report 0.1, 1, 10, 20, ... 90, 99, 99.9 percentile runtimes
- **Result validation** - Check that the query matches the results returned by `utopia`. 
   nicely report, in human-readable form, the differences if it doesn't.
- **Explain Stats** - provides statistics for total scanned rows, pushdown efficiency, join rows, aggregate rows and sort rows

# Theorems
These theorems will be supported:

| Theorem Category | Theorem                                                                                                                                                                  |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| LOAD             | Load TPC-H (also allows us to test check for bulk capability)                                                                                                            |
| CORRECT          | Check that the 22 TPC-H queries returns correct results                                                                                                                  |
| CORRECT          | Edge cases for aggregation functions such as `AVG` over **NULL** values, `COUNT` of empty results, `COUNT DISTINCT` of **NULL** values etc.                              |
| CLI              | Fetch Latency - check how many ms it takes to fetch empty result sets that do not need to touch tables. This will be used for quick testing of engine connection strings |
| CLI              | Full roundtrip touching minimalist table. Anoter quick test to make sure connection and query handling works well                                                        |
| REWRITE          | Automated plan analysis of  transitive closure of pushdown equalities and inequalitis in complex queries                                                                 |
| REWRITE          | Automated plan analysis to detect rewrites for moving expression to optimal part of query                                                                                |
| PLAN             | Measure and validate all 22 plans in TPC-H against the optimal plan allowing analysis of medium complexity plans                                                         |
| PLAN             | Plan highly complex queries with up to 100 joins, measuring ability of planner to deal with large join trees                                                             |
| PLAN             | Automated plan analysis to check estimate quality of filters, joins, aggregates. Explores both skewed and uniform estimation                                             |
| TYPE             | `DECIMAL` and `FLOAT` handling of arithmetic, INF/NaN, Truncation, Rounding, precision. Also includes aggregation function correctness                                   |
| TYPE             | `VARCHAR` handling of UTF8 sequences with focus on dealing with corrupt sequences and edge cases                                                                         |
| EE               | Highly complex expressions with deep stack nesting, looking for OOM and other cases where evaluation can fail                                                            |
| EE               | Automated plan analysis of bloom filter usage along with measured impact at runtime                                                                                      |

To enable automated analysis, a generic plan AST parser will be implemented supporting parsing of the following plan components:

- Sort
- Join
- Group By
- Scan
- Projection
- Filters used in any of the above

These plan components are generic across YB, PG, DuckDB (and all others I have seen too, except BQ). The generic plan AST
will allow us to easily benchmark engines and planners against each other to find plan diffs. 

For phase 1, the following will *not* be in scope for correctness/plan validation:

- Window aggregates
- Rollup and cube aggregation

# Docs
Public facing methods in all libraries will be documented with Doxygen doc strings. Phase1 is not 
complete without this.

All public facing method will be documented. A `README.md` files will be provided, describing how to extend each of the 
libraries that make up `dbprove`.

For phase1, we will *not* auto generate documentation to websites, nor will we do uploads of results to a SQL Arena.


# `main.cpp`
The basic command line interface will be established in Phase1. See `/main.cpp`, specifically the `CLI` parsing
for details.

The tool will be able to generate two output formats:

- Parquet - outputs all theorems in a tabular stream that can be used for automated analysis
- `.md` - Combines tool output into a single MD file that can be used for human-readable reporting


