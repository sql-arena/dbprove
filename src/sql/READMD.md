# The `sql` lib

This library provides a unified abstraction of SQL on top of multiple database engines.

## Current Drivers

This is the current state of the drivers:

| Engine                    | Canonical Name |  SQL | EXPLAIN | Known Limitations                             |
|:--------------------------|:---------------|-----:|:-------:|:----------------------------------------------|
| **PostgreSQL**            | PostgreSQL     |   ✔️ |   ✔️    | No multi results                              |
| **MySQL**                 | MariaDB        |   ✔️ |         |                                               |
| **SQLite**                | SQLite         |   ✔️ |    ️    |                                               |
| **SQL Server**            | SQL Server     | ️ ✔️ |   ✔️    | Only ODBC access, needs ODBC driver installed |
| **DuckDB**                | DuckDB         |    ️ |   ✔️    |
| **ClickHouse on Premise** | ClickHouse     | ️ ✔️ |   ✔️    |
| **Yellowbrick Data**      | Yellowbrick    | ️ ✔️ |   ✔️    |
| **Databricks SQL**        | Databricks     | ️ ✔️ |         | Explain has no `actual` values                |

## Central Objects and Usage

These are the objects you generally need to interact with:

- `ConnectionFactory` - Creates `Connection` objects. Use this to build `Connection` objects.
- `ConnectionBase` - Represents a connection to a database. Use this to execute SQL statements.
- [`ResultBase`](./include/dbprove/sql/result_base.h) - Represents the result of a SQL statement. Use this to iterate
  over the results via the `rows()` method.
- [`RowBase`](./include/dbprove/sql/row_base.h) - Represents a row of a result set. Use this to access individual
  columns and values.
- [`SqlVariant`](./include/dbprove/sql/sql_type.h) - Represents a single value in a `RowBase`
- `Plan` - Represents the execution plan of a SQL statement. Use this to inspect the query plan.

Driver implementors provide subclasses of `ConnectionBase`, `ResultBase` and `RowBase` to support their
specific database engine.

From the perspective of `sql` lib consumers - what driver you’re talking to is entirely transparent.

Consumers of `sql` should consume a single header:

```c++
#include <sql/sql.h>
```

### Example: Opening A Connection to PostgreSQL and running Explain

Notice how we can pass an alias to the Engine here
(in this case: `Postgres` instead of the canonical name: `PostgreSQL`)

```c++
  sql::ConnectionFactory factory(
      sql::Engine("Postgres"),
      sql::CredentialPassword("localhost",
                              "postgres",
                              5432,
                              "postgres",
                              "password"));
  // Create an open connection
  std::unique_ptr<ConnectionBase> conn = factory.create();
  
  std::unique_ptr<Plan> plan = conn->explain("SELECT * FROM mytable WHERE id = 1");
```

Note the unique pointers — once you call a method you own the memory this method returns.

### Example: Iterating over a `ResultBase`

```c++
  sql::ConnectionFactory factory(
      sql::Engine("Postgres"),
      sql::CredentialPassword("localhost",
                              "postgres",
                              5432,
                              "postgres",
                              "password"));
  // Create an open connection
  std::unique_ptr<ConnectionBase> conn = factory.create();
   
  std::unique_ptr<ResultBase> result = conn->fetchAll("SELECT * FROM mytable WHERE id = 1");
  
  for (RowBase& row : result->rows() ) {
    std::cout << row[0].asString()
  }
```

## Implementing new Drivers

Follow this process:

- Copy the `boilerplate` directory into `/sql` and name it after your driver. Use lowercase naming.
    - The `.cpp` files here should give you a good idea of what to do.
    - Look at other drivers for inspiration.
- In [`engine.h`](./include/dbprove/sql/engine.h) extend the `Engine` enum with a new value representing your
  driver.
    - Use the canonical name that the engine is officially known by — including casing.
- in [`engine.cpp`](./engine.cpp) extend the defaults with your driver
    - You can provide additional aliases for your driver (for example: `mysql` for `mariadb`)
    - If you want credentials or other defaults to be read from an environment variable, you can do it here.
    - See the file for examples and details
- Extend [`connection_factory.cpp`](./connection_factory.cpp) so it can construct your engine
- Run the tests in `./test` and extend if you have special requirements (or want to increase test coverage)
- Update this file in the “Current Drivers” section above
- Create a `[driver]/README.md` file in the `sql` directory with a short description of your driver.
    - Include any special requirements (like needing an ODBC driver installed)
    - Include relevant links to the engine’s website and documentation
    - Include any known limitations
- Finally, file a PR against the repo with your changes

When implementing engines, it is crucial you don’t pollute the namespace or linkage.

Keep your engine specific header `#include` inside your `.cpp` files — using
[PImpl](https://en.cppreference.com/w/cpp/language/pimpl.html) idioms if needed.
Lots of different headers and libraries are linked to support SQL, so we want to keep things modular.

You can create drivers that can’t parse canonical explain (per the `explain(std::string_view)` interface).
If you do a partial implementation like this, please note this in the PR so someone else can implement it later.

## Driver File Structures

Inside the driver directory, assuming you used the `boilerplate` directory as a template,
you should have the following files:

| File          | Purpose                                                                                                                                          |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `row.h`       | Typically, you won't need to do anything here. Just pass the `Result` reference  The purpose of the file is to be the return value of iterators. |
| `row.cpp`     | Here, you generally forward the call of the methods into the 'Result` reference                                                                  | 
| `result.h`    | Again, mostly just boilerplate. You generally want everything inside the `impl_`                                                                 |
| `result.cpp`  | Handle all result scrolling and iteration here. Particularly in `nextRow()`                                                                      |
| `parsers.h`   | An internal header. You are likely to need engine specific parsers translating to `SqlVariant`. Put them here                                    |
| `explain.cpp` | The implementation of `Connection::Explain` lives here                                                                                           |

## Expression parsing

The file `sql/expression.css` does a simple tokenising of SQL expressions. All expressions passed into constructors
of `sql::explain::Node` will be parsed this way.

To keep query plans in a canonical form, you may need to handle special cases in `sql/expression.cpp`.
Even though these cases are engine specific - they typically generalise and are used in multiple engines.
Hence, feel free to suggest changes to the expression parser.

For the rare cases where general expression parsing isn't sufficient (example: the `error()` function in DuckDB)
you can fix the expression before constructing a `sql::explain::Node` object.`

## Logging

The `sql` library shares the same log as `dbprove` which is located in `[pwd]/logs/dbprove.log`

First: Do *not* excessively log stuff.
This isn’t some dumbass microservice with Java developers who can’t use a debugger hacking at it.

Logging is done by appending to `PLOG` like this:

```c++
PLOGI << "This is a log message"
```

Errors and warnings are logged with `PLOGE` and `PLOGW` respectively.

What to log:

- Status of large operations — like loading a full table.
- Connection issues.
- Status of external libraries — such as ODBC drivers and loader things brought in during runtime.
- Issue with plan parsing.
- Bulk loads format errors - log the first 1-3 issues and then stop spewing.

Logging individual SQL Statements is not recommended.

If you must, put those in `[pwd]/logs/sql.log`

## Exception Handling

The `sql` library turns all exceptions from the underlying drivers into subclasses of `sql::Exception`.

This means that exception handling can be unified across all drivers.

A great many ways things can go wrong in the various engines.
Because of this, the exception handling code is, and will remain for a long time, a work in progress.
Please feel free to contribute tests and fixes that will help us unify and cover this part of the code.
This is a great way to get involved with the project doing small-scale contribution and getting yourself
familiar with the codebase.

### Note: on exceptions vs return codes

I’m well aware modern C++ projects often prefer return codes over exceptions.
I have a great deal of sympathy for this style of programming.

However, on balance I think exceptions are more widely understood
by the community — particularly programmers who aren’t yet deeply familiar with C++.

Hence, its `sql::Excpetion` and its subclasses for this project.

## Note: on Async Programming

The initial version of this library does not support async programming.
I am a database programmer by trade, so I understand the inherent scalability limits of threads and processes.

However, to do the simple benchmarking we currently require — particularly the focus
on plan parsing — I believe a synchronous model is easier to understand for contributors.

As we ramp up the test cases and look into high concurrency, we may revisit this decision and add async support.

## Implementing Explain Parsing

Bringing `EXPLAIN` into a unified model requires a great deal of query optimiser knowledge.

In general, every single database can be translated into the classes represents in `sql/explain`.

The implementation of `EXPLAIN` parsing should be done in the file `sql/[driver]/explain.cpp`.
This file implements `Connection::explain` and it on purpose kept
separate from `sql/[driver]/connection.cpp` to keep PR that work on explain from creating necessary merge conflicts.

It also keeps code that is typically messy in a separate file.

A full treatment on how to translate `EXPLAIN` from different engines will not fit in this document.
If you have an interest in contributing - please feel free to reach out directly to “The Database Doctor"
[via LinkedIn](https://www.linkedin.com/in/thomaskejser).


