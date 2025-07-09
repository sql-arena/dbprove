# dbprove architecture overview

`dbprove` is made of the following components

- **generator** - data generation
- **sql** - connectors to various database engines providing a unified interface
- **runner** - execution methods to run and measure queries
- **theorems** - the things that dbprove about the target database

# Project Planing
`dbprove` gets planned in phases, which are roughly timeboxed deadlines. Each phase starts with the writing of 
a spec for the new functionality that is to be added.

Specs source their requirements from stakeholders as well as feature requests file on GitHub.

Once the spec is written, it is discussed with shareholders and eventually declared final.

Development then proceeds according to spec.

Once a phase ends, the spec for that phase gets merged into the documentation files in this directory and into the 
individual `README.md` files the libraries that were changed.

The current phase is here:

- [Phase 1](phases/phase1.md)

Also see: [Terminology](terminology.md)

Here follows an overview of the modules making up `dbprove`

# Generator
The [generator](../generator) library is responsible for generating CSV streams 
or files that can be loaded directly into databases. 

It will dump files into the a `./data/` directory that `dbprove` was executed from.

Various classes are available in the generator library to construct distributions of
different data types, create random samples from sets of data.

When generator is invoked, it will always generate the same data by fixing the seed
of the Mersenne twister it uses as a random source.

New generators can be added by calling `REGISTER_GENERATOR` defined in `generator_state.h`.

A generator is a function with the signature:

```c++
void generator_func(GeneratorState&);
```

Generators are invoked lazily (when a theorem needs the data) and are idempotent.

When called, a generator function must check if the data already exists on disk and if not
it must create the data. Irrespective of the previous state, the passed`GeneratorState`
must be updated to reflect that data is now available before returning.

A generator is allowed to generate more than one CSV file. If it does, `REGISTER_GENERATOR`
must be called once per table the generator can construct.

## Generator Dependencies

- `common`

Generator can be build as a standalone library.

# SQL

The [SQL library](../sql) is responsible for creating **Connections** and executing
**Statements** in a **Driver** and **Engine** agnostic way.

## Constructing a Connection
Connections are made via a factory method in `connection_factory.h`. To construct
 a `Connection` you must first create a `Credential`

Example, opening a connection to PostgreSQL and running a statement

```c++
sql::Engine engine("Postgres");
sql::Credential credential("localhost", "mydatabase", 5432, "myuser", "mypassword");
sql::ConnectionFactory factory(engine, credential);

std::unique_ptr<sql::ConnectionBase> my_connection = factory.create();

my_connection->execute("SELECT 1");
```

The `sql::Engine` automatically selects the right **Driver** to use from its internal
library of **Engine**/**Driver** mapping.

## SQL Dependencies
- `common`.

Each driver can be built in isolation and depends on the interface of `sql` (which is 
called `sql_interface`) to implement subclasses of the interfaces defined in`sql`.

The interfaces are all postfixed `*_base.h`

Drivers depend on the libraries they define in their `README.md`. Drivers must
use the Pimpl idiom to avoid polluting the header namespace with definitnions from 
thirdparty libraries needed to manipulate the Engines they can talk to.

SQL can be built as a standalone library and consumed by other applications that need
generic SQL running functionality.

# Runner Library

The [Runner](../runner) is responsible for running, measuring and instrumentaing 
queries. It depends on the `sql` library.

Runner will do things like:

- Run queries in sequence
- Gather aggregated statistics about queries being run this way
- Parallelise queries
- Gather query plans for future analysis

## Runner Dependencies

- `sql`
- `common`

# UX 
`dbprove` is a console application. It makes use of ANSI colour codes and UTF8 glyphs
to present beautiful UTF/ASCII-art to its user.

It will also prompt for missing information when required.

UX provides the convenience methods that allow us to render **Theorem** runtime info
and results.

## UX Dependencies

- `rang` (Thirdparty) - f or terminal colour output
- `tabulate` (Thirdparty) - for rendering pretty tables to the user

UX relies on a background thread created by in `main.cpp` to keep its progress counters up to 
date. The background thread periodically polls running theroms to check for progress. 

# Common
C++ sometimes lacks a few convenience functions because its designed by a committee
that takes a very long time to agree on anything at all. 

In [common](../common) we put little extensions that don't seem to belong anywhere
else. All other libraries are allowed to depend on `common`.

# Theorems
Each **Therom** is a function that is executed by `main.cpp`. The theorems are
located in the [theorem](../theorem) folder with one subfolder per theorem category.

Each theorem category has a `prove.h` which defines this function:

```c++
void prove(const std::vector<std::string>& theorem, 
           sql::Engine engine, 
           const sql::Credential& credentials);
```

The function takes the `std::vector` of string name of the theorem (ex: `{'CLI-0001', 'CLI-0002'}`) and the 
`Engine` and `Credential` needed to prove that theorem. It will then execute the theroms passed.

If an empty vector is passed, all therems in that category are proven.

All theorems for a category live in a namespace with the same name as the category.

In addition to `prove` the theorem category must also expose the following function:

```c++
const std::vector<std::string>& list();
```
This simply returns a vector of valid theorems.

## Theorem Dependencies

- `sql`
- `common`
- `generator`
- `ux' - to render output of theorems
