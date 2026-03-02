# dbprove
[![Build Status](https://travis-ci.org/DBProve/dbprove.svg?branch=master)](https://travis-ci.org/DBProve/dbprove)
[![Coverage Status](https://coveralls.io/repos/github/DBProve/dbprove/badge.svg?branch=master)](https://coveralls.io/github/DBProve/dbprove?branch=master)
Tool to analyse, benchmark and prove the capabilities of a database engine

# Building `dbprove` from source

The goal of `dbprove` is to be a static, fully self-contained binary. 

Whenever possible, the libraries needed from these vendors will be statically linked into
`dbprove`.

Of course, that won't always work - because some database vendors don't even distribute
their driver source or even allow static linking (ex: Oracle and Teradata). For those 
cases, `dbprove` will dynamically try to load the library at startup.

## CMake Presets
The project uses CMake presets for configuration. To build on macOS with Apple Silicon (ARM64), use:
```bash
cmake --preset osx-arm-base
cmake --build out/build/osx-arm-base --target dbprove
```
The `osx-arm-base` preset is configured to build with `CMAKE_BUILD_TYPE: Debug` by default to ensure symbols are available for debugging.

## Contributing to Driver development and adding your own Database Driver
There is a lot of work involved with adding new drivers to `dbprove`. Each database
vendor has their own idiosyncrasies. If you are interested in contributing or hooking up your
own database - please shoot me a mail and I can help you get started.

Since `dbprove` is an Apache licensed product, it means that static linking with
GPL licensed, prebuild libraries causes copy-left contamination. Because of this, drivers
that are licensed on less restrictive terms will be preferred. When no other driver
exists - dynamic og runtime loading  is acceptable. It is important that `dbprove`
can always run with drivers that it has statically linked and that newly added drivers
do not cause it to fail on startup.

### Databricks Support
Databricks connectivity relies on a browser-based authentication flow for some features (like plan dumping). 

#### Plan Artifacts
To make analysis and debugging easier, you can cache Databricks plan artifacts (the scraped JSON and raw EXPLAIN output) using the `-a/--artifacts <path>` flag. 

```bash
dbprove -e Databricks ... -a ./my_artifacts
```
When this flag is used, `dbprove` will first check the specified directory for cached files (named `databricks_<hash>_json` and `databricks_<hash>_raw_explain`). If found, it will skip all remote calls and use the local files. If not found, it will perform the full explain flow and save the results for next time.

#### Authentication
Before running Databricks-related commands that require a browser session, run the authentication script:
```bash
./authenticate_databricks.sh
```
This script will open a browser window using Playwright. Complete the login and 2FA process, then close the browser. Your session will be saved in a local profile (`~/.databricks-playwright-profile`) and reused by `dbprove`.

# Coding Guidelines
Detailed coding guidelines for contributors and AI agents are maintained in [ai-rules.md](ai-rules.md).

# Thirdparty Rules

Thirdparty libraries are managed with `vcpkg`. In general, the goal is to keep third party dependencies to a minimum.

If you only need a single function from a massive library - please just make a header with that function instead of
pulling in the entire library.
