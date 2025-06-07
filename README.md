# dbprove
[![Build Status](https://travis-ci.org/DBProve/dbprove.svg?branch=master)](https://travis-ci.org/DBProve/dbprove)
[![Coverage Status](https://coveralls.io/repos/github/DBProve/dbprove/badge.svg?branch=master)](https://coveralls.io/github/DBProve/dbprove?branch=master)
Tool to analyse, benchmark and prove the capabilities of a database engine

# Building `dbprove` from source

The goal of `dbprove` is to be static, fully self contained binary. 

Whenever possible, the libraries needed from these vendors will be statically linked into
`dbprove`.

Of course, that won't always work - because some database vendors don't even distribute
their driver source or even allow static linking (ex: Oracle and Teradata). For those 
cases, `dbprove` will dynamically try to load the library at startup.

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

# Thirdparty Rules

Thirdparty libraries are managed with `vcpkg`. In general, the goal is to keep third party dependencies to a minimum.

If you only need a single function from a massive library - please just make a header with that function instead of
pulling in the entire library.
