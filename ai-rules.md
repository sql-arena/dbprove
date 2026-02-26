# Rules for AI agents

# Introduction
This project aims to validate variuos database and test what types of functionality
they possess. It also seeks to performance profile various, basic operations.

## Terminology
A test of a database capability is called `Theorem`. Theorems are stored in the 
folder `theorems` and grouped by the area of the database they use

## Coding Style
The following rules apply to the C/C++ coding style

- Keep comments inside code to a minimum
- Classes and methods have doxygen comments. This is where comments should be kept
- Headers user `#pragma once` instead of guard macros
- Coding conventions as described in `.clang-format` and `.clang-tidy`

## AI Behaviour
The following is the authors preferences

- Suggest only the change requested, do not try to add additional functions not requested
- Always seeks out existing functionality in libraries already provided before suggesting additional, third party code
- When something fails (for example, missing expected headers or API fields), throw an exception instead of returning a bogus value. Early failure is preferred over silent continuation with incorrect state.
- **Avoid deep nesting**: Use early exits (`return`, `continue`, `break`) to keep the "happy path" flat and readable.
- **Explicit Logging**: Always log key identifiers (e.g., query IDs, statement IDs, timestamps) using the available logging framework (`PLOGI`) or `std::cout` when appropriate for diagnostics.
- **Robust Error Handling**: Do not swallow exceptions in low-level API handlers unless recovery is truly possible. Throw descriptive `std::runtime_error` or custom exceptions when expectations are not met.