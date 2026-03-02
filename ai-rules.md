# Rules for AI agents

# Introduction
This project aims to validate variuos database and test what types of functionality
they possess. It also seeks to performance profile various, basic operations.

## Terminology
A test of a database capability is called `Theorem`. Theorems are stored in the 
folder `theorems` and grouped by the area of the database they use

## Coding Style
The following rules apply to the C/C++ coding style

- Standard: C++23 (as defined in `CMakeLists.txt`)
- Headers: Use `#pragma once` instead of guard macros
- Documentation: Classes and methods should have Doxygen comments
- Comments: Keep comments inside code to a minimum; favor clear naming
- Style: Use `.clang-format` and `.clang-tidy` for styling and conventions

## AI Behaviour
The following is the author's preference:

- **Minimal Changes**: Suggest only the change requested, do not try to add additional functions not requested.
- **Library Reuse**: Always seek out existing functionality in libraries already provided before suggesting additional, third-party code.
- **Fail Fast**: When something fails (for example, missing expected headers or API fields), throw an exception instead of returning a bogus value. Early failure is preferred over silent continuation with incorrect state.
- **Scratchpad**: Use the `scratchpad/` directory for any temporary files or outputs created during the session. This directory is `.gitignored`.
- **Run Directory**: Always execute commands (building, running tests/theorems) from the `run/` directory. This ensures that generated artifacts like `logs/`, `proof/`, `table_data/`, and `scratchpad/` are contained within the `run/` folder and do not clutter the project root.
- **Avoid Deep Nesting**: Use early exits (`return`, `continue`, `break`) to keep the "happy path" flat and readable.
- **Explicit Logging**: Always log key identifiers (e.g., query IDs, statement IDs, timestamps) using the available logging framework (`PLOGI`) or `std::cout` when appropriate for diagnostics.
- **Robust Error Handling**: Do not swallow exceptions in low-level API handlers unless recovery is truly possible. Throw descriptive `std::runtime_error` or custom exceptions when expectations are not met.
- **Playwright Scripts**: When using Playwright for scrapers (e.g., in `scripts/`), be aware that `context.isClosed()` is not a standard method. Use event listeners on `close` events for `page` or `context` objects and track state with flags.