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
- **DRY Principle**: Always prefer DRY (Don't Repeat Yourself) code. Extract repeated boilerplate into reusable functions or classes to keep the codebase maintainable and clean.
- **Library Reuse**: Always seek out existing functionality in libraries already provided before suggesting additional, third-party code.
- **Fail Fast**: When something fails (for example, missing expected headers or API fields), throw an exception instead of returning a bogus value. Early failure is preferred over silent continuation with incorrect state.
- **Scratchpad**: Use the `scratchpad/` directory for any temporary files or outputs created during the session. This directory is `.gitignored`.
- **Run Directory**: Always execute commands (building, invoking `dbprove` to run tests/theorems) from the `run/` directory. This ensures that generated artifacts like `logs/`, `proof/`, `table_data/`, and `scratchpad/` are contained within the `run/` folder and do not clutter the project root.
- **Avoid Deep Nesting**: Use early exits (`return`, `continue`, `break`) to keep the "happy path" flat and readable.
- **TreeNode Iterators**: Always prefer using the `TreeNode` iterators (`depth_first()`, `breadth_first()`) for tree traversal and operations instead of manual recursion or custom traversal logic.
- **Explicit Logging**: Always log key identifiers (e.g., query IDs, statement IDs, timestamps) using the available logging framework (`PLOGI`) or `std::cout` when appropriate for diagnostics.
- **Robust Error Handling**: Do not swallow exceptions in low-level API handlers unless recovery is truly possible. Throw descriptive `std::runtime_error` or custom exceptions when expectations are not met.
- **Playwright Scripts**: When using Playwright for scrapers (e.g., in `scripts/`), be aware that `context.isClosed()` is not a standard method. Use event listeners on `close` events for `page` or `context` objects and track state with flags.
- **Dirty Worktree Handling**: If unrelated files are already modified, continue your assigned task and ignore those files. Do not revert unrelated changes. Only stop and ask for guidance if an unexpected change appears in files you must edit for the current task.
- **Test Placement**: Place tests close to the implementation area when practical. For ClickHouse-specific SQL logic, add tests under `src/sql/clickhouse/test/` and wire them into the shared SQL test target.
- **Single-Responsibility Methods**: Keep top-level methods focused on orchestration. If a method starts handling multiple independent responsibilities (for example fetch, transform, and specialized post-processing), extract those responsibilities into clearly named helper functions.
- **Engine Boundary**: Keep engine-specific behavior (for example ClickHouse-specific SQL rendering and alias/qualification rewrites) out of generic `sql::explain::Node` subclasses; implement those behaviors in the engine explain pipeline instead.
- **Machine-Generated Plans**: Treat engine explain JSON as deterministic contracts. Prefer exact field-driven wiring over heuristics. Do not add case-folding/whitespace-normalization/suffix/substring fallback matching for plan-node linkage; instead fail with explicit diagnostics when expected exact links are missing.
- **ExpressionNode First for Lineage**: For ClickHouse explain work, prefer resolving aliases, join keys, and lineage through resolved `ExpressionNode` structures before adding string-based SQL heuristics. Use textual fallback logic only when the expression graph does not carry enough information.
