# ClickHouse Driver

Uses the native ClickHouse protocol to talk with ClickHouse.

## New Session Bootstrap

If the task is "ClickHouse explain rendering", load context in this order:

1. `ai-rules.md` at the repo root for contributor and AI-agent coding preferences.
2. `src/sql/clickhouse/README.md` (this file) for behavior and edge cases.
3. `src/sql/clickhouse/explain.cpp` for actual parser and post-processing logic.
4. `src/sql/clickhouse/plan_node.cpp` for `Expression.Actions` parsing and cross-node expression wiring.
5. `src/sql/clickhouse/query_tree_parser.{h,cpp}` for query-tree relation extraction (`IN`/`NOT IN`, correlated `EXISTS`).
6. `src/sql/clickhouse/ast_parser.{h,cpp}` for set literal extraction from AST.
7. `src/sql/clickhouse/expression_node.{h,cpp}` for `ExpressionNode` tree rendering (`renderSql`, `renderUser`, executable SQL).
8. `src/sql/explain/join.cpp` for join SQL reconstruction behavior shared across engines.

Fast entry points inside `src/sql/clickhouse/explain.cpp`:

- `Connection::explain(...)`: artifact load/fetch and full parse pipeline.
- `fetchClickHouseExplainJson(...)`, `fetchClickHouseExplainAst(...)`, `fetchClickHouseExplainQueryTree(...)`: explain artifact fetch/store.
- `buildResolvedJsonPlanTreeFromExplainJson(...)`: resolved `PlanNode` tree build + shape rewrites before canonical lowering.
- `createExplainNodeFromResolvedPlanNode(...)`, `buildExplainNode(...)`, `buildExplainPlan(...)`: `PlanNode` to canonical `sql::explain::Node` lowering.
- `flattenCreatingSetWrappersPlanNodes(...)`, `flipJoinChildrenPlanNodes(...)`, `pruneBroadcastPlanNodes(...)`: active pre-lowering structural passes.

Fast entry points outside `explain.cpp` that matter during ClickHouse explain work:

- `Connection::fetchAll(...)` in `connection.cpp`: used by explain artifact fetches and by `Plan::fixActuals(...)`; `DBPROVE_ACTUALS` queries temporarily set `max_execution_time` and restore it afterward.
- `buildResolvedPlanNodeTree(...)` in `plan_node.cpp`: parses raw JSON into `PlanNode`/`ExpressionNode`, wires references bottom-up, validates the tree, wires common-buffer lineage, and assigns aliases before canonical lowering.
- `stripClickHouseTypedLiterals(...)` in `literals.cpp`: normalizes engine-specific typed literal spellings before SQL rendering and AST set recovery.

## Initial Debugging Workflow (`dump_clickhouse_plan_tree.sh`)

Use `./dump_clickhouse_plan_tree.sh` for first-pass debugging of ClickHouse plan parsing.

What it does:
- Builds target `clickhouse_plan_tree_dump` if needed.
- Loads a ClickHouse plan artifact JSON.
- Prints the resolved `PlanNode`/`ExpressionNode` tree (node ids, headers, actions, outputs, keys, clauses, aggregates).

Build directory behavior:
- Script default is `BUILD_DIR=./build`.
- Override with `BUILD_DIR=/path/to/cmake-build ./dump_clickhouse_plan_tree.sh ...`.
- This matters when using CMake presets that build under `out/build/...`.

Input resolution behavior:
- Accepts `QNN`, `TPCH-QNN`, or a direct artifact path.
- `Q1` is normalized to `Q01`.
- Resolution order is: direct existing path -> `<artifacts-dir>/TPCH-QNN.json` -> `<artifacts-dir>/<input>.json`.
- `--artifacts-dir` defaults to `./run/artifacts/clickhouse`.
- Underlying binary currently accepts one positional input plus optional `--artifacts-dir`; extra args fail fast.

Important scope note:
- The dump tool calls `buildResolvedPlanNodeTree(...)` directly.
- It shows resolved ClickHouse plan tree state (including expression wiring and common-buffer linkage), but it does not run the canonical `Connection::explain(...)` rewrite pipeline.

## Execution Plan Parsing

This document tracks the current parsing model for ClickHouse plans in `dbprove`.

### Plan Sources

The ClickHouse `explain` flow now executes and stores three explain artefacts:
1. `EXPLAIN PLAN json = 1, actions = 1, header = 1, description = 1 ... FORMAT TSVRaw` (plan JSON)
2. `EXPLAIN AST ... FORMAT TSVRaw` (AST text tree)
3. `EXPLAIN QUERY TREE ... FORMAT TSVRaw` (query tree text)

Both are needed:
- JSON is used to build the canonical operator tree.
- JSON actions are parsed into `ExpressionNode` trees and used as the primary source for filters/join predicates/projections.
- AST is used to recover `IN`/`NOT IN` set literals that are hidden behind internal `__set_*` names in plan expressions.
- Query tree is used to extract subquery relationships and correlated `EXISTS` relations.

### Artefacts

With `-a/--artifacts`, ClickHouse explain now caches:
- `clickhouse_<name>.json`
- `clickhouse_<name>.ast`
- `clickhouse_<name>.query_tree`

On cache hit, both are reused when present.
If JSON is cached but AST is missing, AST is fetched and cached so set extraction remains deterministic.

### Node Mapping Notes

Core mappings in `src/sql/clickhouse/explain.cpp`:
- `ReadFromMergeTree` / `ReadFromStorage` -> `SCAN`
- `SaveSubqueryResultToBuffer` -> `MATERIALISE`
- `ReadFromCommonBuffer` -> `SCAN MATERIALISED`
- `Join` -> `JOIN`
- `Filter` -> `FILTER`
- `Expression` -> `PROJECT` (if it adds real projections)
- `Aggregating` -> `GROUP BY`
- `Sorting` -> `SORT`
- `Limit` -> `LIMIT`

For common-buffer nodes, ClickHouse JSON does not expose an explicit producer reference on `ReadFromCommonBuffer`.
Current linkage uses node-id suffix pairing observed in plans: `ReadFromCommonBuffer_<N>` -> `SaveSubqueryResultToBuffer_<N+1>`.
For subtree SQL reconstruction (`fixActuals(...)`), `ReadFromCommonBuffer` is rendered as a placeholder projection with `NULL AS <column>` for each buffer header column so join/filter references can bind.

### `CreatingSets` / `CreatingSet`

ClickHouse 26 emits top-level `CreatingSets` branches (for set/subquery preparation).  
Parser behavior:
- `CreatingSets` and `CreatingSet` wrappers are treated as non-relational wrappers.
- If any `CreatingSets` node is encountered, we build a single top-level `SEQUENCE` node.
- All children from all encountered `CreatingSets` nodes are hoisted and attached under that one root `SEQUENCE`.
- Remaining relational root (if any) is appended as the final `SEQUENCE` child.
- Nested `CreatingSet -> CreatingSets` chains are also hoisted during wrapper-skipping, so multi-child set-prep nodes do not trigger skip-loop failures.

This prevents failures on multi-child skip paths and models "prepare then run" execution order.

### `MATERIALISE` Placement Rule

`SaveSubqueryResultToBuffer` maps to canonical `MATERIALISE`.  
As a general rendering rule, all `MATERIALISE` nodes are hoisted to the top-level `SEQUENCE` root (when present), so set/subquery preparation appears before the main relational tree.

#### Inline Producer + Consumer Shape

Some ClickHouse plans use a `SaveSubqueryResultToBuffer` subtree both as:
- a producer (buffer write), and
- an inline relational branch that continues upward in the same join/filter path.

This appears in plans where:
- `SaveSubqueryResultToBuffer_<N>` is present in-branch, and
- one or more `ReadFromCommonBuffer_<M>` nodes resolve to that same producer (`common_buffer_producer == SaveSubqueryResultToBuffer_<N>`).

To keep the canonical tree readable while still hoisting the producer, the PlanNode pre-lowering pipeline inserts a synthetic inline `ReadFromCommonBuffer` wrapper above eligible `SaveSubqueryResultToBuffer` nodes. This allows canonical lowering to show both:
- top-level `MATERIALISE AS m_<producer>`, and
- local `SCAN MATERIALISED <producer>` where the branch reads back from that materialised source.

### Scalar Subquery Rendering

Uncorrelated scalar subqueries (for example `col > (SELECT AVG(...) ...)`) are explained separately and attached as children of a top-level `SEQUENCE`, before the main query plan.

Current extraction scope is SQL-text based and targets `(SELECT ...)` in scalar-expression contexts. It intentionally skips subqueries immediately preceded by:
- `EXISTS`
- `IN`
- `FROM`
- `JOIN`

Correlation detection for scalar-subquery attachment uses `EXPLAIN QUERY TREE` metadata:
- scalar subqueries appearing as `CONSTANT ... EXPRESSION ... QUERY is_subquery: 1` are candidates
- candidates marked `is_correlated: 1` are not attached separately

### Set Resolution Strategy (`__set_*`)

ClickHouse plan JSON often contains filters like:
- `in(col, __set_...)`
- `notIn(col, __set_...)`

Set contents are reconstructed in two passes:
1. `guessSetsFromAst(...)`: extracts tuple literals from AST `in`/`notIn` functions.

Then `replaceSets(...)` rewrites internal set references into explicit `col IN (...)` forms for rendered SQL.
When `replaceSets(...)` re-renders expression trees, `REFERENCE` traversal must follow `ExpressionNode::references`/linked child output pointers (not just child nodes), otherwise rendered filters can regress to synthetic symbols like `__tableX.col`.

There is no SQL-text guessing fallback anymore. Set recovery is AST-only, and set membership predicates are matched from expression trees.

### ExpressionNode Wiring First Architecture

Before canonical plan construction, `ExpressionNode` trees are resolved while building the `PlanNode` tree (`buildResolvedPlanNodeTree(...)` in `plan_node.cpp`):
- `buildPlanNodeTreeRecursive(...)` parses node-local expression payloads (`Prewhere`, `Expression.Actions`, keys/outputs/header expressions).
- `PlanNode::bottom_up()` iteration resolves cross-node references and output bindings node-by-node (children first, parent last).
- `assignExpressionAliases(...)` assigns stable debug aliases (`alias_sql`, `alias_user`) across all parsed expression roots.
- `INPUT` leaf wiring is strict:
  - resolve input offset from `Expression.Actions[*].Arguments[0]` for `Node Type == INPUT`
  - then resolve by `Expression.Inputs[offset].Name` to child output name with byte-for-byte exact string equality
  - action `Result` is still used as the action output slot in `Positions`/intra-action dependencies; it is not the `Expression.Inputs` offset
  - no case normalization, whitespace normalization, suffix matching, or substring heuristics are used
  - unresolved slot/name links are logged at debug level (`PLOGD`) with node id, slot, expected input name, and child output candidates

Parent/child binding semantics:
- Parent expressions bind to child headers only.
- Each child `Header` entry must be wired to an expression root (`header.expression`) before parent binding.
- Header wiring source:
  - first local node outputs (`Expression.Outputs`-derived `output_expressions`, or synthesized node-owned outputs when `Expression` is absent)
  - then child header candidates (for pass-through columns)
- Parent binding candidates are `(header.name -> header.expression)` pairs, not raw `output_expressions`.

`ExpressionNode` trees are used directly when constructing `sql::explain::Node` expressions:
- `SCAN` filters come from prewhere expression trees.
- `FILTER` predicates come from expression trees.
- `JOIN` conditions come from expression trees.
- `PROJECT` columns come from expression tree roots/sinks.
- set predicate identification (`IN`/`NOTIN`) and replacement uses expression tree predicates.

Prewhere rendering rule (current):
- Render prewhere from resolved `ExpressionNode` trees via `PlanNode::renderPrewhere()`.
- Render as SQL (`renderSql`) rather than user form to avoid alias suffixes (`AS aN`) in scan `WHERE`.
- Do not run `replaceSets(...)` on prewhere rendering for now; prewhere filters should not be re-rooted/re-rendered through set-replacement logic.

Important implementation detail:
- `expression_lineage` is no longer part of the ClickHouse driver build path.
- `Expression.Actions` are parsed directly in `plan_node.cpp` into `ExpressionNode` trees.
- Tree traversal/wiring uses `TreeNode` iterators (`depth_first`, child traversal), not a parallel lineage model.
- Bottom-up plan wiring avoids recursive parent loops over `children()` spans; this prevents sibling-skip bugs from nested child traversal.
- Canonical lowering (`buildExplainNode(...)`) must snapshot `PlanNode` children before recursive descent. `TreeNode::children()` returns a thread-local span view, so iterating it directly across recursion can skip siblings and drop branches (for example, missing `Aggregating_*` subtrees in Q02).
- The same thread-local `children()` caveat applies to recursive `ExpressionNode` traversal/rendering and tree dump output; snapshot child pointers before recursive descent to avoid dropped/miswired sibling rendering.
- Alias reference rendering rule uses `ExpressionNode::isRealAliased()`:
  - `true` for non-synthetic `ALIAS` nodes
  - `true` for nodes with `alias_user` (for example function nodes)
  - when resolving a `REFERENCE` chain, resolution stops at the first `isRealAliased()` node and renders that alias token
  - synthetic aliases still unfold to underlying expressions
  - alias assignment prefers existing non-synthetic aliases in-tree (for example `sum_qty`) over generated `aN` user aliases

### PlanNode Naming Convention

For `PlanNode` fields that keep both raw JSON payload and resolved AST objects, use this naming rule consistently:

- `unresolved_<name>`: raw value from JSON (`string`/`vector<string>`/JSON-shaped source data)
- `<name>`: resolved semantic value (typically `ExpressionNode` or `vector<ExpressionNode>`)

Examples:

- `unresolved_keys` -> `keys`
- `unresolved_clauses` -> `clauses`

Do not use `resolved_*` names for resolved fields in `PlanNode`; resolved fields use the canonical name directly.

### Helper Naming

Avoid `OrThrow` suffixes in function names. Throwing behavior should be obvious from implementation and call-site contracts, without encoding it in the function name.

### Aliasing Strategy

ClickHouse explain JSON and query tree expose synthetic aliases (`__tableN`) rather than user SQL aliases (`l1`, `l2`, ...).
User aliases are visible in AST, but there is no stable direct mapping from AST aliases to plan JSON nodes without cross-artifact scope matching.

Current strategy is therefore synthetic-alias-first:
- run a pre-build alias analysis pass over parsed expression trees/headers
- detect where synthetic qualifiers are actually required to disambiguate
- assign matching synthetic aliases to `SCAN` nodes before node construction only when required
- preserve/qualify expressions against those synthetic scan aliases

This keeps filter/join expressions and subtree SQL generation consistent without relying on fragile user-alias reconstruction.

### Post-processing

Current `Connection::explain(...)` pipeline runs:
1. Fetch/reuse JSON, AST, and query-tree artifacts.
2. Extract AST sets + query-tree correlated `EXISTS` relations into `ExplainCtx`.
3. Build resolved JSON `PlanNode` tree and apply active structural passes:
   - `applyStrictnessJoinTypePlanNodes(...)`
   - `flattenCreatingSetWrappersPlanNodes(...)`
   - `flipJoinChildrenPlanNodes(...)`
   - `pruneBroadcastPlanNodes(...)`
   - `insertInlineMaterialisedReadPlanNodes(...)`
4. Lower resolved `PlanNode` tree into canonical plan via `buildExplainPlan(...)`.
5. Unless `DBPROVE_SKIP_ACTUALS=1`, call `Plan::fixActuals(...)` after canonical lowering to attach actual-row information.

### Connection and Result Layer Notes

The ClickHouse transport layer is intentionally small compared with the explain parser:

- `Connection` lazily creates a native `clickhouse::Client`.
- `DELETE FROM <table>` is rewritten to `TRUNCATE TABLE <table>` before execution.
- Generic DDL is normalized to ClickHouse-friendly types and given a default `ENGINE = MergeTree() ORDER BY tuple()`.
- `Result` stores deep-copied `clickhouse::Block` instances because the upstream client exposes blocks only inside the `Select(...)` callback lifetime.

These transport details are usually not where explain bugs live, but they are relevant when debugging artifact fetches or actual-row collection.

### Projection Source Resolution

ClickHouse `Expression` nodes often carry two different names for the same projected value:

- a semantic alias that should survive in the canonical plan (`nation`, `supplier_no`, `total_revenue`, `numcust`)
- an executable child-slot name that parent subtree SQL must actually reference (`a1`, `a2`, `a5`, `l_suppkey`, `nation`)

The stable rule is:

1. Build the initial canonical `Projection` columns from the resolved `ExpressionNode` tree.
2. If a projected expression is only forwarding a referenced child value, walk its alias/reference lineage and collect candidate source names in order.
3. After children are lowered, compare those candidates against the immediate child node's executable output names.
4. Rewrite the projection source to the first candidate that the child really exposes.

This avoids both failure modes that showed up during TPC-H work:

- using semantic aliases that the child does not actually output yet (`supplier_no`, `total_revenue`, `totacctbal`)
- using positional child-column rewrites that accidentally remap unrelated columns in wide joins/projections

Practical examples:

- Q15: outer revenue projections must read `l_suppkey` / `a5`, even though the semantic aliases are `supplier_no` / `total_revenue`
- Q22: final top projection must read `a1` / `a2`, not re-expand `COUNT()` / `SUM(c_acctbal)` and not fall back to base `c_acctbal`
- Q09: grouped `nation` must keep reading child output `nation`, not regress to base `n_name`

Update after fixing the Q04 actuals failure:

- For semi/anti joins, `deriveJoinConditionFromLineage(...)` should prefer rendering `PlanNode::clauses` through `ExpressionNode::renderSql(aliases_by_plan_node_id)` before falling back to child-output/token heuristics.
- This matters because the resolved expression graph can carry alias chains like `__table1.o_orderkey -> __table4.l_orderkey -> l_orderkey`; plain token-based reconstruction loses that lineage and can emit invalid correlated actuals SQL such as `inner_alias.o_orderkey` even when the inner subtree only exposes `l_orderkey`.
- After the change, Q04 actuals SQL reconstructs the correlated predicate as `inner_alias.l_orderkey = outer_alias.o_orderkey`, which ClickHouse accepts.

Update after fixing the Q21 correlated `EXISTS` / `NOT EXISTS` actuals path:

- The raw semi/anti join clauses emitted by ClickHouse for correlated `EXISTS` subqueries can be misleading. In Q21 they arrive as tuple equalities that mention both `l_orderkey` and `l_suppkey`, even though the real correlation key is only `l_orderkey` and the `l_suppkey <> ...` predicate lives separately as a filter.
- For these joins, the safer path is:
  1. extract correlated `EXISTS` equality relationships from `EXPLAIN QUERY TREE`
  2. compute clause key bases from the resolved `ExpressionNode` clause tree
  3. synthesize the canonical semi/anti join condition from the query-tree equality columns first
  4. fall back to child-output/token heuristics only if no query-tree correlation can be recovered
- `extractClauseLeafTokens(...)` must follow alias/reference wrappers and leaf child-output bindings, not only direct `REFERENCE` nodes. Without that, clause-key extraction comes back empty for wrapped tuple/equality nodes and the query-tree correlation path never activates.
- The practical effect is that Q21 now reconstructs:
  - `RIGHT SEMI ... ON outer_alias.l_orderkey = inner_alias.l_orderkey`
  - `RIGHT ANTI ... ON outer_alias.l_orderkey = inner_alias.l_orderkey`
  instead of self-equalities or over-constrained `(l_orderkey, l_suppkey)` matches.
- With `DBPROVE_ACTUALS_TIMEOUT_SEC=30`, the local ClickHouse `PLAN` sweep now runs through TPC-H Q14 and stops at the separate known Q15 typed-literal `PlanNode` validation bug rather than at Q21 actuals.

Update after tuning common semi/anti SQL rendering for actuals:

- Canonical semi/anti subtree SQL in `src/sql/explain/join.cpp` now renders `EXISTS` / `NOT EXISTS` probes as `SELECT 1 ... LIMIT 1` instead of `SELECT *`.
- This is a shared SQL-layer optimization, not ClickHouse-specific logic, and it helps expensive actuals probes short-circuit once a matching inner row is found.

Cleanup note:
- The historical post-lowering rewrite layer in `explain.cpp` has been removed.
- The active implementation path is now the resolved-`PlanNode` pipeline plus the pre-lowering shape passes listed above.
- Dead helpers for sequence dependency reordering, subquery filter rewrites, join re-qualification, scalar-subquery attachment, and other unused post-build canonical rewrites are no longer part of the file.
- The small SQL-text scalar-subquery parser helpers (`startsWithKeywordAt`, `skipWhitespace`, `findMatchingParen`, `previousWordUpper`) were also removed once scalar-subquery attachment stopped being part of the active pipeline.

Logging behavior:
- Actual-subquery SQL and materialise tracing are debug-level (`PLOGD`).
- Info-level actuals start/done noise is intentionally removed.

Actuals SQL aliasing notes:
- Alias assignment is constructor-driven in canonical nodes (`Column(..., alias)`), not inferred later in `treeSQLImpl(...)`.
- For ClickHouse `Aggregating`, aggregate output aliases are passed explicitly from resolved expression metadata (prefer `alias_user`, then stable output/result names).
- For ClickHouse `Expression` projections, function expressions with stable `alias_user` are emitted as `expr AS alias` so parent nested nodes can reference internal slots (`aN`) safely.
- For forwarded projections, executable source selection is child-aware: prefer the first lineage-derived candidate that the immediate child actually exposes, rather than the first semantic alias seen in the reference chain.

### Validation Status

As of 2026-03-18, running

```bash
DBPROVE_ACTUALS_TIMEOUT_SEC=30 dbprove -e clickhouse -T PLAN
```

against the local ClickHouse 26.1 setup completes the full TPC-H `PLAN` sweep without:

- `PlanNode` validation failures
- `fixActuals failed`
- correlated `CommonSubplan` errors
- zeroed-out actual rowcounts on the previously failing Q15 / Q21 / Q22 paths

### Known Limitations

- Set recovery from AST currently targets tuple literal patterns and may need extension for future AST variants.
- `SORT` and `GROUP BY` keys/aggregates are still read from JSON metadata fields (`Sort Description`, `Keys`, `Aggregates`) rather than expression actions.
- `fixActuals(...)` depends on reconstructed SQL being valid for each subtree shape; unusual expressions/functions may still require parser updates when that path is used.

### Test Coverage Notes

- `src/sql/test/explain.cpp` currently exercises explain rendering for PostgreSQL and DuckDB.
- ClickHouse explain behavior is validated mainly through theorem runs and artifact-driven debugging.
- When changing ClickHouse explain parsing, prefer re-running with `-a/--artifacts` to capture and replay plan inputs.

### Literal / Function Normalization Notes

ClickHouse may emit typed numeric literals in expressions (for example `15_UInt16`, `25_UInt16`).
Typed literals and synthetic `__tableX.` qualifiers are normalized before canonical rendering.
No string/regex fallback rewrites are used for wiring; matching is exact and field-driven from machine-generated JSON.

## Running with Docker

To start a ClickHouse instance for testing:
```bash
cd docker
docker-compose up -d clickhouse
```

Data is persisted in `run/mount/clickhouse`.
