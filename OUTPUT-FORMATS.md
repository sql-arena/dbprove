# Output Formats of dbprove

By default, `dbprove` writes its output under `./out` relative to the directory it is invoked from.

Proof output has this structure:

- Folder: `out/proof/[Engine]`
- Folder: `out/proof/[Engine]/[Version]`
- File: `out/proof/[Engine]/[Version]/[Theorem].json`
- Folder: `out/proof/[Engine]/[Version]/artefacts`
- Folder: `out/proof/[Engine]/logs`

`[Version]` is the engine version returned by the engine driver for the current run.

If the user does not specify an artefact directory, `dbprove` writes artefacts to
`out/proof/[Engine]/[Version]/artefacts`.

If the user supplies `--artefact-dir <path>`, `dbprove` switches to replay mode and consumes artefacts from that
directory instead. Missing artefacts are treated as errors.

If the user supplies `--log-dir <path>`, `dbprove` writes logs there instead of `out/proof/[Engine]/logs`.

## Proof JSON

Each theorem proof is written as one JSON document with top-level theorem metadata, engine metadata, runtime summary,
and zero or more query entries.

Current top-level fields:

- `theorem`
  Object with:
  `name`, `displayName`, `description`, `categories`, and `tags`.
  `name` is the canonical theorem identifier used by `dbprove`.
  `displayName` is the user-facing short name for the theorem.
- `engine`
  Display name of the engine, for example `DuckDB`.
- `version`
  Engine version discovered during the run, for example `1.5.0`.
- `storageVariant`
  Always present. The storage layout used for the run, for example `native` or `iceberg`.
- `runtime`
  Aggregate runtime summary across the measured executions of the query:
  `bestMs`, `avgMs`, `minMs`, `maxMs`, and `stdDevMs`.
  Values are reported in milliseconds and rounded to 3 decimal places.
  This section is omitted when no runtime measurements were collected.
- `queries`
  Array of one or more query objects. This section is present when the theorem executed at least one query.
- `other`
  Optional bucket for proof values that do not yet have a dedicated section.

Each `queries[]` entry may contain:

- `sql`
  The SQL text that was executed.
- `startTime`
  Wall clock start time for the query in UTC with microsecond precision, formatted as
  `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- `timeMs`
  Representative runtime for the query in milliseconds, rounded to 3 decimal places.
  Today this is the best runtime recorded for the query.
- `status`
  Query outcome such as `OK`, `ERROR`, or `TIMEOUT`.
- `errorMessage`
  Present on the failing query when theorem execution fails after partial proof data has already been collected.
- `plan`
  Optional rendered plan as a single string.
- `operatorRows`
  Optional object containing row-oriented counts grouped by operator family.
  Current keys may include `join`, `aggregate`, `sort`, `distribute`, `seek`, `scan`, and `hash`.
  A value of `-1` means the actual row count for that operator family is unavailable — at least one
  plan node of that type did not have a measurable actual count (see *Missing actuals* below).
- `misEstimates`
  Optional object containing estimate-quality buckets grouped by operator family.
  Each operator contains counters for buckets such as `<16x`, `-8x`, `-4x`, `-2x`, `=`, `+2x`, `+4x`, `+8x`,
  and `>16x`.

Example shape:

```json
{
  "engine": "DuckDB",
  "queries": [
    {
      "misEstimates": {
        "scan": {
          "-4x": 1
        }
      },
      "operatorRows": {
        "aggregate": 5962146,
        "scan": 5962146,
        "sort": 4
      },
      "plan": "Estimate    Actual  Operator\n...",
      "sql": "SELECT ...",
      "startTime": "2026-06-17T09:48:46.424123Z",
      "status": "OK",
      "timeMs": 21.910
    }
  ],
  "runtime": {
    "avgMs": 21.910,
    "bestMs": 21.910,
    "maxMs": 21.910,
    "minMs": 21.910,
    "stdDevMs": 0.000
  },
  "storageVariant": "native",
  "theorem": {
    "categories": ["PLAN"],
    "description": "TPC-H Q01 Analysis",
    "displayName": "TPCH-Q01",
    "name": "PLAN-TPC-H-Q01",
    "tags": ["TPC-H"]
  },
  "version": "1.5.0"
}
```

## Missing actuals

After each query, dbprove runs a `SELECT COUNT(*) FROM <subtree SQL>` pass (`fixActuals`) against
every plan node to collect actual row counts.  If any of these follow-up queries fails — for example
because the engine rejects the generated SQL — the affected node's actual count is left as *unknown*
(`ROWS_UNKNOWN`).  This propagates to the `operatorRows` aggregate for that operator family as `-1`.

**A fixActuals failure is a bug**, not a graceful degradation.  It means dbprove produced SQL that
the target engine cannot execute.  The proof itself still passes or fails independently of actuals, but
the row-count data in the JSON is incomplete and the rendered plan will show `∞` for the missing nodes.

Known root cause (fixed in 2026-06): Trino's `fixActuals` SQL failed with *"Column 'id' is ambiguous"*
for JOB benchmark queries where multiple tables share a column named `id`.  The underlying `SELECT *`
scan SQL exposed every column from each table, causing ambiguity in join conditions.  Fixed by having
Trino scans emit an explicit column list (the engine's own projected outputs) instead of `SELECT *`
when no Projection wrapper is required.

Special case:

- `CONFIG-VERSION.json`
  Uses the same top-level `theorem`, `engine`, `version`, and `storageVariant` fields, but may not contain
  `runtime` or `queries`.
