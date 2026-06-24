Publish the latest proof results from `run/proof/` to the `dbprove-results` repo and push.

Arguments: `$ARGUMENTS` (optional — a free-text note to include in the commit message, e.g. "add PLAN-JOB results")

## Steps

1. **Publish** — run from `/Users/thomaskejser/source/dbprove-agent1/run`:
   ```
   ../out/build/osx-arm-base/src/dbprove/dbprove --publish database-doctor
   ```
   This copies `run/proof/<Engine>/<version>/` → `dbprove-results/engine/<Engine>/<version>/database-doctor/`.

2. **Commit and push** — in `/Users/thomaskejser/source/dbprove-results`:
   - `git add -A`
   - `git status` to see what changed (new engines, updated versions, artefacts)
   - Commit with a message that summarises what changed. Include the optional note from `$ARGUMENTS` if provided.
   - `git push`

3. **Report** what was published: which engines and versions were updated, and the commit SHA.

## Notes

- The `dbprove-results` repo is at `/Users/thomaskejser/source/dbprove-results`.
- If `--publish` fails because the `proof/` directory is missing, report that clearly — it means no theorems have been run yet.
- If nothing changed in the results repo after publishing, say so and skip the commit.
