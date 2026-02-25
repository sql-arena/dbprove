#include "dbprove/theorem/theorem.h"
#include "init.h"
#include "runner.h"
#include "query.h"

namespace dbprove::theorem::test {

void run_test(Proof& proof) {
    // 1. Ensure the tables are generated/loaded
    proof.ensure("test.pk");
    proof.ensure("test.fk");

    // 2. Define the join query
    Query join_query(
        "SELECT pk.val, fk.fk_val "
        "FROM test.pk pk "
        "JOIN test.fk fk ON pk.id = fk.pk_id",
        "test"
    );

    // 3. Create a runner and execute/explain
    Runner runner(proof.factory());
    
    // We want to capture the plan
    runner.serialExplain(std::move(join_query), proof);
}

void init() {
    auto& theorem = addTheorem("test", "Simple join test for Databricks implementation", run_test);
    categoriseTheorem(theorem, Category::PLAN);
}

} // namespace dbprove::theorem::test
