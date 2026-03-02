#include "dbprove/theorem/theorem.h"
#include "init.h"
#include "runner.h"
#include "query.h"

namespace dbprove::theorem::test {

void run_test(Proof& proof) {
    // 1. Ensure the tables are generated/loaded
    proof.ensureSchema("test").ensure("test.pk").ensure("test.fk");

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

void run_test_scan(Proof& proof) {
    // 1. Ensure the tables are generated/loaded
    proof.ensureSchema("test").ensure("test.pk");

    // 2. Define the scan query
    Query scan_query(
        "SELECT val FROM test.pk",
        "test"
    );

    // 3. Create a runner and execute/explain
    Runner runner(proof.factory());
    
    // We want to capture the plan
    runner.serialExplain(std::move(scan_query), proof);
}

void init() {
    auto& test = addTheorem("test-join-inner", "Simple join test for Databricks implementation", run_test);
    categoriseTheorem(test, Category::TEST);
    tagTheorem(test, Tag("TEST"));

    auto& test_scan = addTheorem("test-scan", "Simple scan test for Databricks implementation", run_test_scan);
    categoriseTheorem(test_scan, Category::TEST);
    tagTheorem(test_scan, Tag("TEST"));
}

} // namespace dbprove::theorem::test
