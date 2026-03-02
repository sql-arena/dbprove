#include "dbprove/theorem/theorem.h"
#include "init.h"
#include "runner.h"
#include "query.h"

namespace dbprove::theorem::test {

void run_test_query(Proof& proof, const std::string& sql, const std::vector<std::string>& tables) {
    auto& ensure = proof.ensureSchema("test");
    for (const auto& table : tables) {
        ensure.ensure(table);
    }
    Query query(sql, "test");
    Runner runner(proof.factory());
    runner.serialExplain(std::move(query), proof);
}

void run_test(Proof& proof) {
    run_test_query(proof, 
        "SELECT pk.val, fk.fk_val "
        "FROM test.pk pk "
        "JOIN test.fk fk ON pk.id = fk.pk_id",
        {"test.pk", "test.fk"}
    );
}

void run_test_scan(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk",
        {"test.pk"}
    );
}

void run_test_join_left(Proof& proof) {
    run_test_query(proof, 
        "SELECT pk.val, fk.fk_val "
        "FROM test.pk pk "
        "LEFT JOIN test.fk fk ON pk.id = fk.pk_id",
        {"test.pk", "test.fk"}
    );
}

void run_test_join_full(Proof& proof) {
    run_test_query(proof, 
        "SELECT pk.val, fk.fk_val "
        "FROM test.pk pk "
        "FULL JOIN test.fk fk ON pk.id = fk.pk_id",
        {"test.pk", "test.fk"}
    );
}

void init() {
    auto& test = addTheorem("test-join-inner", "Simple join test for Databricks implementation", run_test);
    categoriseTheorem(test, Category::TEST);
    tagTheorem(test, Tag("TEST"));

    auto& test_left = addTheorem("test-join-left", "Left join test for Databricks implementation", run_test_join_left);
    categoriseTheorem(test_left, Category::TEST);
    tagTheorem(test_left, Tag("TEST"));

    auto& test_full = addTheorem("test-join-full", "Full join test for Databricks implementation", run_test_join_full);
    categoriseTheorem(test_full, Category::TEST);
    tagTheorem(test_full, Tag("TEST"));

    auto& test_scan = addTheorem("test-scan", "Simple scan test for Databricks implementation", run_test_scan);
    categoriseTheorem(test_scan, Category::TEST);
    tagTheorem(test_scan, Tag("TEST"));
}

} // namespace dbprove::theorem::test
