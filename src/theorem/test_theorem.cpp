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

void run_test_groupby(Proof& proof) {
    run_test_query(proof, 
        "SELECT val, COUNT(*) FROM test.pk GROUP BY val",
        {"test.pk"}
    );
}

void run_test_union(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk UNION ALL SELECT val FROM test.pk",
        {"test.pk"}
    );
}

void run_test_union_distinct(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk UNION SELECT fk_val FROM test.fk",
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

    auto& test_groupby = addTheorem("test-groupby", "Simple groupby test for Databricks implementation", run_test_groupby);
    categoriseTheorem(test_groupby, Category::TEST);
    tagTheorem(test_groupby, Tag("TEST"));

    auto& test_union = addTheorem("test-union", "Simple union test for Databricks implementation", run_test_union);
    categoriseTheorem(test_union, Category::TEST);
    tagTheorem(test_union, Tag("TEST"));

    auto& test_union_distinct = addTheorem("test-union-distinct", "Union distinct test for Databricks implementation", run_test_union_distinct);
    categoriseTheorem(test_union_distinct, Category::TEST);
    tagTheorem(test_union_distinct, Tag("TEST"));
}

} // namespace dbprove::theorem::test
