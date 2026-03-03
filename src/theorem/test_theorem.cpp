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

void run_test_aggregate(Proof& proof) {
    run_test_query(proof, 
        "SELECT SUM(id), COUNT(*), MIN(val), MAX(val) FROM test.pk",
        {"test.pk"}
    );
}

void run_test_subquery_in(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk WHERE id IN (SELECT pk_id FROM test.fk)",
        {"test.pk", "test.fk"}
    );
}

void run_test_subquery_not_in(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk WHERE id NOT IN (SELECT pk_id FROM test.fk)",
        {"test.pk", "test.fk"}
    );
}

void addTestTheorem(const std::string& name, const std::string& description, const TheoremFunction& fn) {
    auto& test = addTheorem(name, description, fn);
    categoriseTheorem(test, Category::TEST);
    tagTheorem(test, Tag("TEST"));
}

void init() {
    addTestTheorem("test-join-inner", "Simple join test for Databricks implementation", run_test);
    addTestTheorem("test-join-left", "Left join test for Databricks implementation", run_test_join_left);
    addTestTheorem("test-join-full", "Full join test for Databricks implementation", run_test_join_full);
    addTestTheorem("test-scan", "Simple scan test for Databricks implementation", run_test_scan);
    addTestTheorem("test-groupby", "Simple groupby test for Databricks implementation", run_test_groupby);
    addTestTheorem("test-union", "Simple union test for Databricks implementation", run_test_union);
    addTestTheorem("test-union-distinct", "Union distinct test for Databricks implementation", run_test_union_distinct);
    addTestTheorem("test-aggregate", "Simple aggregate test for Databricks implementation", run_test_aggregate);
    addTestTheorem("test-subquery-in", "Subquery IN test for Databricks implementation", run_test_subquery_in);
    addTestTheorem("test-subquery-not-in", "Subquery NOT IN test for Databricks implementation", run_test_subquery_not_in);
}

} // namespace dbprove::theorem::test
