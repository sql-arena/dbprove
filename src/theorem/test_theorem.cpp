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

void run_test_scan_filter(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk WHERE val = 'A'",
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
        "SELECT val FROM test.pk UNION DISTINCT SELECT fk_val FROM test.fk",
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

void run_test_sort(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk ORDER BY val",
        {"test.pk"}
    );
}

void run_test_limit(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk LIMIT 2",
        {"test.pk"}
    );
}

void run_test_limit_sort(Proof& proof) {
    run_test_query(proof, 
        "SELECT val FROM test.pk ORDER BY val LIMIT 2",
        {"test.pk"}
    );
}

void run_test_projection_alias_outer_group(Proof& proof) {
    run_test_query(
        proof,
        "SELECT supp_val, COUNT(*) "
        "FROM (SELECT pk.val AS supp_val FROM test.pk pk) t "
        "GROUP BY supp_val",
        {"test.pk"}
    );
}

void run_test_group_alias_join(Proof& proof) {
    run_test_query(
        proof,
        "SELECT pk.id "
        "FROM test.pk pk "
        "JOIN ("
        "  SELECT fk.pk_id AS p_partkey, MIN(fk.id) AS min_id "
        "  FROM test.fk fk "
        "  GROUP BY fk.pk_id"
        ") x ON pk.id = x.p_partkey",
        {"test.pk", "test.fk"}
    );
}

void run_test_aggregate_sort_alias(Proof& proof) {
    run_test_query(
        proof,
        "SELECT val, sum_id "
        "FROM ("
        "  SELECT val, SUM(id) AS sum_id "
        "  FROM test.pk "
        "  GROUP BY val "
        "  ORDER BY SUM(id) DESC"
        ") q",
        {"test.pk"}
    );
}

void run_test_aggregate_sort_expression(Proof& proof) {
    run_test_query(
        proof,
        "SELECT val, SUM(id) AS sum_id "
        "FROM test.pk "
        "GROUP BY val "
        "ORDER BY SUM(id) DESC",
        {"test.pk"}
    );
}

void run_test_projection_aggregate_expression_alias(Proof& proof) {
    run_test_query(
        proof,
        "SELECT g.pk_id, g.scaled_avg "
        "FROM ("
        "  SELECT fk.pk_id, 0.2 * AVG(fk.id) AS scaled_avg "
        "  FROM test.fk fk "
        "  GROUP BY fk.pk_id"
        ") g "
        "JOIN test.pk pk ON pk.id = g.pk_id",
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
    addTestTheorem("test-scan-filter", "Scan with filter test for Databricks implementation", run_test_scan_filter);
    addTestTheorem("test-groupby", "Simple groupby test for Databricks implementation", run_test_groupby);
    addTestTheorem("test-union", "Simple union test for Databricks implementation", run_test_union);
    addTestTheorem("test-union-distinct", "Union distinct test for Databricks implementation", run_test_union_distinct);
    addTestTheorem("test-aggregate", "Simple aggregate test for Databricks implementation", run_test_aggregate);
    addTestTheorem("test-subquery-in", "Subquery IN test for Databricks implementation", run_test_subquery_in);
    addTestTheorem("test-subquery-not-in", "Subquery NOT IN test for Databricks implementation", run_test_subquery_not_in);
    addTestTheorem("test-sort", "Sort test for Databricks implementation", run_test_sort);
    addTestTheorem("test-limit", "Limit test for Databricks implementation", run_test_limit);
    addTestTheorem("test-limit-sort", "Limit with sort test for Databricks implementation", run_test_limit_sort);
    addTestTheorem("test-projection-alias-outer-group", "Projection alias survives to outer group by", run_test_projection_alias_outer_group);
    addTestTheorem("test-group-alias-join", "Grouped key alias survives to outer join condition", run_test_group_alias_join);
    addTestTheorem("test-aggregate-sort-alias", "Aggregate alias projected after sort", run_test_aggregate_sort_alias);
    addTestTheorem("test-aggregate-sort-expression", "Aggregate order by expression with alias output", run_test_aggregate_sort_expression);
    addTestTheorem("test-projection-aggregate-expression-alias", "Aggregate expression aliases survive projection and join", run_test_projection_aggregate_expression_alias);
}

} // namespace dbprove::theorem::test
