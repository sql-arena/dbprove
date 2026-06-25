#include "theorem.h"
#include "runner.h"
#include "dbprove_theorem/embedded_sql.h"
#include <dbprove/generator/job.h>
#include <dbprove/generator/tpch.h>
#include "init.h"
#include "../query.h"
#include <plog/Log.h>
#include <array>

using namespace dbprove::theorem;

namespace {

constexpr std::pair<std::string_view, std::string_view> kJobQueries[] = {
#include "job_query_registry.inc"
};

Proof& job_ensure_basics(Proof& proof) {
  proof.ensureDataset("job");
  return proof;
}

void run_job_query(Proof& proof, const std::string_view sql) {
  const Runner runner(proof.factory());
  runner.serialExplain(Query(sql, proof.theorem.name.c_str(), proof.theorem.expectedRowCount()), proof);
}

void register_job(std::string_view job_name, std::string_view sql) {
  auto& theorem = addTheorem("PLAN-JOB-" + std::string(job_name),
                             "Join Order Benchmark " + std::string(job_name) + " Analysis",
                             [sql](Proof& proof) {
                               run_job_query(job_ensure_basics(proof), sql);
                             },
                             std::nullopt,
                             "JOB-" + std::string(job_name));
  categoriseTheorem(theorem, Category::PLAN);
  tagTheorem(theorem, Tag("JOB"));
  tagTheorem(theorem, Tag("IMDB"));
}

} // namespace

Proof& tpch_ensure_basics(Proof& proof) {
  proof.ensureDataset("tpch_sf1");
  return proof;
}

void run_tpch_query(Proof& proof, const std::string_view sql) {
  const Runner runner(proof.factory());
  runner.serialExplain(Query(sql, proof.theorem.name.c_str(), proof.theorem.expectedRowCount()), proof);
}

void register_plan_join_check(std::string_view theorem_name, std::string_view description,
                              std::string_view sql) {
  auto& theorem = addTheorem("PLAN-JOIN-" + std::string(theorem_name),
                             std::string(description),
                             [sql](Proof& proof) {
                               run_tpch_query(tpch_ensure_basics(proof), sql);
                             },
                             1);
  categoriseTheorem(theorem, Category::PLAN);
  tagTheorem(theorem, Tag("PLAN-JOIN"));
  tagTheorem(theorem, Tag("join"));
}

void tpch_q01(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q01_sql);
}

void tpch_q02(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q02_sql);
}

void tpch_q03(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q03_sql);
}

void tpch_q04(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q04_sql);
}

void tpch_q05(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q05_sql);
}

void tpch_q06(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q06_sql);
}

void tpch_q07(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q07_sql);
}

void tpch_q08(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q08_sql);
}

void tpch_q09(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q09_sql);
}

void tpch_q10(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q10_sql);
}

void tpch_q11(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q11_sql);
}

void tpch_q12(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q12_sql);
}

void tpch_q13(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q13_sql);
}

void tpch_q14(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q14_sql);
}

void tpch_q15(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q15_sql);
}

void tpch_q16(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q16_sql);
}

void tpch_q17(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q17_sql);
}


void tpch_q18(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q18_sql);
}


void tpch_q19(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q19_sql);
}


void tpch_q20(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q20_sql);
}

void tpch_q21(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q21_sql);
}

void tpch_q22(Proof& proof) {
  run_tpch_query(tpch_ensure_basics(proof), resource::q22_sql);
}

void register_tpch(unsigned query_number, const TheoremFunction& func,
                   std::optional<sql::RowCount> expected_row_count = std::nullopt, bool handroll = false) {
  std::string q = std::format("Q{:02}", query_number);
  if (handroll) {
    q += "HR";
  }
  auto& t = addTheorem("PLAN-TPC-H-" + q,
                       "TPC-H " + q + " Analysis",
                       func,
                       expected_row_count,
                       "TPCH-" + q);
  categoriseTheorem(t, Category::PLAN);
  tagTheorem(t, Tag("TPC-H"));
}


namespace dbprove::theorem::plan {
void init() {
  static bool is_initialised = false;
  if (is_initialised) {
    return;
  }
  register_tpch(1, tpch_q01, 4);
  register_tpch(2, tpch_q02, 495);
  register_tpch(3, tpch_q03, 10);
  register_tpch(4, tpch_q04, 5);
  register_tpch(5, tpch_q05, 5);
  register_tpch(6, tpch_q06, 1);
  register_tpch(7, tpch_q07, 4);
  register_tpch(8, tpch_q08, 2);
  register_tpch(9, tpch_q09, 175);
  register_tpch(10, tpch_q10, 38182);
  register_tpch(11, tpch_q11, 1225);
  register_tpch(12, tpch_q12, 2);
  register_tpch(13, tpch_q13, 42);
  register_tpch(14, tpch_q14, 1);
  register_tpch(15, tpch_q15, 1);
  register_tpch(16, tpch_q16, 18282);
  register_tpch(17, tpch_q17, 1);
  register_tpch(18, tpch_q18, 9);
  register_tpch(19, tpch_q19, 1);
  register_tpch(20, tpch_q20, 158);
  register_tpch(21, tpch_q21, 396);
  register_tpch(22, tpch_q22, 7);

  register_plan_join_check("LEFT",
                           "LEFT JOIN",
                           resource::inner_match_sql);
  register_plan_join_check("LEFT-SELECTIVE",
                           "LEFT JOIN with selective filter",
                           resource::inner_match_filter_part_sql);
  register_plan_join_check("LEFT-LEFT",
                           "LEFT JOIN",
                           resource::left_orders_customer_sql);
  register_plan_join_check("LEFT-LEFT-SELECTIVE",
                           "LEFT JOIN with selective filter",
                           resource::left_orders_customer_filter_customer_sql);
  register_plan_join_check("LEFT-RIGHT",
                           "RIGHT JOIN",
                           resource::left_customer_orders_sql);
  register_plan_join_check("LEFT-RIGHT-SELECTIVE",
                           "RIGHT JOIN with selective filter",
                           resource::left_customer_orders_filter_customer_sql);
  register_plan_join_check("FULL-LEFT",
                           "LEFT JOIN",
                           resource::full_orders_customer_sql);
  register_plan_join_check("FULL-LEFT-SELECTIVE",
                           "LEFT JOIN with selective filter",
                           resource::full_orders_customer_filter_customer_sql);
  register_plan_join_check("SEMI-LEFT",
                           "SEMI LEFT",
                           resource::semi_orders_customer_sql);
  register_plan_join_check("SEMI-LEFT-SELECTIVE",
                           "SEMI LEFT with selective filter",
                           resource::semi_orders_customer_filter_customer_sql);
  register_plan_join_check("SEMI-RIGHT",
                           "SEMI RIGHT",
                           resource::semi_customer_orders_sql);
  register_plan_join_check("SEMI-RIGHT-SELECTIVE",
                           "SEMI RIGHT with selective filter",
                           resource::semi_customer_orders_filter_customer_sql);
  register_plan_join_check("ANTI-LEFT",
                           "ANTI LEFT",
                           resource::anti_orders_customer_sql);
  register_plan_join_check("ANTI-LEFT-SELECTIVE",
                           "ANTI LEFT with selective filter",
                           resource::anti_orders_customer_filter_customer_sql);
  register_plan_join_check("ANTI-RIGHT",
                           "ANTI RIGHT",
                           resource::anti_customer_orders_sql);
  register_plan_join_check("ANTI-RIGHT-SELECTIVE",
                           "ANTI RIGHT with selective filter",
                           resource::anti_customer_orders_filter_customer_sql);
  for (const auto& [job_name, sql] : kJobQueries) {
    register_job(job_name, sql);
  }

  is_initialised = true;
}
}
