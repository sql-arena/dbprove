#include "prove.h"

#include "theorem/types.h"
#include <vector>

#include "ux/PreAmple.h"
#include "sql/connection_factory.h"
#include "runner/runner.h"
#include "sql/Engine.h"
#include "sql/Credential.h"
#include "sql/query.h"
#include "theorem/run_theorems.h"
#include "generator/tpch/tpch.h"

void tpch_q01(const std::string& theorem,  const TheoremState& state) {
  sql::ConnectionFactory factory(state.engine, state.credentials);
  auto cn = factory.create();
  state.generator.ensure("lineitem", *cn);
}

void tpch_q05(const std::string& theorem,  const TheoremState& state) {
  sql::ConnectionFactory factory(state.engine, state.credentials);
  auto cn = factory.create();
  state.generator.ensure("lineitem", *cn);
}


namespace plan {
void prove(const std::vector<std::string>& theorems, const TheoremState& state) {
  ux::PreAmple("PLAN - Planner Theorems");
  static TheoremCommandMap planMap = {
    {"PLAN-TPCH-Q01", {"TPC-H Q01 Analysis", tpch_q01}},
    {"PLAN-TPCH-Q05", {"TPC-H Q05 Analysis", tpch_q05}},
  };

  run_theorems("PLAN", planMap, theorems, state);
}
}