#include "prove.h"

#include "theorem/types.h"
#include <vector>

#include "PreAmple.h"
#include "connection_factory.h"
#include "runner/runner.h"
#include "sql/Engine.h"
#include "sql/Credential.h"
#include "sql/query.h"
#include "theorem/run_theorems.h"
#include "generator/tpch/tpch.h"

void tpch_q01(const std::string& theorem,  const TheoremState& state) {

  state.generator.generate("REGION");
  sql::ConnectionFactory factory(state.engine, state.credentials);
  auto cn = factory.create();
  state.generator.load("REGION", *cn);
  auto sql = Query("SELECT * FROM REGION", theorem.c_str());
  Runner runner(factory);
  runner.serial(sql, 1000);
}


namespace plan {
void prove(const std::vector<std::string>& theorems, const TheoremState& state) {
  ux::PreAmple("PLAN - Planner Theorems");
  static TheoremCommandMap planMap = {
    {"PLAN-TPCH-Q01", {"TPC-H Q01 Analysis", tpch_q01}}
  };

  run_theorems("PLAN", planMap, theorems, state);
}
}