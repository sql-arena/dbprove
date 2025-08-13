#include "prove.h"

#include "theorem/types.h"
#include <vector>

#include "ux/PreAmple.h"
#include "sql/connection_factory.h"
#include "../runner.h"
#include "sql/Engine.h"
#include "sql/Credential.h"
#include "sql/query.h"
#include "theorem/run_theorems.h"
#include "generator/tpch/tpch.h"
#include "theorem/embedded_sql.h"

void tpch_q01(TheoremProof& proof) {
  proof.ensure("lineitem");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q01_sql), proof);
}

void tpch_q19(TheoremProof& proof) {
  proof.ensure("lineitem").ensure("part");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q19_sql), proof);
}


namespace plan {
void prove(const std::vector<std::string>& theorems, TheoremState& input_state) {
  ux::PreAmple("PLAN - Planner Theorems");
  static TheoremCommandMap planMap = {
    {"PLAN-TPCH-Q01", {"TPC-H Q01 Analysis", tpch_q01}},
    {"PLAN-TPCH-Q19", {"TPC-H Q19 Analysis", tpch_q19}},
  };

  run_theorems("PLAN", planMap, theorems, input_state);
}
}