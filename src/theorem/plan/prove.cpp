#include "theorem.h"
#include "runner.h"
#include "query.h"
#include "dbprove_theorem/embedded_sql.h"
#include <dbprove/generator/tpch.h>
#include "init.h"

using namespace dbprove::theorem;

void tpch_q01(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q01_sql), proof);
}

void tpch_q19(Proof& proof) {
  proof.ensure("tpch.lineitem").ensure("tpch.part");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q19_sql), proof);
}


namespace dbprove::theorem::plan {
void init() {
  static bool is_initialised = false;
  if (is_initialised) {
    return;
  }

  addTheorem(Type::PLAN, "PLAN-TPCH-Q01", "TPC-H Q01 Analysis", tpch_q01);
  addTheorem(Type::PLAN, "PLAN-TPCH-Q09", "TPC-H Q19 Analysis and Bloom Check", tpch_q19);

  is_initialised = true;
}
}