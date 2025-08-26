#include "theorem.h"
#include "runner.h"
#include "dbprove_theorem/embedded_sql.h"
#include <dbprove/generator/tpch.h>
#include "init.h"
#include "../query.h"

using namespace dbprove::theorem;

Proof& tpch_ensure_basics(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.part")
       .ensure("tpch.supplier")
       .ensure("tpch.nation")
       .ensure("tpch.region")
       .ensure("tpch.customer")
       .ensure("tpch.lineitem")
       .ensure("tpch.orders");
  return proof;
}

void tpch_q01(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q01_sql), proof);
}

void tpch_q02(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.part")
       .ensure("tpch.supplier")
       .ensure("tpch.partsupp")
       .ensure("tpch.nation")
       .ensure("tpch.region");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q02_sql), proof);
}

void tpch_q03(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.lineitem")
       .ensure("tpch.orders")
       .ensure("tpch.customer");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q03_sql), proof);
}

void tpch_q04(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.lineitem")
       .ensure("tpch.orders");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q04_sql), proof);
}

void tpch_q05(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.lineitem")
       .ensure("tpch.orders")
       .ensure("tpch.nation")
       .ensure("tpch.region")
       .ensure("tpch.customer")
       .ensure("tpch.supplier");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q05_sql), proof);
}

void tpch_q06(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.lineitem");

  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q06_sql), proof);
}

void tpch_q07(Proof& proof) {
  tpch_ensure_basics(proof);
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q07_sql), proof);
}

void tpch_q08(Proof& proof) {
  tpch_ensure_basics(proof);
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q08_sql), proof);
}

void tpch_q09(Proof& proof) {
  tpch_ensure_basics(proof).ensure("tpch.partsupp");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q09_sql), proof);
}

void tpch_q10(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.lineitem")
       .ensure("tpch.orders")
       .ensure("tpch.customer")
       .ensure("tpch.nation");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q10_sql), proof);
}

void tpch_q11(Proof& proof) {
  proof.ensureSchema("tpch")
       .ensure("tpch.partsupp")
       .ensure("tpch.supplier")
       .ensure("tpch.nation");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q11_sql), proof);
}


void tpch_q19(Proof& proof) {
  proof.ensure("tpch.lineitem").ensure("tpch.part");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q19_sql), proof);
}


void register_tpch(unsigned query_number, const TheoremFunction& func) {
  std::string q = std::format("Q{:02}", query_number);
  addTheorem(Type::PLAN,
             "PLAN-TPCH-" + q,
             "TPC-H " + q + " Analysis",
             func);
}


namespace dbprove::theorem::plan {
void init() {
  static bool is_initialised = false;
  if (is_initialised) {
    return;
  }
  register_tpch(1, tpch_q01);
  register_tpch(2, tpch_q02);
  register_tpch(3, tpch_q03);
  register_tpch(4, tpch_q04);
  register_tpch(5, tpch_q05);
  register_tpch(6, tpch_q06);
  register_tpch(7, tpch_q07);
  register_tpch(8, tpch_q08);
  register_tpch(9, tpch_q09);
  register_tpch(10, tpch_q10);
  register_tpch(11, tpch_q11);
  register_tpch(19, tpch_q19);

  is_initialised = true;
}
}