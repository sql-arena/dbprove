#include "theorem.h"
#include "runner.h"
#include "dbprove_theorem/embedded_sql.h"
#include <dbprove/generator/tpch.h>
#include "init.h"
#include "../query.h"
#include <plog/Log.h>

using namespace dbprove::theorem;

Proof& tpch_ensure_basics(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.part").ensure("tpch.supplier").ensure("tpch.nation").ensure("tpch.region").
        ensure("tpch.customer").ensure("tpch.lineitem").ensure("tpch.orders");
  return proof;
}

void tpch_q01(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q01_sql), proof);
}

void tpch_q02(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.part").ensure("tpch.supplier").ensure("tpch.partsupp").ensure("tpch.nation").
        ensure("tpch.region");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q02_sql), proof);
}

void tpch_q03(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders").ensure("tpch.customer");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q03_sql), proof);
}

void tpch_q04(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q04_sql), proof);
}

void tpch_q05(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders").ensure("tpch.nation").ensure("tpch.region").
        ensure("tpch.customer").ensure("tpch.supplier");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q05_sql), proof);
}

void tpch_q06(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem");

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
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders").ensure("tpch.customer").
        ensure("tpch.nation");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q10_sql), proof);
}

void tpch_q11(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.partsupp").ensure("tpch.supplier").ensure("tpch.nation");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q11_sql), proof);
}

void tpch_q12(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q12_sql), proof);
}

void tpch_q13(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.customer").ensure("tpch.orders");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q13_sql), proof);
}

void tpch_q14(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.part");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q14_sql), proof);
}

void tpch_q15(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.supplier").ensure("tpch.lineitem");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q14_sql), proof);
}

void tpch_q16(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.partsupp").ensure("tpch.part");
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q16_sql), proof);
}

void tpch_q17(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.part");
  const Runner runner(proof.factory());

  const bool is_postgres_bad = proof.factory().engine().type() == sql::Engine::Type::Postgres;

  if (is_postgres_bad) {
    /* Performance for Q17 is so horrible for Postgres that we have to create an assistaing index */
    try {
      proof.factory().create()->execute("CREATE INDEX ix_q17 ON tpch.lineitem(l_partkey)");
    } catch (const std::exception& e) {
      PLOGW << "Could not create assisting index for TPC-H Q17: " << e.what();
    }
  }
  runner.serialExplain(Query(resource::q17_sql), proof);
  if (is_postgres_bad) {
    try {
      proof.factory().create()->execute("DROP INDEX ix_q17");
    } catch (const std::exception& e) {
      // NOOP
    }
  }
}


void tpch_q18(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.orders").ensure("tpch.lineitem").ensure("tpch.customer");;
  const Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q18_sql), proof);
}


void tpch_q19(Proof& proof) {
  proof.ensure("tpch.lineitem").ensure("tpch.part");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q19_sql), proof);
}


void tpch_q20(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.supplier").ensure("tpch.nation").ensure("tpch.partsupp").
        ensure("tpch.lineitem").ensure("tpch.part");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q20_sql), proof);
}

void tpch_q21(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.lineitem").ensure("tpch.orders").ensure("tpch.supplier").
        ensure("tpch.nation");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q21_sql), proof);
}

void tpch_q22(Proof& proof) {
  proof.ensureSchema("tpch").ensure("tpch.customer").ensure("tpch.orders");
  Runner runner(proof.factory());
  runner.serialExplain(Query(resource::q22_sql), proof);
}

void register_tpch(unsigned query_number, const TheoremFunction& func, bool handroll = false) {
  std::string q = std::format("Q{:02}", query_number);
  if (handroll) {
    q += "HR";
  }
  auto& t = addTheorem("TPCH-" + q, "TPC-H " + q + " Analysis", func);
  categoriseTheorem(t, Category::PLAN);
  tagTheorem(t, Tag("TPC-H"));
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
  register_tpch(12, tpch_q12);
  register_tpch(13, tpch_q13);
  register_tpch(14, tpch_q14);
  register_tpch(15, tpch_q15);
  register_tpch(16, tpch_q16);
  register_tpch(17, tpch_q17);
  register_tpch(18, tpch_q18);
  register_tpch(19, tpch_q19);
  register_tpch(20, tpch_q20);
  register_tpch(21, tpch_q21);
  register_tpch(22, tpch_q22);

  is_initialised = true;
}
}