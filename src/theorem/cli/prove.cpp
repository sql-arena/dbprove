#include "theorem.h"
#include "runner.h"
#include "init.h"
#include <vector>

namespace dbprove::theorem::cli {
void cli_1(Proof& proof) {
  auto sql = Query("SELECT 1");
  Runner runner(proof.factory());
  runner.serial(sql, 1);
}


void init() {
  addTheorem(Type::CLI, "CLI-1", "Measure roundtrip Time on NOOP", cli_1);
}
}