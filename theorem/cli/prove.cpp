#include "prove.h"

#include "ux/PreAmple.h"
#include <string>
#include <vector>
#include "theorem/types.h"
#include <chrono>
#include "../runner.h"
#include "theorem/run_theorems.h"
#include "sql/connection_factory.h"

void cli_1(TheoremProof& proof) {
  auto sql = Query("SELECT 1");
  Runner runner(proof.factory());
  runner.serial(sql, 1);
}


namespace cli {
void prove(const std::vector<std::string>& theorems, TheoremState& state) {
  ux::PreAmple("CLI - Client Interface Theorems");
  static TheoremCommandMap cliMap = {
      {"CLI-1", {"Measure roundtrip Time on NOOP", &cli_1}}
  };

  run_theorems("CLI", cliMap, theorems, state);
}
}