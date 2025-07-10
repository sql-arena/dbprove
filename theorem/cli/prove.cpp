#include "prove.h"

#include "ux/PreAmple.h"
#include <string>
#include <vector>
#include "theorem/types.h"
#include "ux.h"
#include <chrono>
#include "runner/runner.h"

#include "connection_factory.h"

void cli_1(const std::string &theorem, sql::Engine engine, const sql::Credential &credentials) {
    sql::ConnectionFactory factory(engine, credentials);
    auto sql = Query("SELECT 1", theorem.c_str());
    Runner runner(factory);
    runner.serial(sql, 1000);
}


namespace cli {
    void prove(const std::vector<std::string> &theorems, sql::Engine engine,
               const sql::Credential &credentials) {
        ux::PreAmple("CLI - Client Interface Theorems");
        static TheoremCommandMap cliMap = {
            {"CLI-1", {"Measure roundtrip Time on NOOP", &cli_1}}
        };

        for (const auto &t: theorems) {
            if (cliMap.count(t) > 0) {
                ux::PreAmpleTheorem(t);
                cliMap[t].func(t, engine, credentials);
            } else {
                ux::Error("Unknown theorem: " + t);
            }
        }
    }
}
