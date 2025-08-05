#include <CLI/CLI.hpp>
#include <iostream>

#include "sql/connection_base.h"
#include "sql/Credential.h"
#include "sql/Engine.h"
#include <string>
#include <version>
#include <map>
#include <format>
#include "theorem_type.h"
#include "theorem/ee/prove.h"
#include "theorem/cli/prove.h"
#include "theorem/se/prove.h"
#include "theorem/wlm/prove.h"
#include "theorem/plan/prove.h"
#include "common/string.h"
#include "ux/Terminal.h"

void parseTheorems( std::map<TheoremType, std::vector<std::string>>& theoremMap, std::vector<std::string> theorems) {
    for (const auto& t: theorems) {
        bool parsed = false;
        for (const auto& pair: theorem_types) {
            if (t.starts_with(pair.first)) {
                theoremMap[pair.second].push_back(t);
                parsed = true;
                break;
            }
        }
        if (!parsed) {
            std::cerr << "Unknown theorem type: " << t << ".";
            std::cerr << "the following prefixes are supported: ";
            std::cerr << join(theorem_names, ",");
            for (const auto& pair: theorem_types) {
                std::cerr << pair.first << ", ";
            }
            std::cerr << std::endl;
            std::exit(1);
        }
    }
}


int main(int argc, char** argv) {
    CLI::App app{"dbprove"};

    std::string platform;
    std::string database;
    std::string username;
    std::string password;
    std::vector<std::string> all_theorems;
    std::string engine_arg;
    uint32_t port;

    app.add_option(
        "-e, --engine",
        engine_arg,
        "Database Engine to use")->transform(CLI::CheckedTransformer(sql::Engine::known_names));
    app.add_option("-d,--database", database, "Database to use")->default_val("test");
    app.add_option("-p,--port", port, "Port to use")->default_val(0);
    app.add_option("-U,--username", username, "Username ")->default_val("test");
    app.add_option("-P,--password", password, "Password. If omitted, will prompt")->default_val("test");
    app.add_option("-t,--access-token", password, "Access Token")->default_val("");
    app.add_option("-T,--theorem", all_theorems, "Which theorems to prove")->delimiter(',');


    sql::Engine engine(engine_arg);

    CLI11_PARSE(app, argc, argv);


    Terminal::configure();

    auto credentials = sql::Credential(database, port, username, password);

    std::map<TheoremType, std::vector<std::string>> theoremMap;
    parseTheorems(theoremMap, all_theorems);

    for (const auto&[type, theorems] : theoremMap) {
        switch (type) {
            case TheoremType::CLI:
                cli::prove(theorems, engine, credentials);
                break;
            case TheoremType::EE:
                ee::prove(theorems, engine, credentials);
                break;
            case TheoremType::SE:
                se::prove(theorems, engine, credentials);
                break;
            case TheoremType::PLAN:
                plan::prove(theorems, engine, credentials);
                break;
            case TheoremType::WLM:
                wlm::prove(theorems, engine, credentials);
            break;
        }
    }

    return 0;
}
