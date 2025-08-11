#include <CLI/CLI.hpp>
#include <iostream>

#include "sql/connection_base.h"
#include "sql/Credential.h"
#include "sql/Engine.h"
#include <string>
#include <version>
#include <map>
#include <format>
#include <ranges>
#include "theorem_type.h"
#include "theorem/ee/prove.h"
#include "theorem/cli/prove.h"
#include "theorem/se/prove.h"
#include "theorem/wlm/prove.h"
#include "theorem/plan/prove.h"
#include "common/string.h"
#include "ux/Terminal.h"

void parseTheorems(std::map<TheoremType, std::vector<std::string>>& theoremMap,
                   const std::vector<std::string>& theorems) {
  for (const auto& t : theorems) {
    bool parsed = false;
    for (const auto& [name, theorem_type] : theorem_types) {
      if (t.starts_with(name)) {
        theoremMap[theorem_type].push_back(t);
        parsed = true;
        break;
      }
    }
    if (!parsed) {
      std::cerr << "Unknown theorem type: " << t << ".";
      std::cerr << "the following prefixes are supported: ";
      std::cerr << join(theorem_names, ",");
      for (const auto& pair : theorem_types) {
        std::cerr << pair.first << ", ";
      }
      std::cerr << std::endl;
      std::exit(1);
    }
  }
}

sql::Credential parseCredentials(
    const sql::Engine& engine,
    const std::string& host,
    const uint16_t port,
    const std::string& database,
    const std::optional<std::string>& username,
    const std::optional<std::string>& password,
    const std::optional<std::string>& token) {
  const auto engine_name = engine.name();

  try {
    switch (engine.type()) {
      case sql::Engine::Type::MariaDB:
      case sql::Engine::Type::Postgres:
      case sql::Engine::Type::SQLServer:
      case sql::Engine::Type::Oracle: {
        if (!username) {
          throw std::invalid_argument("Username is required for " + engine_name);
        }
        if (!password) {
          throw std::invalid_argument("Password is required for " + engine_name);
        }
        return sql::CredentialPassword(host, database, port, username.value(), password.value());
      }
      case sql::Engine::Type::Utopia:
        return sql::CredentialNone();
      case sql::Engine::Type::Databricks: {
        if (!token) {
          throw std::invalid_argument("Token is required for " + engine_name);
        }
        return sql::CredentialAccessToken(engine, host, database, token.value());
      }
    }
    throw std::invalid_argument("Cannot generate credentials for engine: " + engine_name);
  } catch (const std::invalid_argument& e) {
    std::cerr << e.what() << std::endl;
    std::exit(1);
  }
}

int main(int argc, char** argv) {
  CLI::App app{"dbprove"};

  std::string platform;
  std::optional<std::string> database = std::nullopt;
  std::optional<std::string> username = std::nullopt;
  std::optional<std::string> password = std::nullopt;
  std::optional<std::string> host = std::nullopt;
  std::optional<std::string> token = std::nullopt;
  std::vector<std::string> all_theorems;
  std::string engine_arg;
  uint32_t port;

  app.set_help_flag("-?", "--help");
  app.add_option(
      "-e, --engine",
      engine_arg);

  app.add_option("-h,--host",
                 host, "Host or endpoint");
  app.add_option("-p,--port",
                 port, "Port to use")->default_val(0);
  app.add_option("-d,--database",
                 database, "Database to use");
  app.add_option("-U,--username",
                 username, "Username ");
  app.add_option("-P,--password",
                 password, "Password. If omitted, will prompt");
  app.add_option("-t,--access-token",
                 token, "Access Token");
  app.add_option("-T,--theorem",
                 all_theorems, "Which theorems to prove")->delimiter(',');

  CLI11_PARSE(app, argc, argv);

  const sql::Engine engine(engine_arg);
  Terminal::configure();

  if (all_theorems.size() == 0) {
    /* If user did not supply theorems, default to all of them */
    for (auto& theorem_name : std::views::keys(theorem_types)) {
      all_theorems.push_back(std::string(theorem_name));
    }
  }

  database = engine.defaultDatabase(database);
  host = engine.defaultHost(database);
  port = engine.defaultPort(port);
  username = engine.defaultUsername(username);
  token = engine.defaultToken(token);

  auto credentials =
      parseCredentials(engine,
                       host.value(),
                       port,
                       database.value(),
                       username,
                       password,
                       token);

  std::map<TheoremType, std::vector<std::string>> theoremMap;
  parseTheorems(theoremMap, all_theorems);

  for (const auto& [type, theorems] : theoremMap) {
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
}

