#include "connection.h"
#include "result.h"
#include "result_base.h"
#include "sql_exceptions.h"
#include "credential.h"
#include "block_holder.h"
#include "group_by.h"
#include "join.h"
#include "projection.h"
#include "row.h"
#include "scan.h"
#include "selection.h"
#include "sort.h"
#include "explain/plan.h"

#include <string_view>
#include <fstream>
#include <regex>
#include <cstdlib>

#include "limit.h"
#include <clickhouse/client.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

namespace ch = clickhouse;


namespace sql::clickhouse {
namespace {
bool isActualsQuery(const std::string_view statement) {
  return statement.find("DBPROVE_ACTUALS") != std::string_view::npos;
}

void bootstrapSession(ch::Client& client) {
  // Match ANSI outer-join semantics so COUNT(right_col) does not count
  // ClickHouse default-filled values for unmatched right-side rows.
  client.Execute("SET join_use_nulls = 1");
}

int actualsTimeoutSeconds() {
  constexpr int default_timeout = 120;
  constexpr int min_timeout = 1;
  constexpr int max_timeout = 120;
  if (const auto* env = std::getenv("DBPROVE_ACTUALS_TIMEOUT_SEC")) {
    try {
      const auto parsed = std::stoi(env);
      if (parsed >= min_timeout && parsed <= max_timeout) {
        return parsed;
      }
      LOG_WARNING << "Ignoring DBPROVE_ACTUALS_TIMEOUT_SEC=" << parsed
                  << " outside allowed range [" << min_timeout << ", " << max_timeout << "]";
    } catch (...) {
      LOG_WARNING << "Ignoring invalid DBPROVE_ACTUALS_TIMEOUT_SEC='" << env << "'";
    }
  }
  return default_timeout;
}
}

void handleClickHouseException(ch::Client& client, const ch::ServerException& e) {
  try {
    client.ResetConnection();
    bootstrapSession(client);
  } catch (const ch::Exception& e) {
    // NOOP: we want to make sure we still get the throw below.
  }
  const auto error_code = e.GetCode();
  const auto what = e.what();
  switch (error_code) {
    case 60:
      throw InvalidObjectException(what);
    case 62:
      throw SyntaxException(what);
    default:
      throw InvalidException("Unknown ClickHouse Error Code " + std::to_string(error_code) + " what " + what);
  }
}

class Connection::Pimpl {
public:
  const CredentialPassword& credential;

  explicit Pimpl(const CredentialPassword& credential)
    : credential(credential) {
  }

  ch::Client& getClient() {
    if (!client) {
      ch::ClientOptions options;
      options.SetHost(credential.host).SetPort(credential.port).SetUser(credential.username).
              SetPassword(credential.password.value_or("")).SetDefaultDatabase(credential.database);
      client = std::make_unique<ch::Client>(options);
      bootstrapSession(*client);
    }
    return *client;
  }

  std::unique_ptr<ch::Client> client;
};


std::vector<std::string> Connection::tableColumns(const std::string_view table) {
  std::vector<std::string> ret;
  auto result = fetchAll("DESCRIBE TABLE " + std::string(table));

  for (auto& row : result->rows()) {
    ret.push_back(row[0].asString());
  }
  return ret;
}

Connection::Connection(const CredentialPassword& credential, const Engine& engine, std::optional<std::string> artifacts_path)
  : ConnectionBase(credential, engine, std::move(artifacts_path))
  , impl_(std::make_unique<Pimpl>(credential)) {
}

Connection::~Connection() {
}


void Connection::execute(std::string_view statement) {
  auto& client = impl_->getClient();
  auto sql = std::string(statement);
  static const std::regex delete_all_regex(R"(^\s*DELETE\s+FROM\s+([a-zA-Z0-9_\.]+)\s*;?\s*$)", std::regex::icase);
  std::smatch delete_match;
  if (std::regex_match(sql, delete_match, delete_all_regex) && delete_match.size() > 1) {
    sql = "TRUNCATE TABLE " + delete_match[1].str();
  }
  try {
    client.Execute(sql);
  } catch (const ch::ServerException& e) {
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    throw std::runtime_error(e.what());
  }
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  /* Clickhouse contains the Block object and expects you to process it immediately
    in the lambda.
    After that, the block goes out of scope and is deconstructed,
    But we want to hang on to it and pass to Result: We do this dance
    of taking a shared pointer (which will create a deep copy) before passing it on
    to the constructor of Result.

    ...Ugly!
  */

  auto& client = impl_->getClient();
  std::vector<std::shared_ptr<ch::Block>> blocks;
  const bool actuals_query = isActualsQuery(statement);
  const auto timeout_seconds = actuals_query ? actualsTimeoutSeconds() : 0;
  try {
    if (actuals_query) {
      client.Execute("SET max_execution_time = " + std::to_string(timeout_seconds));
      client.Execute("SET timeout_overflow_mode = 'throw'");
    }
    client.Select(std::string(statement), [&](const ch::Block& b) {
      if (b.GetRowCount() == 0) {
        return;
      }
      blocks.push_back(std::make_shared<ch::Block>(b));
    });
    if (actuals_query) {
      client.Execute("SET max_execution_time = 0");
      client.Execute("SET timeout_overflow_mode = 'break'");
    }
  } catch (const ch::ServerException& e) {
    if (actuals_query) {
      try {
        client.Execute("SET max_execution_time = 0");
        client.Execute("SET timeout_overflow_mode = 'break'");
      } catch (const ch::Exception&) {
        // ignore cleanup failure
      }
    }
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    if (actuals_query) {
      try {
        client.Execute("SET max_execution_time = 0");
        client.Execute("SET timeout_overflow_mode = 'break'");
      } catch (const ch::Exception&) {
        // ignore cleanup failure
      }
    }
    throw std::runtime_error(e.what());
  }
  return std::make_unique<Result>(std::make_unique<BlockHolder>(blocks));
}


void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  for (const auto& path : source_paths) {
    const auto file_name = path.filename().string();
    const auto statement =
        "INSERT INTO " + std::string(table) +
        " SELECT * FROM file('table_data/" + file_name + "', 'CSVWithNames')"
        " SETTINGS format_csv_delimiter='|'";
    execute(statement);
  }
}

std::string Connection::version() {
  return fetchScalar("SELECT version() as v").asString();
}

void Connection::createSchema(const std::string_view schema_name) {
  // ClickHouse calls a schema a database
  execute("CREATE DATABASE " + std::string(schema_name));
}

std::string Connection::translateDialectDdl(const std::string_view ddl) const {
  auto result = std::string(ddl);
  // Normalise generic type names used by test generators.
  result = std::regex_replace(result, std::regex(R"(\bSTRING\b)", std::regex::icase), "String");
  result = std::regex_replace(result, std::regex(R"(\bBIGINT\b)", std::regex::icase), "Int64");
  result = std::regex_replace(result, std::regex(R"(\bINT\b)", std::regex::icase), "Int32");
  // Map generic TPCH DDL to a valid ClickHouse table definition.
  // We use MergeTree with ORDER BY tuple() as a neutral default that works for all tables.
  result = std::regex_replace(result, std::regex(R"(;\s*$)"), " ENGINE = MergeTree() ORDER BY tuple();");
  return result;
}

void Connection::declareForeignKey(std::string_view fk_table, std::span<std::string_view> fk_columns,
                                   std::string_view pk_table, std::span<std::string_view> pk_columns) {
  // ClickHouse doesn't support these
}

void Connection::analyse(std::string_view table_name) {
  // ClickHouse has no need for Analyse because it doesn't have a planner.
}
}
