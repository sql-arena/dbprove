#include "connection.h"

#include <fstream>
#include <regex>

#include "result.h"
#include "result_base.h"
#include "sql_exceptions.h"
#include "credential.h"
#include <string_view>

#include "block_holder.h"
#include "row.h"
#include <clickhouse/client.h>
#include <plog/Log.h>

namespace ch = clickhouse;


namespace sql::clickhouse {
void handleClickHouseException(ch::Client& client, const ch::ServerException& e) {
  try {
    client.ResetConnection();
  } catch (const ch::Exception& e) {
    // NOOP
  }

  switch (e.GetCode()) {
    case 60:
      throw InvalidObjectException(e.what());
    case 62:
      throw SyntaxException(e.what());
    default:
      throw InvalidException("Unknown ClickHouse Error Code " + std::to_string(e.GetCode()) + " what " + e.what());
  }
}

class Connection::Pimpl {
public:
  const CredentialPassword& credential;

  explicit Pimpl(const CredentialPassword& credential)
    : credential(credential) {
    ch::ClientOptions options;
    options.SetHost(credential.host)
           .SetPort(credential.port)
           .SetUser(credential.username)
           .SetPassword(credential.password.value_or(""))
           .SetDefaultDatabase(credential.database);
    client = std::make_unique<ch::Client>(options);
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

Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(credential)) {
}

Connection::~Connection() {
}


void Connection::execute(std::string_view statement) {
  auto& client = *impl_->client;
  try {
    client.Execute(std::string(statement));
  } catch (const ch::ServerException& e) {
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    throw std::runtime_error(e.what());
  }
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  /* Clickhouse contains the Block object and expects you to process it immediately
    in the lamdba. After that, the block goes out of scope and is deconstructed,
    But we want to hang on to it and pass to Result, so we have do do this dance
    of taking a shared pointer (which will create a deep copy) before passing it on
    to the constructor of Result. Ugly!
  */

  auto& client = *impl_->client;
  std::vector<std::shared_ptr<ch::Block>> blocks;
  try {
    client.Select(std::string(statement), [&](const ch::Block& b) {
      if (b.GetRowCount() == 0) {
        return;
      }
      blocks.push_back(std::make_shared<ch::Block>(b));
    });
  } catch (const ch::ServerException& e) {
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    throw std::runtime_error(e.what());
  }
  return std::make_unique<Result>(std::make_unique<BlockHolder>(blocks));
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  return fetchAll(statement);
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  auto& client = *impl_->client;
  auto columns = tableColumns(table);
  std::vector<std::shared_ptr<ch::ColumnString>> column_data(columns.size());
  for (auto& col : column_data) {
    col = std::make_shared<ch::ColumnString>();
  }
  for (const auto& path : source_paths) {
    std::ifstream file(path);
    if (!file.is_open()) {
      throw std::runtime_error("Failed to open file: " + path.string());
    }
    size_t batch_size = 100000;
    size_t row_count = 0;
    std::string line;
    std::getline(file, line); // Skip header
    while (std::getline(file, line)) {
      std::istringstream ss(line);
      std::string value;
      size_t i = 0;
      while (std::getline(ss, value, '|')) {
        column_data[i]->Append(value);
        ++i;
      }
      ++row_count;
      if (row_count >= batch_size) {
        ch::Block block;
        for (size_t j = 0; j < columns.size(); ++j) {
          block.AppendColumn(columns[j], column_data[j]);
          column_data[j] = std::make_shared<ch::ColumnString>();
        }
        try {
          client.Insert(std::string(table), block);
        } catch (const ch::ServerException& e) {
          handleClickHouseException(client, e);
        }
        row_count = 0;
      }
    }
    if (row_count > 0) {
      ch::Block block;
      for (size_t j = 0; j < columns.size(); ++j) {
        block.AppendColumn(columns[j], column_data[j]);
      }
      try {
        client.Insert(std::string(table), block);
      } catch (const ch::ServerException& e) {
        handleClickHouseException(client, e);
      }
    }
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
  std::string result = std::string(ddl);
  if (ddl.contains("tpch.lineitem")) {
    result = std::regex_replace(result, std::regex(";"), "PRIMARY KEY (l_orderkey, l_linenumber);");
  }
  return result;
}

void Connection::analyse(std::string_view table_name) {
  // ClickHouse has no need for Analyse because it does not have a planner
}
}