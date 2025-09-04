#include "connection.h"

#include <fstream>

#include "result.h"
#include "result_base.h"
#include "sql_exceptions.h"
#include "credential.h"
#include <string_view>
#include <clickhouse/client.h>

namespace ch = clickhouse;

namespace sql::clickhouse {
class Connection::Pimpl {
public:
  const Credential& credential;

  explicit Pimpl(const CredentialPassword& credential)
    : credential(credential) {
    client = std::make_unique<ch::Client>(ch::ClientOptions()
                                          .SetHost(credential.host)
                                          .SetPort(credential.port)
                                          .SetUser(credential.username)
                                          .SetPassword(credential.password.value_or(""))
                                          .SetDefaultDatabase(credential.database));
  }

  std::unique_ptr<ch::Client> client;
};


std::vector<std::string> Connection::tableColumns(const std::string_view table) const {
  std::vector<std::string> result;
  auto& client = *impl_->client.get();
  ch::Block block;
  client.Select("DESCRIBE TABLE" + std::string(table), [&](const ch::Block& b) {
    block = b;
  });

  for (size_t i = 0; i < block.GetColumnCount(); ++i) {
    const auto& column = block[i];
    auto name = column->As<ch::ColumnString>()->At(i);
    result.push_back(std::string(name));
  }
  return result;
}

Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(credential)) {
}

Connection::~Connection() {
}

void Connection::execute(std::string_view statement) {
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  // TODO: Talk to the engine and acquire the memory structure of the result, then pass it to result
  return std::make_unique<Result>(nullptr);
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  // TODO: Implement result set scrolling
  return fetchAll(statement);
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
  return nullptr;
}

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  auto& client = *impl_->client.get();
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
    size_t batch_size = 1000;
    size_t row_count = 0;
    std::string line;
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
        client.Insert(std::string(table), block);
        row_count = 0;
      }
    }
    if (row_count > 0) {
      ch::Block block;
      for (size_t j = 0; j < columns.size(); ++j) {
        block.AppendColumn(columns[j], column_data[j]);
      }
      client.Insert(std::string(table), block);
    }
  }
}
}