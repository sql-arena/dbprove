#include "connection.h"
#include "result.h"
#include "sql_exceptions.h"

namespace sql::boilerplate {
class Connection::Pimpl {
public:
  explicit Pimpl() {}
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>()) {
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

}

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
  validateSourcePaths(source_paths);
}
}