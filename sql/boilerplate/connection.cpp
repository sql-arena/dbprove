#include "connection.h"
#include "result.h"

namespace sql::boilerplate {
class Connection::Pimpl {
public:
  explicit Pimpl() {}
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>()) {
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
}

void Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
  validateSourcePaths(source_paths);
}
}