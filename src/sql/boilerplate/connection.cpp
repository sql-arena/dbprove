#include "connection.h"
#include "result.h"
#include "sql_exceptions.h"

namespace sql::boilerplate {
class Connection::Pimpl {
public:
  explicit Pimpl() {
  }

  void executeRaw(const std::string_view statement) {
    // TODO: Implement
  }

  void close() {
    // TODO: Implement
  }

  ~Pimpl() {
    close();
  };
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>()) {
}

Connection::~Connection() {
  impl_->close();
}

void Connection::execute(std::string_view statement) {
  return impl_->executeRaw(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  // TODO: Talk to the engine and acquire the memory structure of the result, then pass it to result
  return std::make_unique<Result>(nullptr);
}


void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  // TODO: Implement using Bulk API (if available) of the database
}
}