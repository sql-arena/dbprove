#pragma once
#include "connection_base.h"
#include "result_base.h"
#include <memory>

namespace sql::odbc {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

public:
  explicit Connection(const Credential& credential, const Engine& engine, std::string connection_string);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  const char* connectionString() const;
  void close() override;
};
}