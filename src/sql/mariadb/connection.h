#pragma once
#include "connection_base.h"


namespace sql::mariadb {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

public:
  explicit Connection(const Credential& credential, const Engine& engine);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  void close() override;
};
}