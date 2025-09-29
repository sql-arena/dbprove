#pragma once
#include "connection_base.h"


namespace sql::snowflake {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
public:
  explicit Connection(const Credential& credential, const Engine& engine);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  void bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) override;
};
}