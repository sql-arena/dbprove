#pragma once
#include "connection_base.h"

namespace sql::duckdb {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
public:
  explicit Connection(const CredentialPassword& credential, const Engine& engine);

  const TypeMap& typeMap() const override;
  ~Connection() override;

  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  void bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;

};
} // namespace sql::duckdb
