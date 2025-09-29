#pragma once
#include "connection_base.h"
#include "credential.h"
#include <memory>
#include <string_view>
#include <vector>

namespace sql::clickhouse {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  std::vector<std::string> tableColumns(std::string_view table);

public:
  explicit Connection(const CredentialPassword& credential, const Engine& engine);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchMany(std::string_view statement) override;
  void bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) override;
  std::string version() override;
  void createSchema(std::string_view schema_name) override;
  [[nodiscard]] std::string translateDialectDdl(const std::string_view ddl) const override;
  void analyse(std::string_view table_name) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
};
}
