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
  explicit Connection(const CredentialPassword& credential, const Engine& engine, std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  void constructTable(std::string_view ddl,
                      std::span<const std::filesystem::path> source_stems,
                      dbprove::StorageVariant storage_variant) override;
  const TypeMap& typeMap() const override;
  std::string version() override;
  void createSchema(std::string_view schema_name) override;
  void analyse(std::string_view table_name) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  void declareForeignKey(std::string_view fk_table, std::span<std::string_view> fk_columns, std::string_view pk_table,
                         std::span<std::string_view> pk_columns) override;
};
}
