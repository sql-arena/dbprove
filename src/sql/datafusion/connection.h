#pragma once

#include "connection_base.h"
#include "credential.h"

#include <mutex>
#include <nlohmann/json_fwd.hpp>

namespace sql::datafusion {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::shared_ptr<Pimpl> impl_;

  std::unique_ptr<ResultBase> fetchJsonQuery(std::string_view statement);
  nlohmann::json fetchPhysicalPlanJson(std::string_view statement) const;
  std::recursive_mutex& driverMutex() const;

public:
  explicit Connection(const CredentialNone& credential, const Engine& engine,
                      std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;

  const TypeMap& typeMap() const override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  void constructTable(std::string_view ddl,
                      std::span<const std::filesystem::path> source_stems,
                      dbprove::StorageVariant storage_variant,
                      IcebergRegistrationCallback register_iceberg_table = nullptr) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  void analyse(std::string_view table_name) override;
  std::string version() override;
  void close() override;
};
}
