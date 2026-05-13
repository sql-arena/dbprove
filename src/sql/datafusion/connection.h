#pragma once

#include "connection_base.h"
#include "credential.h"

#include <nlohmann/json_fwd.hpp>

namespace sql::datafusion {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

  std::unique_ptr<ResultBase> fetchJsonQuery(std::string_view statement);
  nlohmann::json fetchPhysicalPlanJson(std::string_view statement) const;

public:
  explicit Connection(const CredentialNone& credential, const Engine& engine,
                      std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;

  const TypeMap& typeMap() const override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  void analyse(std::string_view table_name) override;
  bool shouldSkipDatasetTuning(std::string_view dataset) override;
  std::string version() override;
  void close() override;
};
}
