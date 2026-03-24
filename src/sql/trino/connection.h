#pragma once

#include "connection_base.h"
#include "credential.h"

namespace sql::trino {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

public:
  explicit Connection(const CredentialPassword& credential, const Engine& engine, std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;

  const TypeMap& typeMap() const override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  std::string version() override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  void close() override;
};
}
