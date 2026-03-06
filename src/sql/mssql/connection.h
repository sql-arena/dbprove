#pragma once
#include <filesystem>
#include "odbc/connection.h"


namespace sql::mssql {
std::string makeConnectionString(const Credential& credential, std::optional<std::string> database_override = std::nullopt);

class Connection final : public odbc::Connection {
public:
  explicit Connection(const Credential& credential, const Engine& engine, std::optional<std::string> artifacts_path = std::nullopt);
  std::string version() override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  const TypeMap& typeMap() const override;
  void analyse(std::string_view table_name) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;

private:
  std::string fetchLivePlan(std::string_view statement);
};
} // namespace sql::mssql
