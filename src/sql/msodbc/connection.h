#pragma once
#include <filesystem>
#include "connection_base.h"


namespace sql::msodbc {
class Connection final : public odbc::Connection {
public:
  explicit Connection(const Credential& credential, const Engine& engine);
  std::string version() override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  const TypeMap& typeMap() const override;
  void analyse(std::string_view table_name) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
};
}
