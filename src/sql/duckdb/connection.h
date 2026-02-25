#pragma once
#include <dbprove/sql/sql.h>

namespace sql::duckdb {
class Connection final : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

public:
  explicit Connection(const CredentialFile& credential, const Engine& engine);

  const TypeMap& typeMap() const override;
  ~Connection() override;

  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
  std::string version() override;
  void close() override;
};
} // namespace sql::duckdb