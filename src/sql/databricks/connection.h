#pragma once
#include <dbprove/sql/sql.h>


namespace sql::databricks {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const CredentialAccessToken token_;

public:
  explicit Connection(const CredentialAccessToken& credential, const Engine& engine);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  const TypeMap& typeMap() const override;
  void analyse(std::string_view table_name) override;

};
}