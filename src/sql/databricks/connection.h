#pragma once
#include <dbprove/sql/sql.h>
#include <nlohmann/json_fwd.hpp>


namespace sql::databricks {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const CredentialAccessToken token_;

public:
  explicit Connection(const CredentialAccessToken& credential, const Engine& engine);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::string execute(std::string_view statement, const std::map<std::string, std::string>& tags);
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement) override;
  nlohmann::json getHistory();
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  const TypeMap& typeMap() const override;
  void analyse(std::string_view table_name) override;

private:
  std::string getOrgId(std::string& workspace_url);
  struct QueryHistoryInfo {
    std::string query_id;
    std::string cache_query_id;
    int64_t start_time_ms;
  };
  QueryHistoryInfo getQueryHistoryInfo(const std::string& statement_id);
};
}