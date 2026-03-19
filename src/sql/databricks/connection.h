#pragma once
#include <dbprove/sql/sql.h>
#include <nlohmann/json_fwd.hpp>


namespace sql::databricks {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const CredentialAccessToken token_;

public:
  explicit Connection(const CredentialAccessToken& credential, const Engine& engine, std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;
  void execute(std::string_view statement) override;
  std::string execute(std::string_view statement, const std::map<std::string, std::string>& tags);
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override;
  std::string version() override;
  nlohmann::json queryHistory(const std::string& statement_id) const;
  void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override;
  const TypeMap& typeMap() const override;
  std::optional<RowCount> tableRowCount(const std::string_view table) override;
  void analyse(std::string_view table_name) override;
  bool shouldSkipDatasetTuning(std::string_view dataset) override;

private:
  nlohmann::json getLiveScrapedPlan(std::string_view statement);
  std::string getLiveExplainCost(std::string_view statement);
  std::string getOrgId() const;
  struct QueryHistoryInfo {
    std::string query_id;
    std::string cache_query_id;
    int64_t start_time_ms;
  };
  QueryHistoryInfo getQueryHistoryInfo(const std::string& statement_id);
  std::string runNodeDumpPlan(const std::string& statement_id,
                                    const std::string& startTimeMs) const;
};
}
