#pragma once
#include "../include/dbprove/sql/connection_base.h"

namespace sql::msodbc {
class Connection final : public ConnectionBase {
  /**
   * @note: Use the Pimpl Pattern here to avoid polluting the main namespace and to keep all
   * engine specific noise in the implementation files
   */
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
public:
  explicit Connection(const Credential& credential, const Engine& engine_type);
  ~Connection() override;

  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  void bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) override;
};
} // namespace sql::postgres