#pragma once
#include "../connection_base.h"

namespace sql::postgres {
class Connection : public ConnectionBase {
  /**
   * @note: Use the Pimpl Pattern here to avoid polluting the main namespace and to keep all
   * engine specific noise in the implementation files
   */
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
public:
  explicit Connection(const Credential& credential);

  ~Connection() override;

  void execute(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchValue(std::string_view statement) override;
  void bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) override;
};
} // namespace sql::postgres