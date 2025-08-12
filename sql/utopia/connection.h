#pragma once
#include "connection_base.h"
#include "result_base.h"
#include "row_base.h"
#include "sql_type.h"
#include "row.h"

namespace sql::utopia {
class Connection final : public sql::ConnectionBase {
public:
  explicit Connection();
  void execute(std::string_view statement) override;
  std::unique_ptr<sql::ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<sql::ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<sql::RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchScalar(std::string_view statement) override;
  void bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) override;
};

}