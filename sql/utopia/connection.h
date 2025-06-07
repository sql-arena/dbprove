#pragma once
#include "Credential.h"
#include "connection_base.h"
#include "result_base.h"
#include "row_base.h"
#include "sql_type.h"
#include "row.h"

namespace utopia {
class Connection final : public sql::ConnectionBase {
public:
  explicit Connection(const sql::Credential& credential);
  void execute(std::string_view statement) override;
  std::unique_ptr<sql::ResultBase> fetchAll(std::string_view statement) override;
  std::unique_ptr<sql::ResultBase> fetchMany(std::string_view statement) override;
  std::unique_ptr<sql::RowBase> fetchRow(std::string_view statement) override;
  SqlVariant fetchValue(std::string_view statement) override;
  void bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) override;
};
}