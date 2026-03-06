#pragma once
#include "connection_base.h"
#include "result_base.h"
#include <memory>

namespace sql::odbc {
class Connection : public ConnectionBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;

public:
  explicit Connection(const Credential& credential, const Engine& engine, std::string connection_string, std::optional<std::string> artifacts_path = std::nullopt);
  ~Connection() override;
  virtual void execute(std::string_view statement) override;
  virtual std::unique_ptr<ResultBase> fetchAll(std::string_view statement) override;
  virtual std::string version() override { return ""; }
  virtual void bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) override {}
  virtual const TypeMap& typeMap() const override { static TypeMap empty; return empty; }
  virtual void analyse(std::string_view table_name) override {}
  virtual std::unique_ptr<explain::Plan> explain(std::string_view statement, std::optional<std::string_view> name = std::nullopt) override { return nullptr; }

  const char* connectionString() const;
  void close() override;
};
}