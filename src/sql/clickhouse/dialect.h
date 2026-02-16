#pragma once
#include <map>
#include <string>
#include <vector>

#include "dbprove/sql/expression.h"


namespace sql::clickhouse {
class ExplainCtx;

struct ClickHouseDialect final : EngineDialect {
  ExplainCtx& ctx_;

  explicit ClickHouseDialect(ExplainCtx& ctx)
    : ctx_(ctx) {
  }

  void preRender(std::vector<Token>& tokens) override;

  std::string postRender(std::string toRender) override;

  const std::map<std::string_view, std::string_view>& engineFunctions() const override;

  const std::set<std::string_view>& castFunctions() const override;;
};
}