#pragma once
#include <string_view>
#include "sql_type.h"

namespace sql {
class ColumnBase {
public:
  virtual ~ColumnBase() = default;
  virtual std::string_view name() const = 0;
  virtual SqlType type() const = 0;
  virtual size_t size() const = 0;
  virtual bool isNullable() const = 0;
};
}