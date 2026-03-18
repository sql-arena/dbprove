#pragma once
#include "explain/column.h"
#include "explain/node.h"

namespace sql::explain {
class Column;

class Projection : public Node {
  static const constexpr char* symbol_ = "π";

public:
  Projection(const std::vector<Column>& columns_projected);

  std::string compactSymbolic() const override;

  std::string renderMuggle(size_t max_width) const override;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  std::vector<Column> columns_projected;
  std::vector<Column> synthetic_columns_projected;
  bool include_input_columns = true;
};
}
