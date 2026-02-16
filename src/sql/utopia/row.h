#pragma once
#include "utopia_data.h"
#include <dbprove/sql/sql.h>
#include <stdexcept>
#include <vector>

namespace sql::utopia {
class Result;

class Row final : public sql::RowBase {
public:
  explicit Row(std::vector<SqlVariant>&& values)
    : values(std::move(values)) {
  }

  explicit Row(const std::vector<SqlVariant>& values)
    : values(values) {
  }

  explicit Row() {
  }

  explicit Row(const UtopiaData data)
    : values({}) {
    switch (data) {
      case UtopiaData::TEST_ROW:
        values.push_back(SqlVariant(1));
        values.push_back(SqlVariant("abc"));
        values.push_back(SqlVariant(0.42));
        break;
      default:
      break;
    }
  }

  ~Row() override = default;

  bool operator==(const RowBase& other) const override {
    return false;
    /*
    const auto& other_row = dynamic_cast<const Row&>(other);
    return values == other_row.values;
    */
  };
  ColumnCount columnCount() const override {
    return values.size();
  };


protected:
  [[nodiscard]] SqlVariant get(size_t index) const override {
    if (index >= values.size()) {
      throw std::out_of_range("Row index of " + std::to_string(values.size()) + " is out of range");
    }
    return values[index]; // Placeholder implementation
  }


private:
  std::vector<SqlVariant> values;
};
}