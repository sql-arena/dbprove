#pragma once
#include <stdexcept>

#include "row_base.h"
#include <vector>
#include "utopia_data.h"

namespace sql::utopia {
    class Result;

    class Row final : public sql::RowBase {
    public:
        explicit Row(std::vector<SqlVariant>&& values): values(std::move(values)) {
        }
        explicit Row() {
        }
        explicit Row(const UtopiaData data): values({}) {
          switch (data) {
            case UtopiaData::TEST_ROW:
              values.push_back(SqlVariant(1));
              values.push_back(SqlVariant("abc"));
              values.push_back(SqlVariant(0.42));
              break;
          }
        }
        ~Row() override = default;

    protected:
        [[nodiscard]] SqlVariant get(size_t index) const override {
            if (index >= values.size()) {
              throw std::out_of_range("Row index of " + std::to_string(values.size()) + " is out of range");
            }
            return values[index]; // Placeholder implementation
        };

    private:
        std::vector<SqlVariant> values;

    };
}
