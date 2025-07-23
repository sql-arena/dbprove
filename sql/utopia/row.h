#pragma once
#include "row_base.h"
#include <vector>

namespace sql::utopia {
    class Result;

    class Row final : public sql::RowBase {
    public:
        explicit Row(std::vector<SqlVariant>&& values): values(std::move(values)) {
        }
        explicit Row(): values({}) {
        }
        ~Row() override = default;

    protected:
        [[nodiscard]] SqlVariant get(size_t index) const override {
            return values[index]; // Placeholder implementation
        };

    private:
        const std::vector<SqlVariant> values;

    };
}
