#pragma once
#include "row_base.h"


namespace sql::boilerplate {
    class Row : public RowBase {
    protected:
        [[nodiscard]] SqlVariant get(size_t index) const override;

    public:
        ColumnCount columnCount() const override;
    };
}
