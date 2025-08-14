#pragma once
#include <dbprove/sql/sql.h>


namespace sql::databricks
{
    class Result;
    class Row final : public RowBase
    {
        friend class Result;
        Result* data_;
        RowCount currentRow_;
        bool ownsResult_;


    protected:
        [[nodiscard]] SqlVariant get(size_t index) const override;

    public:
        explicit Row(Result* data, const RowCount current_row, const bool owns_result = false)
            : data_(data)
            , currentRow_(current_row)
            , ownsResult_(owns_result)
        {
        };
        ~Row() override;

        ColumnCount columnCount() const override;
    };
}
