#pragma once
#include <libpq-fe.h>

#include "row_base.h"

namespace sql::postgres
{
    class Result;

    class Row final : public RowBase
    {
        PGresult* result_;
        int row_number_ = 0; ///< PG uses signed integer for row number... Odd...
        const bool contained_ = true; ///< Does the row have a containing result?
    public:
        explicit Row(PGresult* result, const int row_number)
            : result_(result)
            , row_number_(row_number)
        {
        }

        explicit Row(PGresult* result)
            : result_(result)
            , contained_(false)
        {
        }

        ~Row() override;

        bool operator==(const RowBase& other) const override
        {
            // TODO: Would be good to have a type field here in case someone tries a cast with a completely different row
            const auto& other_row = static_cast<const Row&>(other);
            return (other_row.result_ == result_ && other_row.row_number_ == row_number_);
        };

        [[nodiscard]] ColumnCount columnCount() const override
        {
            return PQnfields(result_);
        };

        [[nodiscard]] RowCount rowNumber() const
        {
            return row_number_;
        }

        Row(const Row& other)
            : result_(other.result_)
            , row_number_(other.row_number_)
        {
        }

        Row& operator=(const Row& other)
        {
            if (this != &other) {
                result_ = other.result_;
                row_number_ = other.row_number_;
            }
            return *this;
        }

    protected:
        [[nodiscard]] SqlVariant get(const size_t index) const override;
    };
}
