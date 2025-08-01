#pragma once

#include <duckdb/common/types/data_chunk.hpp>

#include "row_base.h"
#include "sql_type.h"


namespace sql::duckdb
{
    class Result;

    class Row final : public RowBase
    {
        ::duckdb::DataChunk* dataChunk_ = nullptr; ///< Parent chunk
        size_t chunkIndex_ = 0;
        RowCount row_number_ = 0;

    public:
        explicit Row()
        {
        }

        explicit Row(::duckdb::DataChunk* chunk, const size_t chunkIndex, const RowCount rowNumber)
            : dataChunk_(chunk)
            , chunkIndex_(chunkIndex)
            , row_number_(rowNumber)
        {
        }

        Row(const Row& other)
            : dataChunk_(other.dataChunk_)
            , chunkIndex_(other.chunkIndex_)
        {
        }

        ~Row() override;

        bool operator==(const RowBase& other) const override
        {
            const auto& other_row = static_cast<const Row&>(other);
            return (other_row.dataChunk_ == dataChunk_ && other_row.chunkIndex_ == chunkIndex_);
        };

        Row& operator=(const Row& other)
        {
            if (this != &other) {
                chunkIndex_ = other.chunkIndex_;
            }
            return *this;
        }


        [[nodiscard]] ColumnCount columnCount() const override;;

        [[nodiscard]] RowCount rowNumber() const
        {
            return row_number_;
        }

    protected:
        [[nodiscard]] SqlVariant get(const size_t index) const override;
    };
}
