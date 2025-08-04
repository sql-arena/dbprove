#pragma once
#include "row_base.h"
#include "sql_type.h"


namespace duckdb
{
    class DataChunk;
}

namespace sql::duckdb
{
    class Result;

    class Row final : public RowBase
    {
        ::duckdb::DataChunk* dataChunk_ = nullptr; ///< Parent chunk
        size_t chunkIndex_ = 0;
        RowCount row_number_ = 0;
        /**
         *  Set this row owns its result (single row fetch)
         *  @note: Ideally, we would have liked a unique_ptr<Result> here. But to do that, we would have to include
         *  "result.h". But result must also include "row.h" (because of its Row member) so we end up circular
         *  Hence, we fall back to manual memory management here to break the circularity. Mea Culpa!
         */
        Result* ownsResult_ = nullptr;
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

        explicit Row(const Row& other)
            : dataChunk_(other.dataChunk_)
            , chunkIndex_(other.chunkIndex_)
        {
        }
        explicit Row(const Row& row, Result* owningResult);

        ~Row() override;

        bool operator==(const RowBase& other) const override
        {
            const auto& other_row = static_cast<const Row&>(other);
            return (other_row.dataChunk_ == dataChunk_ && other_row.chunkIndex_ == chunkIndex_);
        };

        Row& operator=(const Row& other)
        {
            if (this != &other) {
                dataChunk_ = other.dataChunk_;
                chunkIndex_ = other.chunkIndex_;
                row_number_ = other.row_number_;
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
