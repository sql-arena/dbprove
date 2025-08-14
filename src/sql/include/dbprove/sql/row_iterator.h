#pragma once
#include <memory>
#include <iterator>
#include "row_base.h"

namespace sql
{
    // Forward declaration
    class ResultBase;
    class RowBase;

    class RowIterator
    {
    public:
        // Iterator traits
        using iterator_category = std::input_iterator_tag;
        using value_type = std::shared_ptr<RowBase>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;


        // Begin iterator constructor
        explicit RowIterator(ResultBase& result);

        const RowBase& operator*() const;
        RowIterator& operator++();
        RowIterator operator++(int);

        bool operator==(const RowIterator& other) const;
        bool operator!=(const RowIterator& other) const;

    private:
        ResultBase& result_;
        const RowBase* currentRow_;
        bool is_end_;
    };

    class SentinelResult;

    class RowIterable
    {
    public:
        explicit RowIterable(ResultBase& result)
            : result_(result)
        {
        }

        RowIterator begin() const
        {
            return RowIterator(result_);
        }

        RowIterator end()
        {
            return RowIterator(getSentinel());
        }

    private:
        friend class RowIterator;
        ResultBase& result_;
        static ResultBase& getSentinel();
    };
}
