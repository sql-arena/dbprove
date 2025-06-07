#pragma once
#include "double_range.h"

namespace generator
{
    class VariableInteger : DoubleRange
    {
    public:
        VariableInteger()
            : DoubleRange(0.0, 1.0)
        {
        }

        size_t next(size_t n)
        {
            return static_cast<size_t>(DoubleRange::next() * static_cast<double>(n));
        };
    };
}
