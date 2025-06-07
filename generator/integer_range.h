#pragma once
#include <random>

#include "generator_object.h"

namespace generator
{
    template <typename T = uint64_t>
    class IntegerRange : GeneratorObject
    {
        const T minValue_;
        const T maxValue_;
        std::uniform_int_distribution<T> distribution_;

    public:
        explicit IntegerRange(const T minValue, const T maxValue)
            : minValue_(minValue)
            , maxValue_(maxValue)
            , distribution_(std::uniform_int_distribution(minValue, maxValue))
        {
        }

        T next()
        {
            return distribution_(gen_);
        };
    };
}
