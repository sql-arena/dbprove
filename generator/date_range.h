#pragma once
#include <random>
#include <chrono>
#include "generator_object.h"

namespace generator
{
    class DateRange : GeneratorObject
    {
        const std::chrono::sys_days minValue_;
        const std::chrono::sys_days maxValue_;
        std::uniform_int_distribution<int64_t> distribution_;

    public:
        explicit DateRange(std::chrono::sys_days minValue, std::chrono::sys_days maxValue);

        std::chrono::sys_days next();
    };
}
