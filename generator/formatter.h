#pragma once
#include <format>
#include "generator_object.h"

namespace generator
{
    class Formatter : GeneratorObject
    {
        const std::string pattern_;

    public:
        explicit Formatter(std::string pattern)
            : pattern_(pattern)
        {
        }

        template <typename... Args>
        std::string next(Args&&... args)
        {
            return std::vformat(pattern_, std::make_format_args(std::forward<Args>(args)...));
        }
    };
}
