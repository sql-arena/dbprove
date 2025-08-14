#pragma once
#include <string>
#include "generator/integer_range.h"

namespace generator
{
    /// @brief Generates Phone Number strings as per TPC-H spec section 4.2.2.9
    class TpchPhone
    {
        IntegerRange<uint16_t> local_number1_;
        IntegerRange<uint16_t> local_number2_;
        IntegerRange<uint16_t> local_number3_;
        const size_t num_nations_;
        IntegerRange<> country_code_;

    public:
        TpchPhone();
        std::string next();
    };
}
