#pragma once
#include <iostream>
#include <ostream>
#include <rang.hpp>
#include <string_view>

namespace ux {
    inline void Error(std::string_view message) {
        std::cout << rang::fg::red << message << rang::fg::reset << std::endl;
    }
}
