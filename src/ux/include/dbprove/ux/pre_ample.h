#pragma once
#include <format>
#include <iostream>
#include <ostream>
#include <../../../../../build/msvc/vcpkg_installed/x64-windows/include/rang.hpp>
#include "Terminal.h"
#include "../../../Glyph.h"


namespace ux {
    inline void PreAmple(std::string title) {
        std::cout << rang::fg::gray;
        std::cout << BOX_TOP_LEFT;
        for (auto i = 0; i < Terminal::SCREEN_WIDTH - 2; i++) {
            std::cout << BOX_HORIZONTAL;
        }
        std::cout << BOX_TOP_RIGHT << std::endl;
        std::cout << VERTICAL_LINE << " ";
        std::cout << rang::fg::yellow << title ;
        auto padding = Terminal::SCREEN_WIDTH - 4 - title.length();
        std::cout << std::string(padding, ' ');
        std::cout << rang::fg::gray << " " << VERTICAL_LINE << std::endl;
        std::cout << HASH_BUILD_CHILD;
        for (auto i = 0; i < Terminal::SCREEN_WIDTH - 2; i++) {
            std::cout << BOX_HORIZONTAL;
        }
        std::cout << BOX_BOTTOM_RIGHT;
        std::cout << rang::fg::reset << std::endl;
    }


    inline void PreAmpleTheorem(std::string_view theorem) {
        std::cout << std::format("{:10}", theorem) << std::endl;
    }
}