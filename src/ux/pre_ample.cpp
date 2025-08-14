#include "pre_ample.h"
#include "glyphs.h"
#include "terminal.h"
#include <rang.hpp>
#include <iostream>

namespace dbprove::ux {
void PreAmple(std::string title) {
  std::cout << rang::fg::gray;
  std::cout << BOX_TOP_LEFT;
  for (auto i = 0; i < Terminal::SCREEN_WIDTH - 2; i++) {
    std::cout << BOX_HORIZONTAL;
  }
  std::cout << BOX_TOP_RIGHT << std::endl;
  std::cout << VERTICAL_LINE << " ";
  std::cout << rang::fg::yellow << title;
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

void PreAmpleTheorem(std::string_view theorem) {
  std::cout << std::format("{:10}", theorem) << std::endl;
}
}