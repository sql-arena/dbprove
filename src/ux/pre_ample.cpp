#include "pre_ample.h"
#include "glyphs.h"
#include "terminal.h"
#include <rang.hpp>
#include <iostream>

namespace dbprove::ux {
void PreAmple(std::ostream& out, const std::string_view title) {
  out << rang::fg::gray;
  out << BOX_TOP_LEFT;
  for (auto i = 0; i < Terminal::SCREEN_WIDTH - 2; i++) {
    out << BOX_HORIZONTAL;
  }
  out << BOX_TOP_RIGHT << std::endl;
  out << VERTICAL_LINE << " ";
  out << rang::fg::yellow << title;
  const auto padding = Terminal::SCREEN_WIDTH - 4 - title.length();
  out << std::string(padding, ' ');
  out << rang::fg::gray << " " << VERTICAL_LINE << std::endl;
  out << HASH_BUILD_CHILD;
  for (auto i = 0; i < Terminal::SCREEN_WIDTH - 2; i++) {
    out << BOX_HORIZONTAL;
  }
  out << BOX_BOTTOM_RIGHT;
  out << rang::fg::reset << std::endl;
}

void PreAmpleTheorem(std::ostream& out, std::string_view theorem) {
  out << std::format("{:10}", theorem) << std::endl;
}
}