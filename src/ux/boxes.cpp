#include "boxes.h"
#include "dbprove/ux/ux.h"
#include "glyphs.h"
#include <rang.hpp>
#include <algorithm>
#include <vector>

namespace dbprove::ux {
namespace {
  rang::fg mapRangColour(const Colour c) {
    switch (c) {
      case Colour::RED: return rang::fg::red;
      case Colour::ORANGE: return rang::fg::red; // no orange in rang
      case Colour::YELLOW: return rang::fg::yellow;
      case Colour::GREEN: return rang::fg::green;
      case Colour::BLUE: return rang::fg::blue;
      case Colour::WHITE: return rang::fg::reset;
      case Colour::BLACK: return rang::fg::black;
      case Colour::GREY: return rang::fg::gray;
      case Colour::MAGENTA: return rang::fg::magenta;
      case Colour::CYAN: return rang::fg::cyan;
    }
    return rang::fg::reset;
  }

  struct EdgeGlyphs {
    const char* tl;
    const char* tr;
    const char* bl;
    const char* br;
    const char* h;
    const char* v;
  };

  EdgeGlyphs single() {
    return {BOX_TOP_LEFT, BOX_TOP_RIGHT, BOX_BOTTOM_LEFT, BOX_BOTTOM_RIGHT, BOX_HORIZONTAL, BOX_VERTICAL};
  }
  EdgeGlyphs dbl() {
    return {"╔", "╗", "╚", "╝", "═", "║"};
  }

  std::vector<std::string> splitLines(const std::string& text) {
    std::vector<std::string> lines;
    size_t start = 0;
    while (start <= text.size()) {
      const auto end = text.find('\n', start);
      auto line = (end == std::string::npos) ? text.substr(start) : text.substr(start, end - start);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      lines.push_back(std::move(line));
      if (end == std::string::npos) {
        break;
      }
      start = end + 1;
    }
    if (lines.empty()) {
      lines.emplace_back();
    }
    return lines;
  }
}

Box::Box(const Distance width, const Distance height)
  : width(width)
  , height(height) {
}

Box::Box(const Distance height)
  : Box(Terminal::width(), height) {
}

Box::Box()
  : Box(1) {
}

void Box::render(std::ostream& out) const {
  const auto g = (border_style == BorderStyle::DOUBLE) ? dbl() : single();
  const auto edge_col = mapRangColour(border_colour);
  const auto text_col = mapRangColour(text_colour);

  const Distance inner = width > 2 ? width - 2 : 0;

  // Top border
  out << edge_col << g.tl;
  if (inner > 0) {
    for (Distance i = 0; i < inner; ++i) out << g.h;
  } else if (width > 1) {
    // If width is 2, we just have tl and tr
  }
  if (width > 1) {
    out << g.tr;
  }
  out << rang::fg::reset << std::endl;

  // Content lines
  auto lines = splitLines(text);
  std::vector<std::string> display_lines;
  display_lines.reserve(lines.size());
  for (const auto& line : lines) {
    if (inner == 0) {
      display_lines.emplace_back();
      continue;
    }
    if (line.size() > static_cast<size_t>(inner)) {
      display_lines.push_back(line.substr(0, static_cast<size_t>(inner)));
    } else {
      display_lines.push_back(line);
    }
  }

  const auto content_rows = std::max<size_t>(height, display_lines.size());

  auto render_content_row = [&](const std::string_view row_text) {
    out << edge_col << g.v << rang::fg::reset; // left edge
    if (inner > 0) {
      const auto actual_text_len = row_text.size();
      const auto pad_total = (inner > actual_text_len) ? static_cast<size_t>(inner - actual_text_len) : 0;
      const auto pad_left = pad_total / 2;
      const auto pad_right = pad_total - pad_left;
      out << std::string(pad_left, ' ')
          << text_col << row_text << rang::fg::reset
          << std::string(pad_right, ' ');
    }
    if (width > 1) {
      out << edge_col << g.v << rang::fg::reset; // right edge
    }
    out << std::endl;
  };

  for (size_t i = 0; i < content_rows; ++i) {
    if (i < display_lines.size()) {
      render_content_row(display_lines[i]);
    } else {
      render_content_row("");
    }
  }

  // Bottom border
  out << edge_col << g.bl;
  if (inner > 0) {
    for (Distance i = 0; i < inner; ++i) out << g.h;
  }
  if (width > 1) {
    out << g.br;
  }
  out << rang::fg::reset << std::endl;
}
}
