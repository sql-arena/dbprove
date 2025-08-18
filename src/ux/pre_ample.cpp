#include "pre_ample.h"
#include "glyphs.h"
#include <rang.hpp>
#include <iostream>

#include "boxes.h"

namespace dbprove::ux {
void PreAmple(std::ostream& out, const std::string_view title) {
  Box header;
  header.setText(title);
  header.setBorderStyle(BorderStyle::DOUBLE);
  header.setTextColour(Colour::BLUE);
  header.render(out);
}

void PreAmpleTheorem(std::ostream& out, std::string_view theorem) {
  Box header;
  header.setText(theorem);
  header.setBorderColour(Colour::GREY);
  header.setTextColour(Colour::WHITE);
  header.render(out);
}

void Line(std::ostream& out, const Distance width) {
  for (auto i = 0; i < width; ++i) {
    out << BOX_HORIZONTAL;
  }
  out << std::endl;
}

void Header(std::ostream& out, const std::string_view title, Distance min_width) {
  out << rang::style::bold << "  " << title << rang::style::reset << std::endl;
  Line(out, std::max(min_width, static_cast<Distance>(4 + title.size())));
}
}