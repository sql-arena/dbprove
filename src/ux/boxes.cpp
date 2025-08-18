#include "boxes.h"
#include "dbprove/ux/ux.h"

namespace dbprove::ux {
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
  char_table table;
  switch (border_style) {
    case BorderStyle::SINGLE:
      table.set_border_style(FT_SOLID_STYLE);
      break;
    case BorderStyle::DOUBLE:
      table.set_border_style(FT_DOUBLE_STYLE);
  }
  table << header << text << endr;
  table.set_cell_min_width(width - 2);
  table.row(0).set_cell_bottom_padding(height - 1);
  table.row(0).set_cell_content_fg_color(mapFortColour(text_colour));
  out << table.to_string() << std::endl;
}
}