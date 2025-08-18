#include <dbprove/ux/ux.h>
#include <fort.hpp>
#include <ostream>

namespace dbprove::ux {
using namespace fort;

void RowStatTable(std::ostream& out, const std::vector<RowStats>& rows) {
  char_table table;

  table.set_border_style(FT_SOLID_ROUND_STYLE);

  table << header << "Operation" << "Rows" << endr;
  for (const auto& [name, rows] : rows) {
    table << name << rows << endr;
  }
  table[0].set_cell_text_style(text_style::bold);

  table.column(1).set_cell_text_align(text_align::right);

  out << table.to_string() << std::endl;
}
}