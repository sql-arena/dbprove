#include <dbprove/ux/ux.h>
#include <algorithm>
#include <fort.hpp>
#include <ostream>

#include "colour.h"
#include "glyphs.h"
#include "sort_orders.h"

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

std::string order_to_string(int8_t magnitude) {
  return std::to_string(1 << std::abs(magnitude)) + "x";
}

void EstimationStatTable(std::ostream& out, const std::vector<sql::explain::Plan::MisEstimation>& mis_estimations) {
  using namespace sql::explain;
  static const std::map<uint8_t, Colour> magnitude_colour = {
      {5, Colour::RED},
      {4, Colour::RED},
      {3, Colour::ORANGE},
      {2, Colour::YELLOW},
      {1, Colour::GREEN},
      {0, Colour::GREEN}
  };
  char_table table;
  table.set_border_style(FT_SOLID_ROUND_STYLE);
  // Calculate the header based on the unique operations we received
  table << header << "Magnitude";
  auto last_operation_type = Operation::UNKNOWN;
  for (const auto& e : mis_estimations) {
    if (e.operation != last_operation_type) {
      table << to_string(e.operation);
    }
    last_operation_type = e.operation;
  }
  table[0].set_cell_text_style(text_style::bold);

  // We want each magnitude to be its own line -> change sort order
  std::vector<Plan::MisEstimation> by_magnitude = mis_estimations;
  std::ranges::sort(by_magnitude,
                    [](const Plan::MisEstimation& a, const Plan::MisEstimation& b) {
                      if (a.magnitude.value != b.magnitude.value) {
                        return a.magnitude.value < b.magnitude.value;
                      }
                      return a.operation < b.operation;
                    });
  auto last_magnitude = Plan::MisEstimation::INFINITE_OVER;
  unsigned row_number = 0;
  for (auto& e : by_magnitude) {
    if (e.magnitude.value != last_magnitude) {
      table << endr;
      table << e.magnitude.to_string();
      const auto abs_magnitude = std::abs(e.magnitude.value);
      const auto row_colour = magnitude_colour.contains(abs_magnitude)
                                ? magnitude_colour.at(abs_magnitude)
                                : Colour::RED;
      ++row_number;
      table.row(row_number).set_cell_content_fg_color(mapFortColour(row_colour));
    }
    if (e.count > 0) {
      table << e.count;
    } else {
      table << "";
    }
    last_magnitude = e.magnitude.value;
  }
  table << endr;
  table.column(0).set_cell_text_align(text_align::right);
  table.column(1).set_cell_text_align(text_align::right);
  table.column(2).set_cell_text_align(text_align::right);
  table.column(3).set_cell_text_align(text_align::right);
  table.column(4).set_cell_text_align(text_align::right);

  out << table.to_string() << std::endl;
}
}