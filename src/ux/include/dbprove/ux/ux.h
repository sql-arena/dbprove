#pragma once
#include "messages.h"
#include "pre_ample.h"
#include "spinner.h"
#include <dbprove/sql/explain/plan.h>
#include <vector>


namespace dbprove::ux {
struct RowStats {
  std::string name;
  size_t rows;
};


enum class Colour {
  RED, GREEN, ORANGE, BLUE, YELLOW, MAGENTA, CYAN, WHITE, BLACK, GREY
};

using Distance = uint16_t;
void RowStatTable(std::ostream& out, const std::vector<RowStats>& rows);

void EstimationStatTable(std::ostream& out, const std::vector<sql::explain::Plan::MisEstimation>& mis_estimations);

std::string PrettyRows(size_t rows);

void Header(std::ostream& out, std::string_view title, Distance min_width = 0);

class Terminal {
public:
  static void configure();
  static Distance width();
};
}