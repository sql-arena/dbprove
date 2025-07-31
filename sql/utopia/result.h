#pragma once
#include <result_base.h>
#include <cstddef>

#include "row.h"
#include "row_base.h"
#include "utopia_data.h"

namespace sql::utopia {

struct UtopiaConfig {
  utopia::
  UtopiaData data;
  uint32_t runtime_us;
};


class Result final : public ResultBase {
public:
  explicit Result(const UtopiaData data = UtopiaData::EMPTY)
    : data(data) {
  };

  ~Result() override {
  };

  size_t rowCount() const override;
  size_t columnCount() const override;

protected:
  void reset() override {
  };

  const RowBase& nextRow() override;

private:
  const UtopiaData data;
  size_t rowNumber = 0;
  Row currentRow_{};
};
}