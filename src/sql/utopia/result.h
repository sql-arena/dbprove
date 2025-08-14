#pragma once
#include "row.h"
#include "utopia_data.h"
#include <dbprove/sql/sql.h>
#include <cstddef>


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