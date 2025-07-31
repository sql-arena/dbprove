#include <exception>
#include "row_iterator.h"
#include "result_base.h"
#include "row_base.h"
#include <stdexcept>

namespace sql {
RowIterator::RowIterator(ResultBase& result)
  : result_(result)
  , currentRow_(&result_.nextRow()) {
}


const RowBase& RowIterator::operator*() const {
  if (currentRow_->isSentinel()) {
    throw std::runtime_error("Attempted to dereference past end of iterator");
  }
  return *currentRow_;
}

RowIterator& RowIterator::operator++() {
  currentRow_ = &result_.nextRow();
  return *this;
}

bool RowIterator::operator==(const RowIterator& other) const {
  return *currentRow_ == *other.currentRow_;
}

bool RowIterator::operator!=(const RowIterator& other) const {
  return !(*this == other);
}


ResultBase& RowIterable::getSentinel() {
  static SentinelResult sentinel_result;
  return sentinel_result;
}
}