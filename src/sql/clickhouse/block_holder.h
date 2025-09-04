#pragma once

#include <clickhouse/client.h>

namespace ch = clickhouse;

namespace sql::clickhouse {
class BlockHolder {
public:
  explicit BlockHolder(std::vector<std::shared_ptr<ch::Block>> blocks)
    : blocks(std::move(blocks)) {
  }

  ~BlockHolder() = default;
  std::vector<std::shared_ptr<ch::Block>> blocks;
};
}