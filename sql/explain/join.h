#pragma once
#include "node.h"

namespace sql::explain {
class Join: public Node {
  enum class JoinType {
    HASH, LOOP, MERGE
  };
  const JoinType join_type_;
public:
  explicit Join(const JoinType join_type) : Node(NodeType::JOIN),  join_type_(join_type){}
};

}  // namespace sql::explain