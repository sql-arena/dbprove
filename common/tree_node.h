#pragma once
#include <memory>
#include <queue>
#include <stack>
#include <vector>


class TreeNode;


struct TreeDepthIterable {
  struct Iterator {
    std::stack<TreeNode*> stack;
    const bool is_end = false;

    explicit Iterator(TreeNode& root) {
      stack.push(&root);
    }
    Iterator(): is_end(true) {
    }

    TreeNode& operator*() const { return *stack.top(); }

    Iterator& operator++();

    bool operator!=(const Iterator& other) const {
      if (is_end != other.is_end) {
        return true;
      }
      if (is_end) {
        return false;
      }
      return stack != other.stack;
    }
  };

  TreeNode& root;

  explicit TreeDepthIterable(TreeNode& r)
    : root(r) {
  }

  [[nodiscard]] Iterator begin() const { return Iterator{root}; }
  [[nodiscard]] Iterator end() const;
};

struct TreeBreathIterable {
  struct Iterator {
    std::queue<TreeNode*> queue;
    const bool is_end = false;
    Iterator(): is_end(true) {
    }

    explicit Iterator(TreeNode& root) {
      queue.push(&root);
    }

    TreeNode& operator*() const { return *queue.front(); }

    Iterator& operator++();

    bool operator!=(const Iterator& other) const {
      if (is_end != other.is_end) {
        return true;
      }
      if (is_end) {
        return false;
      }
      return queue != other.queue;
    }
  };

  TreeNode& root;

  explicit TreeBreathIterable(TreeNode& r)
    : root(r) {
  }

  [[nodiscard]] Iterator begin() const { return Iterator{root}; }
  [[nodiscard]] Iterator end() const;
};


class TreeNode {
  inline static std::atomic<uint64_t> currentId_{0};
  std::vector<std::unique_ptr<TreeNode>> children_;
  TreeNode* parent_;
  uint64_t id_;

  explicit TreeNode(bool _)
    : id_(std::numeric_limits<uint64_t>::max())
    , parent_(nullptr) {
  }

public:
  TreeNode()
    : id_(currentId_++)
    , parent_(this) {
  }

  void add_child(std::unique_ptr<TreeNode> child) {
    child->parent_ = this;
    children_.push_back(std::move(child));
  }

  bool remove_child(TreeNode* node) {
    for (auto it = children_.begin(); it != children_.end(); ++it) {
      if (it->get() == node) {
        children_.erase(it);
        return true;
      }
    }
    return false;
  }

  /// @brief Iterate depth first over the tree
  TreeDepthIterable depth_first() { return TreeDepthIterable{*this}; }
  /// @brief Iterate breath first over the tree
  TreeBreathIterable breadth_first() { return TreeBreathIterable{*this}; }

  /// @brief Direct parent of the tree
  [[nodiscard]] TreeNode& parent() const {
    return *parent_;
  }

  /// @brief Root of the current tree
  [[nodiscard]] TreeNode& root() const {
    auto p = parent_;
    while (p != parent_) {
      p = p->parent_;
    }
    return *p;
  }

  const std::span<TreeNode* const>& children() const {
    static thread_local std::vector<TreeNode*> raw_ptrs;
    raw_ptrs.clear();
    raw_ptrs.reserve(children_.size());

    for (const auto& child : children_) {
      raw_ptrs.push_back(child.get());
    }

    return std::span<TreeNode* const>(raw_ptrs);
  }

  uint64_t id() const { return id_; }

};

inline TreeDepthIterable::Iterator& TreeDepthIterable::Iterator::operator++() {
  TreeNode* current = stack.top();
  stack.pop();
  for (auto n : current->children()) {
    stack.push(n);
  }
  return *this;
}

inline TreeDepthIterable::Iterator TreeDepthIterable::end() const { return Iterator{}; }

inline TreeBreathIterable::Iterator& TreeBreathIterable::Iterator::operator++() {
  TreeNode* current = queue.front();
  queue.pop();
  for (auto n : current->children()) {
    queue.push(n);
  }
  return *this;
}

inline TreeBreathIterable::Iterator TreeBreathIterable::end() const { return Iterator{}; }