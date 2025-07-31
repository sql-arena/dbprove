#pragma once
#include <cassert>
#include <memory>
#include <queue>
#include <stack>
#include <vector>
#include <span>

template <typename T>
struct TreeDepthIterable {
  struct Iterator {
    std::stack<T*> stack;
    const bool is_end = false;

    explicit Iterator(T& root) {
      stack.push(&root);
    }
    Iterator(): is_end(true) {
    }

    T& operator*() const {
      assert(stack.size() > 0);
      return *stack.top();
    }

    Iterator& operator++() {
      if (!stack.empty()) {
        T* current = stack.top();
        stack.pop();
        // We want to visit children in such a way the first child (leftmost) child
        // is delved into first
        auto children = current->children();
        for (auto it = children.rbegin(); it != children.rend(); ++it) {
          stack.push(*it);
        }

      }
      return *this;
    };

    bool operator!=(const Iterator& other) const {
      if (is_end) { return !other.is_end || !other.stack.empty();}
      if (other.is_end) {return !stack.empty();}
      return stack != other.stack;

    }
  };

  T& root;

  explicit TreeDepthIterable(T& r)
    : root(r) {
  }

  [[nodiscard]] Iterator begin() const { return Iterator{root}; }
  [[nodiscard]] Iterator end() const { return Iterator{};};
};

template <typename T>
struct TreeBreathIterable {
  struct Iterator {
    std::queue<T*> queue;
    const bool is_end = false;
    Iterator(): is_end(true) {
    }

    explicit Iterator(T& root) {
      queue.push(&root);
    }

    T& operator*() const { return *queue.front(); }

    Iterator& operator++() {
      T* current = queue.front();
      queue.pop();
      for (auto n : current->children()) {
        queue.push(n);
      }
      return *this;

    };

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

  T& root;

  explicit TreeBreathIterable(T& r)
    : root(r) {
  }

  [[nodiscard]] Iterator begin() const { return Iterator{root}; }
  [[nodiscard]] Iterator end() const { return Iterator{};};
};


template<typename T>
class TreeNode {
  inline static std::atomic<uint64_t> currentId_{0};
  std::vector<std::unique_ptr<T>> children_;
  T* parent_;
  uint64_t id_;


  explicit TreeNode(bool _)
    : parent_(nullptr)
    , id_(std::numeric_limits<uint64_t>::max()) {

  }

public:
  TreeNode()
    : parent_(static_cast<T*>(this))
    , id_(currentId_++) {
  }

  void addChild(std::unique_ptr<T> child) {
    child->parent_ = static_cast<T*>(this);
    children_.push_back(std::move(child));
  }

  bool removeChild(T* node) {
    for (auto it = children_.begin(); it != children_.end(); ++it) {
      if (it->get() == node) {
        children_.erase(it);
        return true;
      }
    }
    return false;
  }

  /// @brief Iterate depth first over the tree
  TreeDepthIterable<T> depth_first() { return TreeDepthIterable{static_cast<T&>(*this)}; }
  /// @brief Iterate breath first over the tree
  TreeBreathIterable<T> breadth_first() { return TreeBreathIterable{{static_cast<T&>(*this)}}; }

  /// @brief Is this the root node?
  bool isRoot() const { return parent_ == this; }

  /// @brief How deep in the tree is this?
  size_t depth() const {
    size_t d = 0;
    auto p = this;
    while (!p->isRoot()) {
      ++d;
      p = p->parent_;
    }
    return d;

  }

  /// @brief Direct parent of the tree
  [[nodiscard]] T& parent() const {
    return *parent_;
  }

  /// @brief Root of the current tree. Find by iterating upward
  /// @note The root node has itself as parent
  [[nodiscard]] T& root() const {
    auto p = this;
    while (p != parent_) {
      p = p->parent_;
    }
    return *p;
  }

  std::span<T* const> children() const {
    static thread_local std::vector<T*> raw_ptrs;
    raw_ptrs.clear();
    raw_ptrs.reserve(children_.size());

    for (const auto& child : children_) {
      raw_ptrs.push_back(static_cast<T*>(child.get()));
    }

    return std::span<T* const>(raw_ptrs);
  }

  uint64_t id() const { return id_; }

  size_t childCount() const { return children_.size(); }

  const T* firstChild() {
    if (children_.empty()) {
      return nullptr;
    }
    return children_.front().get();
  }
  const T* lastChild() {
    if (children_.empty()) {
      return nullptr;
    }
    return children_.back().get();
  }
  /// @brief is the child here
  bool hasChild(const T* child) {
    if (children_.empty()) {
      return false;
    }
    for (const auto& c : children_) {
      if (c.get() == child) {
        return true;
      }
    }
    return false;
  }

};



