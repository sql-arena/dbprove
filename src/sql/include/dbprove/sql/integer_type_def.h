#include <cstddef>
#include <functional>
#include <iostream>
#include <type_traits>
#include "sql_type.h"

/// @brief A simple strong typedef base with integer arithmetic
template <typename T, typename Tag>
class IntegerTypedef {
    static_assert(std::is_integral_v<T>, "IntegerTypedef requires an integral type");

private:
    T value_;

public:
    using underlying_type = T;
    
    constexpr explicit IntegerTypedef(T value = T{}) noexcept : value_(value) {}
    
    constexpr T& get() noexcept { return value_; }
    constexpr const T& get() const noexcept { return value_; }
    
    // Explicit conversion back to underlying type
    constexpr explicit operator T() const noexcept { return value_; }
    
    // Comparison operators
    friend constexpr bool operator==(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ == rhs.value_;
    }
    
    friend constexpr bool operator!=(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ != rhs.value_;
    }
    
    friend constexpr bool operator<(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ < rhs.value_;
    }
    
    friend constexpr bool operator<=(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ <= rhs.value_;
    }
    
    friend constexpr bool operator>(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ > rhs.value_;
    }
    
    friend constexpr bool operator>=(const IntegerTypedef& lhs, const IntegerTypedef& rhs) noexcept {
        return lhs.value_ >= rhs.value_;
    }
    
    // Arithmetic operators
    constexpr IntegerTypedef& operator+=(const IntegerTypedef& rhs) noexcept {
        value_ += rhs.value_;
        return *this;
    }
    
    friend constexpr IntegerTypedef operator+(IntegerTypedef lhs, const IntegerTypedef& rhs) noexcept {
        lhs += rhs;
        return lhs;
    }
    
    constexpr IntegerTypedef& operator-=(const IntegerTypedef& rhs) noexcept {
        value_ -= rhs.value_;
        return *this;
    }
    
    friend constexpr IntegerTypedef operator-(IntegerTypedef lhs, const IntegerTypedef& rhs) noexcept {
        lhs -= rhs;
        return lhs;
    }
    
    constexpr IntegerTypedef& operator*=(const IntegerTypedef& rhs) noexcept {
        value_ *= rhs.value_;
        return *this;
    }
    
    friend constexpr IntegerTypedef operator*(IntegerTypedef lhs, const IntegerTypedef& rhs) noexcept {
        lhs *= rhs;
        return lhs;
    }
    
    constexpr IntegerTypedef& operator/=(const IntegerTypedef& rhs) noexcept {
        value_ /= rhs.value_;
        return *this;
    }
    
    friend constexpr IntegerTypedef operator/(IntegerTypedef lhs, const IntegerTypedef& rhs) noexcept {
        lhs /= rhs;
        return lhs;
    }
    
    constexpr IntegerTypedef& operator%=(const IntegerTypedef& rhs) noexcept {
        value_ %= rhs.value_;
        return *this;
    }
    
    friend constexpr IntegerTypedef operator%(IntegerTypedef lhs, const IntegerTypedef& rhs) noexcept {
        lhs %= rhs;
        return lhs;
    }
    
    // Increment/decrement
    constexpr IntegerTypedef& operator++() noexcept {
        ++value_;
        return *this;
    }
    
    constexpr IntegerTypedef operator++(int) noexcept {
        IntegerTypedef temp(*this);
        ++(*this);
        return temp;
    }
    
    constexpr IntegerTypedef& operator--() noexcept {
        --value_;
        return *this;
    }
    
    constexpr IntegerTypedef operator--(int) noexcept {
        IntegerTypedef temp(*this);
        --(*this);
        return temp;
    }
    
    friend std::ostream& operator<<(std::ostream& os, const IntegerTypedef& obj) {
        return os << obj.value_;
    }

    SQLInt toSQLInt() const { return SQLInt{static_cast<int>(value_)};}
    SQLBigInt toSQLBigInt() const { return SQLBigInt{static_cast<size_t>(value_)};}
};


// Hash support for std::unordered_map, etc.
namespace std {
    template <typename T, typename Tag>
    struct hash<IntegerTypedef<T, Tag>> {
        std::size_t operator()(const IntegerTypedef<T, Tag>& obj) const noexcept {
            return hash<T>()(static_cast<T>(obj));
        }
    };
}