#include <streambuf>
#include <ostream>

class NullBuffer final : public std::streambuf {
protected:
  int overflow(int c) override {
    return c; // Indicate success, but do nothing
  }
};

/**
* Stream to discard all data
*/
class NullStream final : public std::ostream {
public:
  NullStream()
    : std::ostream(&nullBuffer) {
  }

private:
  NullBuffer nullBuffer;
};
