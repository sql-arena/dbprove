# CityHash uses `byteswap.h` on MinGW Windows

The way `city.cc` file detects compiler, it fails to recognise a Windows system with MinGW. It
then falls back to `#include <byteswap.h>`. But, The header `byteswap.h` is Linux specific.

The patch works around this by providing a Windows compatible `byteswap.h` that satisfies
the requirement of `city.cc`.
