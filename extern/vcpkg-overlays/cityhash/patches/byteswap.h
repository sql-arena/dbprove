// byteswap.h
#ifndef BYTESWAP_WINDOWS_H
#define BYTESWAP_WINDOWS_H

#ifdef _WIN32
#include <cstdint>

#if defined(_MSC_VER) // MSVC compiler
#include <stdlib.h>
#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__GNUC__) // MinGW or other GCC-compatible compiler on Windows
#define bswap_16(x) __builtin_bswap16(x)
#define bswap_32(x) __builtin_bswap32(x)
#define bswap_64(x) __builtin_bswap64(x)

#else // Fallback to manual implementation for other compilers

#include <stdlib.h>
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)
#endif

#endif // _WIN32
#endif // BYTESWAP_WINDOWS_H