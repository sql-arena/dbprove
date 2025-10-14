#include <iostream>

#include "connection.h"
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <msodbcsql.h>
#include <absl/numeric/int128.h>

#include "sql_exceptions.h"
#include <plog/Log.h>
#include <vincentlaucsb-csv-parser/csv.hpp>

/**
 * NOTE:
 * Microsoft doesn't ship the linkable library that allows us to speak BCP to the interface
 * They DO ship an old library that has the right signature, but that library does not correctly implement `odbcss.h`
 * So, you can end up (as I did) spending a lot of time debugging what the library wont talk to you
 *
 * The real method appears to be dynamically loading the ODBC BCP interface
 */
typedef RETCODE (WINAPI*BCP_INIT_A)(HDBC, LPCSTR, LPCSTR, LPCSTR, INT);
typedef RETCODE (WINAPI*BCP_BIND)(HDBC, LPCBYTE, DBINT, DBINT, LPCBYTE, INT, INT, INT);
typedef RETCODE (WINAPI*BCP_SENDROW)(HDBC);
typedef RETCODE (WINAPI*BCP_COLLEN)(HDBC, DBINT, INT);
typedef DBINT (WINAPI*BCP_DONE)(HDBC);
typedef DBINT (WINAPI*BCP_BATCH)(HDBC);

using uint128_t = absl::uint128;

struct BCPAPI {
  // Function pointers
  BCP_INIT_A bcp_initA = nullptr;
  BCP_BIND bcp_bind = nullptr;
  BCP_SENDROW bcp_sendrow = nullptr;
  BCP_DONE bcp_done = nullptr;
  BCP_BATCH bcp_batch = nullptr;
  BCP_COLLEN bcp_collen = nullptr;

private:
  HMODULE dll = nullptr;
  static std::once_flag load_once;
  static BCPAPI instance;

public:
  static const BCPAPI& get() {
    std::call_once(load_once, []() { instance.load(); });
    return instance;
  }

private:
  void load() {
    const std::vector<std::string> candidates = {"msodbcsql18.dll",
                                                 "msodbcsql17.dll",
                                                 "msodbcsql.dll" // fallback
    };

    for (const auto& name : candidates) {
      HMODULE h = LoadLibraryA(name.c_str());
      if (!h)
        continue;

      auto resolve = [&](const char* sym) -> FARPROC {
        return GetProcAddress(h, sym);
      };

      const auto initA = reinterpret_cast<BCP_INIT_A>(resolve("bcp_initA"));
      const auto bind = reinterpret_cast<BCP_BIND>(resolve("bcp_bind"));
      const auto send = reinterpret_cast<BCP_SENDROW>(resolve("bcp_sendrow"));
      const auto done = reinterpret_cast<BCP_DONE>(resolve("bcp_done"));
      const auto batch = reinterpret_cast<BCP_BATCH>(resolve("bcp_batch"));
      const auto collen = reinterpret_cast<BCP_COLLEN>(resolve("bcp_collen"));

      if (initA && bind && send && done && batch && collen) {
        dll = h;
        bcp_initA = initA;
        bcp_bind = bind;
        bcp_sendrow = send;
        bcp_done = done;
        bcp_batch = batch;
        bcp_collen = collen;
        PLOGI << "Loaded BCP functions from: '" << name << "'";
        return;
      } else {
        FreeLibrary(h);
      }
    }

    throw std::runtime_error(
        "Failed to load a compatible msodbcsqlXX.dll for BCP API. "
        "You likely need to install the Microsoft ODBC library");
  }

  // Prevent external construction
  BCPAPI() = default;

  ~BCPAPI() {
    if (dll)
      FreeLibrary(dll);
  }

  BCPAPI(const BCPAPI&) = delete;
  BCPAPI& operator=(const BCPAPI&) = delete;
};

std::once_flag BCPAPI::load_once;
BCPAPI BCPAPI::instance;


SQL_DATE_STRUCT parseDate(const std::string& date) {
  SQL_DATE_STRUCT dbdate{};
  if (sscanf(date.c_str(), "%hd-%hu-%hu", &dbdate.year, &dbdate.month, &dbdate.day) != 3) {
    throw std::invalid_argument("Invalid date format, expected YYYY-MM-DD");
  }
  return dbdate;
}

DBDATETIME parseDateTime(const std::string& s) {
  int y, hour, minute, second;
  unsigned int m, d;
  if (std::sscanf(s.c_str(), "%d-%d-%d %d:%d:%d", &y, &m, &d, &hour, &minute, &second) != 6) {
    throw std::invalid_argument("Invalid datetime format, expected YYYY-MM-DD HH:MM:SS");
  }

  using namespace std::chrono;
  // Validate time-of-day bounds early
  if (m < 1 || m > 12 || d < 1 || d > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second >
      59) {
    throw std::invalid_argument("Out-of-range date/time component");
  }

  // Convert to chrono civil date; this validates (e.g., 2025-02-30 throws)
  year_month_day ymd = year{y} / month{m} / day{d};
  if (!ymd.ok())
    throw std::invalid_argument("Invalid calendar date");

  // DB-Library DATETIME: days since 1900-01-01 and time in 1/300 sec ticks.
  constexpr year_month_day base{1900y / January / 1};

  auto d_value = sys_days{ymd};
  auto d_base = sys_days{base};
  const auto days_since_base = (d_value - d_base).count(); // can be negative for <1900-01-01

  // 1 day = 24*3600 seconds; dttime counts 1/300s since midnight
  const long seconds_since_midnight = hour * 3600L + minute * 60L + second;
  const unsigned long ticks = static_cast<unsigned long>(seconds_since_midnight) * 300UL;

  // Defensive check (max within a day)
  // 24*3600*300 = 25,920,000
  if (ticks >= 24UL * 3600UL * 300UL) {
    throw std::invalid_argument("Time past end-of-day");
  }

  DBDATETIME out{};
  out.dtdays = static_cast<long>(days_since_base);
  out.dttime = ticks;
  return out;
}


SQL_NUMERIC_STRUCT parseDecimal(const std::string& str, DBINT* length) {
  SQL_NUMERIC_STRUCT num{};

  // Parse the string
  std::string s = str;

  // Handle sign
  num.sign = 1; // positive by default
  if (s[0] == '-') {
    num.sign = 0;
    s = s.substr(1);
  }
  // Find decimal point
  size_t dot_pos = s.find('.');
  std::string integer_part;
  std::string fractional_part;

  if (dot_pos != std::string::npos) {
    integer_part = s.substr(0, dot_pos);
    fractional_part = s.substr(dot_pos + 1);
  } else {
    integer_part = s;
    fractional_part = "";
  }

  // Remove leading zeros from integer part
  const size_t first_nonzero = integer_part.find_first_not_of('0');
  if (first_nonzero != std::string::npos) {
    integer_part = integer_part.substr(first_nonzero);
  } else {
    integer_part = "0";
  }

  num.scale = static_cast<SQLSCHAR>(fractional_part.length());
  const std::string all_digits = integer_part + fractional_part;
  num.precision = static_cast<SQLCHAR>(all_digits.length());

  // Convert string digits to little-endian byte array
  // SQL Server stores numeric values as 128-bit integers in little-endian format
  std::memset(num.val, 0, SQL_MAX_NUMERIC_LEN);

  uint128_t value = 0;

  for (auto it = all_digits.rbegin(); it != all_digits.rend(); ++it) {
    if (*it < '0' || *it > '9') {
      throw std::invalid_argument("Invalid decimal format: contains non-digit character");
    }
    value = value * 10 + (*it - '0');
  }
  // TODO: This is currently broken, fix
  // Store as little-endian byte array
  for (int i = 0; i < SQL_MAX_NUMERIC_LEN; ++i) {
    num.val[i] = static_cast<uint8_t>(value & 0xFF);
    value >>= 8;
  }

  // Calculate the length (number of significant bytes)
  int length_bytes = SQL_MAX_NUMERIC_LEN;
  while (length_bytes > 0 && num.val[length_bytes - 1] == 0) {
    --length_bytes;
  }
  if (length) {
    *length = length_bytes;
  }
  return num;
}

namespace sql {
void handleBulkReturn(const int return_code, const SQLHDBC hdbc, const int expected = 0) {
  if (return_code == expected) {
    return;
  }
  SQLCHAR sqlstate[6] = {0};
  SQLINTEGER native = 0;
  SQLCHAR msg[1024] = {0};
  SQLSMALLINT msglen = 0;
  if (SQLGetDiagRecA(SQL_HANDLE_DBC, hdbc, 1, sqlstate, &native, msg, sizeof(msg), &msglen) == SQL_SUCCESS) {
    throw BulkException(std::string(reinterpret_cast<const char*>(msg)));
  }
}


void msodbc::Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  const BCPAPI& bcp = BCPAPI::get();
  auto layout = fetchAll("SELECT * FROM " + std::string(table) + " WHERE 1=0");
  const size_t column_count = layout->columnCount();
  /**
   * NOTE: The BCP interface to SQL Server is truly horrible.
   *
   * It is not possible to switch over an existing connection to bulk mode.
   * You have to create a new one and follow a very specific sequence of events to do so
   *
   */
  SQLHENV henv = nullptr;
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
  SQLHDBC hdbc = nullptr;
  SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

  SQLSetConnectAttr(hdbc, SQL_COPT_SS_BCP, reinterpret_cast<SQLPOINTER>(SQL_BCP_ON), SQL_IS_INTEGER);

  handleBulkReturn(SQLDriverConnectA(hdbc, nullptr, reinterpret_cast<SQLCHAR*>(const_cast<char*>((connectionString()))),
                                     SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT), hdbc, SUCCEED);

  SQLUINTEGER bcp_enabled = 0;
  SQLGetConnectAttr(hdbc, SQL_COPT_SS_BCP, &bcp_enabled, 0, nullptr);
  handleBulkReturn(bcp.bcp_initA(hdbc, table.data(), nullptr, nullptr, DB_IN), hdbc, -42);

  constexpr size_t MAX_ROW_SIZE = 64 * 1024;
  auto buffer = std::make_unique<std::byte[]>(MAX_ROW_SIZE);
  constexpr size_t BATCH_SIZE = 10000;

  struct BcpType {
    int key;
    size_t size;
  };
  static const std::map<SqlTypeKind, BcpType> bcp_types = {{SqlTypeKind::SMALLINT, {SQLINT2, sizeof(short)}},
                                                           {SqlTypeKind::INT, {SQLINT4, sizeof(int32_t)}},
                                                           {SqlTypeKind::BIGINT, {SQLINT8, sizeof(int64_t)}},
                                                           {SqlTypeKind::REAL, {SQLFLT4, sizeof(float)}},
                                                           {SqlTypeKind::DOUBLE, {SQLFLT8, sizeof(double)}},
                                                           {SqlTypeKind::DECIMAL,
                                                            {SQLNUMERICN, sizeof(SQL_NUMERIC_STRUCT)}},
                                                           {SqlTypeKind::STRING, {SQLCHARACTER, 0}},
                                                           {SqlTypeKind::DATE, {SQLDATEN, sizeof(SQL_DATE_STRUCT)}},
                                                           {SqlTypeKind::TIME, {SQLTIMEN, sizeof(SQL_SS_TIME2_STRUCT)}},
                                                           {SqlTypeKind::DATETIME, {SQLDATETIME, sizeof(DBDATETIME)}}};

  /* First, we have to bind a buffer to the BCP input stream
   * Note that in ODBC, column indexes are 1-based
   *
   */
  size_t column_index = 0;
  auto column_types = describeColumnTypes(table);
  std::vector<std::array<std::byte, MAX_ROW_SIZE>> buffers(column_count);
  for (auto meta_type : column_types) {
    RETCODE rc = 0;
    auto [key, size] = bcp_types.at(meta_type.kind);
    auto r = bcp.bcp_bind(hdbc, reinterpret_cast<LPCBYTE>(buffers[column_index].data()), 0, 0, nullptr, 0, key,
                          column_index + 1);
    handleBulkReturn(r, hdbc, SUCCEED);
    if (meta_type.kind == SqlTypeKind::STRING) {
      size = meta_type.length();
    }
    ++column_index;
  }

  const auto fmt = csv::CSVFormat().delimiter('|').quote('"').header_row(0);
  size_t row_count = 0;
  for (const auto& path : source_paths) {
    csv::CSVReader reader(path.string(), fmt);
    for (csv::CSVRow& row : reader) {
      for (int i = 0; i < column_count; ++i) {
        std::byte* buffer = buffers[i].data();
        // const auto row_val = row[i].get<std::string>();
        switch (column_types[i].kind) {
          case SqlTypeKind::SMALLINT: {
            const auto v = row[i].get<int16_t>();
            std::memcpy(buffer, &v, sizeof(int16_t));
            break;
          }
          case SqlTypeKind::INT: {
            const auto v = row[i].get<int32_t>();
            std::memcpy(buffer, &v, sizeof(int32_t));
            break;
          }
          case SqlTypeKind::BIGINT: {
            const auto v = row[i].get<int64_t>();
            std::memcpy(buffer, &v, sizeof(int64_t));
            break;
          }
          case SqlTypeKind::REAL: {
            const auto v = row[i].get<float>();
            std::memcpy(buffer, &v, sizeof(float));
            break;
          }
          case SqlTypeKind::DOUBLE: {
            const auto v = row[i].get<double>();
            std::memcpy(buffer, &v, sizeof(double));
            break;
          }
          case SqlTypeKind::DECIMAL: {
            DBINT length = 0;
            const auto v = parseDecimal(row[i].get<std::string>(), &length);
            std::memcpy(buffer, &v, sizeof(v));
            bcp.bcp_collen(hdbc, length, i + 1);
            break;
          }
          case SqlTypeKind::STRING:
          case SqlTypeKind::TIME: {
            const auto s = row[i].get<std::string>();
            std::memcpy(buffer, s.data(), s.size());
            bcp.bcp_collen(hdbc, static_cast<DBINT>(s.size()), i + 1);
            break;
          }
          case SqlTypeKind::DATE: {
            const auto v = parseDate(row[i].get<std::string>());
            std::memcpy(buffer, &v, sizeof(SQL_DATE_STRUCT));
            break;
          }
          case SqlTypeKind::DATETIME: {
            const auto v = parseDateTime(row[i].get<std::string>());
            std::memcpy(buffer, &v, sizeof(DBDATETIME));
            break;
          }
          default:
            throw std::runtime_error("Unsupported type for BCP bulk load");
        }
      }
      handleBulkReturn(bcp.bcp_sendrow(hdbc), hdbc, SUCCEED);

      if (++row_count % BATCH_SIZE == 0) {
        handleBulkReturn(bcp.bcp_batch(hdbc), hdbc);
      }
    }
  }
  handleBulkReturn(bcp.bcp_done(hdbc), hdbc);
  SQLDisconnect(hdbc);
  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
}
}