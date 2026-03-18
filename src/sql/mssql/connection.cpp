#include "connection.h"

#include <regex>

#include "sql_exceptions.h"

#include <vincentlaucsb-csv-parser/internal/csv_reader.hpp>
// The SQL odbc library needs some strange INT definitions
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint>
#endif

#include <mutex>
#include <string>
#include <stdexcept>
#include <plog/Log.h>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif
#include <odbcinst.h>
#ifndef _WIN32
#include <dlfcn.h>
#ifndef BOOL
#define BOOL int
#endif
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif
#endif

namespace sql::mssql {

std::string getDriverPath(const std::string& driverName) {
#ifdef _WIN32
  return driverName;
#else
  char path[1024];
  // Note: SQLGetPrivateProfileString can be picky about the ini file name or if it's even needed.
  // The first argument is the section name (driver name).
  if (SQLGetPrivateProfileString(driverName.c_str(), "Driver", "", path, sizeof(path), "odbcinst.ini") > 0) {
    PLOGD << "Found driver path in odbcinst.ini: " << path;
    return std::string(path);
  }
  
  // If we couldn't get it via SQLGetPrivateProfileString, we can try to find it in common locations 
  // if the driverName matches what we expect.
  if (driverName == "ODBC Driver 18 for SQL Server") {
      if (std::filesystem::exists("/opt/homebrew/lib/libmsodbcsql.18.dylib")) return "/opt/homebrew/lib/libmsodbcsql.18.dylib";
      if (std::filesystem::exists("/usr/local/lib/libmsodbcsql.18.dylib")) return "/usr/local/lib/libmsodbcsql.18.dylib";
  } else if (driverName == "ODBC Driver 17 for SQL Server") {
      if (std::filesystem::exists("/opt/homebrew/lib/libmsodbcsql.17.dylib")) return "/opt/homebrew/lib/libmsodbcsql.17.dylib";
      if (std::filesystem::exists("/usr/local/lib/libmsodbcsql.17.dylib")) return "/usr/local/lib/libmsodbcsql.17.dylib";
  }

  return driverName;
#endif
}

static std::string cachedDriverName;
static std::string cachedDriverPath;
static std::once_flag driverSearchFlag;

std::string findBestDriver() {
  std::call_once(driverSearchFlag, []() {
    std::vector<char> drivers(4096);
    WORD len = 0;
    BOOL success = FALSE;

#ifdef _WIN32
    success = SQLGetInstalledDrivers(drivers.data(), drivers.size(), &len);
#else
    // On macOS, the linker might not always find SQLGetInstalledDrivers if libodbcinst isn't in the default path.
    // We try to call it directly first.
    success = SQLGetInstalledDrivers(drivers.data(), drivers.size(), &len);
    
    if (!success) {
      // Try to load it from common Homebrew/MacPorts/standard locations if direct call failed
      const char* paths[] = {
          "/opt/homebrew/lib/libodbcinst.dylib",
          "/usr/local/lib/libodbcinst.dylib",
          "libodbcinst.dylib"
      };

      for (const char* path : paths) {
          void* handle = dlopen(path, RTLD_LAZY);
          if (handle) {
              typedef BOOL (*SQLGetInstalledDriversType)(char*, WORD, WORD*);
              auto func = (SQLGetInstalledDriversType)dlsym(handle, "SQLGetInstalledDrivers");
              if (func) {
                  success = func(drivers.data(), drivers.size(), &len);
                  if (success) {
                      PLOGI << "Successfully called SQLGetInstalledDrivers from " << path;
                      dlclose(handle);
                      break;
                  }
              }
              dlclose(handle);
          }
      }
    }
#endif

    if (!success) {
      PLOGW << "SQLGetInstalledDrivers failed. Falling back to default: ODBC Driver 18 for SQL Server";
      cachedDriverName = "ODBC Driver 18 for SQL Server";
      cachedDriverPath = getDriverPath(cachedDriverName);
      return;
    }

    std::vector<std::string> driverList;
    char* p = drivers.data();
    while (*p) {
      std::string d(p);
      driverList.push_back(d);
      PLOGD << "Found ODBC driver: " << d;
      p += strlen(p) + 1;
      // Each driver entry is followed by its attributes (also null-terminated)
      while (*p) {
          p += strlen(p) + 1;
      }
      p++; // Skip the second null terminator of the attributes list
    }

    // Preferred drivers in order
    const std::vector<std::string> preferred = {
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13.1 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server Native Client 10.0",
        "SQL Server"
    };

    for (const auto& pref : preferred) {
      for (const auto& driver : driverList) {
        if (driver == pref) {
          PLOGI << "Using preferred SQL Server driver: " << driver;
          cachedDriverName = driver;
          cachedDriverPath = getDriverPath(cachedDriverName);
          return;
        }
      }
    }

    // If no exact match, look for anything that looks like a SQL Server driver
    for (const auto& driver : driverList) {
      if (driver.find("SQL Server") != std::string::npos) {
        PLOGI << "Using auto-discovered SQL Server driver: " << driver;
        cachedDriverName = driver;
        cachedDriverPath = getDriverPath(cachedDriverName);
        return;
      }
    }

    PLOGW << "No SQL Server ODBC driver found. Falling back to default: ODBC Driver 18 for SQL Server";
    cachedDriverName = "ODBC Driver 18 for SQL Server";
    cachedDriverPath = getDriverPath(cachedDriverName);
  });

  return cachedDriverName;
}

std::string makeConnectionString(const Credential& credential, std::optional<std::string> database_override) {
  if (!std::holds_alternative<CredentialPassword>(credential)) {
    throw NotImplementedException("Currently, only password credentials for SQL Server drivers");
  }
  const auto pwd = std::get<CredentialPassword>(credential);

  findBestDriver();
  PLOGD << "Using SQL Server driver: " << cachedDriverName << " (Path: " << cachedDriverPath << ")";

  std::string r = "DRIVER={" + cachedDriverPath + "};";
  r += "pooling=No;Encrypt=No;MultipleActiveResultSets=True;";
  r += "SERVER=127.0.0.1," + std::to_string(pwd.port) + ";";
  r += "DATABASE=" + database_override.value_or(pwd.database) + ";";
  r += "UID=" + pwd.username + ";";
  r += "PWD=" + pwd.password.value_or("") + ";";
  return r;
}

Connection::Connection(const Credential& credential, const Engine& engine, std::optional<std::string> artifacts_path)
  : odbc::Connection(credential, engine, makeConnectionString(credential, "master"), artifacts_path) {
  if (artifacts_path) {
    // In artifact mode we avoid touching the database during construction.
    return;
  }
  // We connected to master initially to ensure we can always get in.
  // Now we create the target database if it doesn't exist and switch to it.
  try {
    const auto pwd = std::get<CredentialPassword>(credential);
    const std::string db_name = pwd.database;
    
    // Create database if it doesn't exist
    execute("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'" + db_name + "') CREATE DATABASE [" + db_name + "]");
    
    // Switch to the target database
    execute("USE [" + db_name + "]");
    PLOGI << "Connected and switched to database: " << db_name;
  } catch (const std::exception& e) {
    PLOGW << "Failed to ensure target database exists and is in use: " << e.what();
  }
}


std::string Connection::version() {
  const auto versionString = fetchScalar("SELECT @@VERSION AS v").asString();
  // @@VERSION returns something like:
  // Microsoft SQL Server 2022 (RTM-CU16) (KB5044390) - 16.0.4165.4 (X64) ...
  // We want to extract the 16.0.4165.4 part.
  std::regex ver_regex(R"(- (\d+\.\d+\.\d+\.\d+))");
  std::smatch match;
  if (std::regex_search(versionString, match, ver_regex)) {
    return match[1];
  }
  return versionString;
}

/**
 * Translate from the generic ANSI syntax to SQL
 * @param sql
 * @return
 */
std::string translateSQL(std::string_view sql) {
  /* Limit is TOP
   * The translation here is (for now) simple. We can look for queries ending with LIMIT and put top next to SELECT
   * This is obviously error prone, but good enough for TPC-H
   * Ideally, we will want to parse the query
   */
  std::string query(sql);
  std::smatch match;
  std::regex limit_regex(R"(LIMIT\s+(\d+)\s*;?\s*$)", std::regex_constants::icase);

  if (std::regex_search(query, match, limit_regex)) {
    // Find and remove the LIMIT clause
    std::string limit_val = match[1];
    query = std::regex_replace(query, limit_regex, "");
    // Insert TOP x after SELECT
    std::regex select_regex(R"((SELECT\s+))", std::regex_constants::icase);
    query = std::regex_replace(query, select_regex, "$1TOP " + limit_val + " ",
                               std::regex_constants::format_first_only);
  }

  // Replace EXTRACT(YEAR FROM x) with YEAR(x)
  std::regex extract_year_regex(R"(EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+)\))", std::regex_constants::icase);
  query = std::regex_replace(query, extract_year_regex, "YEAR($1)");

  return query;
}

void Connection::execute(const std::string_view statement) {
  odbc::Connection::execute(translateSQL(statement));
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  return odbc::Connection::fetchAll(translateSQL(statement));
}

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT(53)"}, {"STRING", "VARCHAR"}};
  return map;
}

void Connection::analyse(const std::string_view table_name) {
  execute("UPDATE STATISTICS " + std::string(table_name) + " WITH FULLSCAN");
}
} // namespace sql::mssql
