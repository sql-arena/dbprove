#include <duckdb.hpp>

#include <iostream>
#include <string>

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: duckdb_csv_repro <csv-path>\n";
    return 2;
  }

  try {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query(
        "CREATE TABLE t("
        "id INTEGER, "
        "name VARCHAR, "
        "imdb_index VARCHAR, "
        "imdb_id INTEGER, "
        "gender VARCHAR, "
        "name_pcode_cf VARCHAR, "
        "name_pcode_nf VARCHAR, "
        "surname_pcode VARCHAR, "
        "md5sum VARCHAR)");

    const std::string sql =
        std::string("COPY t FROM '") + argv[1]
        + "' WITH (FORMAT 'csv', DELIM '|', AUTO_DETECT true, HEADER true, STRICT_MODE false)";
    auto result = con.Query(sql);
    if (result->HasError()) {
      std::cerr << result->GetError() << "\n";
      return 1;
    }

    auto count_result = con.Query("SELECT COUNT(*) FROM t");
    if (count_result->HasError()) {
      std::cerr << count_result->GetError() << "\n";
      return 1;
    }

    auto row = count_result->Fetch();
    std::cout << row->GetValue(0, 0).ToString() << "\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
