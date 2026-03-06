#include <iostream>
#include <vector>
#include <filesystem>
#include <fstream>
#include <dbprove/sql/engine.h>
#include <dbprove/sql/connection_factory.h>

int main() {
    try {
        sql::Engine engine("mssql");
        auto credential = engine.parseCredentials("127.0.0.1", 1433, "dbprove", "sa", "YourStrong!Passw0rd", std::nullopt);
        sql::ConnectionFactory factory(engine, credential);
        auto conn = factory.create();

        std::cout << "Connected to: " << conn->version() << std::endl;

        // Create a test table
        conn->execute("IF OBJECT_ID('test_bulk', 'U') IS NOT NULL DROP TABLE test_bulk");
        conn->execute("CREATE TABLE test_bulk (id INT)");

        // Create a small CSV file
        std::filesystem::path test_file = "test_bulk.csv";
        std::ofstream ofs(test_file);
        // We set header_row(-1) which means no header, so just provide data.
        ofs << "1\n";
        ofs << "2\n";
        ofs << "3\n";
        ofs.close();

        std::cout << "Starting bulk load of " << test_file << "..." << std::endl;
        conn->bulkLoad("test_bulk", {test_file});
        std::cout << "Bulk load completed!" << std::endl;

        // Verify data
        auto result = conn->fetchAll("SELECT COUNT(*) FROM test_bulk");
        for (const auto& row : result->rows()) {
            std::cout << "Rows in test_bulk: " << row.asString(0) << std::endl;
        }

        // Cleanup
        std::filesystem::remove(test_file);
        conn->execute("DROP TABLE test_bulk");

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
