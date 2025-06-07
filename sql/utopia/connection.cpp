#include "connection.h"
#include "result.h"
#include <chrono>
#include <thread>
#include <regex>
#include <map>

static std::map<std::string_view, std::vector<std::filesystem::path>> bulk_loader_tables = {};
static const std::regex comment_pattern("/\\*(.*?)\\*/");

void sleep_us(int microseconds) {
    std::this_thread::sleep_for(std::chrono::microseconds(microseconds));
}

const utopia::UtopiaConfig& getConfig(std::string_view statement) {
    using namespace utopia;
    std::match_results<std::string_view::const_iterator> matches;
    if (!std::regex_search(statement.begin(), statement.end(), matches, comment_pattern)) {
        throw std::runtime_error("Utopia did not find a comment in the SQL statement, cannot generate result");
    }
    /* Clean up to prepare for exact match */
    std::string comment_text = matches[1].str();
    comment_text.erase(0, comment_text.find_first_not_of(" \t\r\n"));
    comment_text.erase(comment_text.find_last_not_of(" \t\r\n") + 1);

    static const std::map<std::string_view, UtopiaConfig> data = {
        {"n10", {UtopiaData::N10, 0}},
        {"CLI-1", {UtopiaData::NOOP, 10}}
    };

    if (!data.contains(comment_text)) {
        throw std::runtime_error("Utopia cannot match requested result" + comment_text);
    }
    return data.at(comment_text);
}

utopia::Connection::Connection(const sql::Credential &credential): sql::ConnectionBase(credential) {
}

void utopia::Connection::execute(std::string_view statement) {
    const auto& config = getConfig(statement);
    sleep_us(config.runtime_us);
}

std::unique_ptr<sql::ResultBase> utopia::Connection::fetchAll(std::string_view statement) {
    const auto& c = getConfig(statement);
    sleep_us(c.runtime_us);
    return std::make_unique<Result>(c.data);
}

std::unique_ptr<sql::ResultBase> utopia::Connection::fetchMany(std::string_view statement) {
    const auto& c = getConfig(statement);
    sleep_us(c.runtime_us);
    return std::make_unique<Result>();
}

std::unique_ptr<sql::RowBase> utopia::Connection::fetchRow(const std::string_view statement) {
    const auto& c = getConfig(statement);
    sleep_us(c.runtime_us);
    return std::make_unique<Row>();
}

SqlVariant utopia::Connection::fetchValue(const std::string_view statement) {
    const auto& c = getConfig(statement);
    sleep_us(c.runtime_us);
    return SqlVariant(42);
}

void utopia::Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
  bulk_loader_tables[table] = source_paths;
}
