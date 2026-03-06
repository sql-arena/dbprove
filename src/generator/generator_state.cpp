#include "generator_state.h"

#include <fstream>

#include "generated_table.h"
#include "dbprove/sql/connection_factory.h"
#include <dbprove/sql/sql.h>
#include <plog/Log.h>
#include <google/cloud/storage/client.h>
#include <zip.h>

#include "dbprove/common/file_utility.h"
#include "dbprove/generator/test.h"

#include <dbprove/ux/ux.h>

namespace generator
{
    std::map<std::string_view, GeneratedTable*>&
    available_tables()
    {
        static std::map<std::string_view, GeneratedTable*> registry;
        return registry;
    }

    std::map<std::string_view, std::vector<std::string_view>>&
    available_datasets()
    {
        static std::map<std::string_view, std::vector<std::string_view>> registry;
        return registry;
    }

    void GeneratorState::prepareFileInput(const std::string_view schemaName,
        const std::string_view tableName,
        const std::string_view relativePath)
    {
        const auto fullTablename = std::format("{}.{}", schemaName, tableName);
        const auto final_csv_path = csvPath(tableName);

        if (std::filesystem::exists(final_csv_path) && std::filesystem::file_size(final_csv_path) > 0) {
            PLOGD << "Table: " << fullTablename << " CSV already exists. Skipping download and unzip.";
            registerGeneration(fullTablename, final_csv_path);
            return;
        }

        if (!engine_.needsLocalFile()) {
            PLOGD << "Engine does not need local files. Registering " << fullTablename << " as ready.";
            // We use an empty path since there is no local file, but the table is "generated" 
            // (i.e. its prepare stage is done)
            registerGeneration(fullTablename, ""); 
            return;
        }

        if (cloudProvider() != CloudProvider::GCS) {
            throw std::runtime_error("prepareFileInput currently supports only Google Cloud Storage (GCS) for local downloads");
        }

        dbprove::common::make_directory(basePath_);

        const std::string bucket = "sql-arena-data";
        std::string object = std::format("{}/{}{}.csv.zip",
             schemaName, relativePath, tableName);;
        const auto base_dir = basePath();
        const auto downloadPath = base_dir / std::format("{}.csv.zip", tableName);
        const auto zipCSVFileName = std::format("{}.csv", tableName);

        // Check if the file exists and is a valid zip archive. 
        // If not, we should delete it so we can re-download.
        if (std::filesystem::exists(downloadPath)) {
            int err = 0;
            zip_t* za = zip_open(downloadPath.string().c_str(), ZIP_RDONLY, &err);
            if (za) {
                zip_close(za);
            } else {
                PLOGW << "Existing zip archive " << downloadPath.string() << " is corrupted or invalid. Deleting and re-downloading...";
                std::filesystem::remove(downloadPath);
            }
        }

        if (!std::filesystem::exists(downloadPath)) {
            PLOGI << "Downloading GCS object " << object << " to " << downloadPath.string();
            namespace gcs = ::google::cloud::storage;
            auto client = gcs::Client::CreateDefaultClient().value();
            std::ofstream ofs(downloadPath, std::ios::binary);
            if (!ofs.is_open()) {
                throw std::runtime_error("Failed to open local zip file for writing: " + downloadPath.string());
            }
            auto reader = client.ReadObject(bucket, object);
            if (!reader.status().ok()) {
                // If it fails, remove the empty/partial file so we don't try to unzip it next time
                ofs.close();
                std::filesystem::remove(downloadPath);
                throw std::runtime_error("GCS ReadObject failed: " + reader.status().message());
            }

            ofs << reader.rdbuf();

            // Check for errors after the loop
            if (reader.bad() || !ofs.good()) {
                ofs.close();
                std::filesystem::remove(downloadPath);
                throw std::runtime_error("GCS ReadObject or file write failed during download: " + reader.status().message());
            }
            ofs.close();
        }

        PLOGI << "Download complete. Unzipping...";
        int err = 0;
        zip_t* za = zip_open(downloadPath.string().c_str(), ZIP_RDONLY, &err);
        if (!za) {
            zip_error_t zerr;
            zip_error_init_with_code(&zerr, err);
            const auto error_msg = std::format("Failed to open zip archive: {} (libzip error: {})", downloadPath.string(), zip_error_strerror(&zerr));
            PLOGE << error_msg;
            zip_error_fini(&zerr);
            // If the zip is corrupted, we should probably delete it so the next run can try again
            std::filesystem::remove(downloadPath);
            throw std::runtime_error(error_msg);
        }

        zip_stat_t st{};
        if (zip_stat(za, zipCSVFileName.c_str(), 0, &st) != 0) {
            zip_close(za);
            const auto msg = std::format("File '{}' not found inside zip: {}", zipCSVFileName, downloadPath.string());
            throw std::runtime_error(msg);
        }

        zip_file_t* zf = zip_fopen(za, zipCSVFileName.c_str(), 0);
        if (!zf) {
            zip_close(za);
            const auto msg = std::format("Failed to open '{}' from zip: {}", zipCSVFileName, downloadPath.string());
            throw std::runtime_error(msg);
        }

        std::ofstream out_csv(final_csv_path, std::ios::binary);
        if (!out_csv.is_open()) {
            zip_fclose(zf);
            zip_close(za);
            throw std::runtime_error("Failed to create extracted file: " + final_csv_path.string());
        }

        std::vector<char> zbuf(1 << 16);
        zip_int64_t n;
        while ((n = zip_fread(zf, zbuf.data(), static_cast<zip_uint64_t>(zbuf.size()))) > 0) {
            out_csv.write(zbuf.data(), n);
        }
        out_csv.close();
        zip_fclose(zf);
        zip_close(za);

        PLOGI << "Unzip complete. CSV available at: " << final_csv_path.string();
        registerGeneration(fullTablename, final_csv_path);
    }

    GeneratorState::GeneratorState(const sql::Engine& engine, const std::filesystem::path& basePath, const CloudProvider dataProvider,
                                   std::string dataPath)
        : engine_(engine)
        , basePath_(basePath)
        , dataProvider_(dataProvider)
        , dataPath_(std::move(dataPath))
    {
    }

    GeneratorState::~GeneratorState()
    {
    }

    void GeneratorState::ensure(const std::string_view table_name, sql::ConnectionFactory& conn)
    {
        std::vector table_names{table_name};
        ensure(std::span(table_names), conn);
    }

    void GeneratorState::ensure(std::span<const std::string_view> table_names, sql::ConnectionFactory& conn)
    {
        for (auto table_name : table_names) {
            sql::checkTableName(table_name);
            if (ready_tables_.contains(table_name)) {
                continue;
            }
            std::unique_ptr<sql::ConnectionBase> cn = conn.create();
            PLOGD << "Ensuring table: " << table_name;

            // 1. Check if the table is generated
            if (!table(table_name).is_generated) {
                PLOGD << "Table: " << table_name << " is not marked as generated. Preparing input...";
                generate(table_name, cn.get());
            }

            auto existing_rows = cn->tableRowCount(table_name);
            const auto expected_rows = table(table_name).row_count;

            // 2. Check row count and skip if matches
            if (existing_rows && *existing_rows == expected_rows) {
                PLOGI << "Table: " << table_name << " already exists with correct " << *existing_rows << " rows";
                ready_tables_.insert(std::string(table_name));
                continue;
            }

            // 3. Create schema if missing
            if (!existing_rows) {
                PLOGI << "Table: " << table_name << " does not exist. Constructing it from DDL";
                const auto table_ddl = table(table_name).ddl;
                cn->executeDdl(table_ddl);
            }

            // 4. Handle incorrect row count
            auto actual_rows = cn->tableRowCount(table_name).value_or(0);
            if (actual_rows > 0 && actual_rows != expected_rows) {
                PLOGW << "Table: " << table_name << " has " << actual_rows << " rows, but we expected " << expected_rows << ". Deleting and reloading...";
                cn->execute("DELETE FROM " + std::string(table_name));
                actual_rows = 0;
            }

            // 5. Load if empty
            if (actual_rows == 0) {
                PLOGI << "Loading table: " << table_name << " (expected: " << expected_rows << ")";
                load(table_name, *cn);
            }

            ready_tables_.insert(std::string(table_name));
        }
    }

    void GeneratorState::ensureDataset(const std::string_view dataset_name, sql::ConnectionFactory& conn)
    {
        if (!containsDataset(dataset_name)) {
            throw std::runtime_error("Dataset not found: " + std::string(dataset_name));
        }

        const auto& tables = available_datasets().at(dataset_name);
        if (tables.empty()) {
            return;
        }

        std::set<std::string> schemas;
        for (const auto table_name : tables) {
            auto [schema_name, _] = sql::splitTable(table_name);
            if (!schema_name.empty()) {
                schemas.insert(schema_name);
            }
        }

        if (!schemas.empty()) {
            auto schema_conn = conn.create();
            for (const auto& schema_name : schemas) {
                try {
                    schema_conn->createSchema(schema_name);
                } catch (std::exception& e) {
                    PLOGD << "Schema creation failed (might already exist): " << e.what();
                }
            }
        }

        ensure(std::span<const std::string_view>(tables), conn);
    }

    bool GeneratorState::contains(const std::string_view table_name)
    {
        sql::checkTableName(table_name);
        return available_tables().contains(table_name);
    }

    bool GeneratorState::containsDataset(const std::string_view dataset_name)
    {
        return available_datasets().contains(dataset_name);
    }


    sql::RowCount generator::GeneratorState::generate(const std::string_view table_name, sql::ConnectionBase* conn)
    {
        sql::checkTableName(table_name);
        if (!contains(table_name)) {
            throw std::runtime_error("Generator not found for table: " + std::string(table_name));
        }

        const auto target_row_count = table(table_name).row_count;
        const auto file_name = csvPath(table_name);
        
        // Remove zero-length file if it exists
        if (exists(file_name) && file_size(file_name) == 0) {
            PLOGW << "Removing zero-length file: " << file_name;
            std::filesystem::remove(file_name);
        }

        if (exists(file_name) && file_size(file_name) > 0) {
            // NOTE: even a zero row file will still have a header.
            PLOGI << "Table: " << table_name << " input already exists.";
            table(table_name).path = file_name;
            return target_row_count;
        }

        if (!engine_.needsLocalFile()) {
            PLOGI << "Engine " << engine_.name() << " does not need local files. Skipping generator.";
            return target_row_count;
        }

        PLOGI << "Table: " << table_name << " input being generated...";
        table(table_name).generator(*this, conn);

        return target_row_count;
    }

    sql::RowCount GeneratorState::load(const std::string_view table_name, sql::ConnectionBase& conn)
    {
        sql::checkTableName(table_name);
        const auto& t = table(table_name);
        const auto expected_rows = t.row_count;
        
        PLOGI << "Loading table: " << table_name << "...";
        conn.bulkLoad(table_name, {t.path});

        auto actual_rows = conn.tableRowCount(table_name).value_or(0);
        PLOGI << "Table: " << table_name << " loaded. Now has " << actual_rows << " rows";

        if (actual_rows != expected_rows) {
            const auto error_msg = std::format("Table: {} still has incorrect row count after bulk load. Got {}, expected {}", 
                                                table_name, actual_rows, expected_rows);
            PLOGE << error_msg;
            throw std::runtime_error(error_msg);
        }

        return actual_rows;
    }

    void GeneratorState::registerGeneration(const std::string_view table_name, const std::filesystem::path& path) const
    {
        sql::checkTableName(table_name);
        table(table_name).path = path;
        table(table_name).is_generated = true;
    }

    void GeneratorState::printSummary(std::ostream& out) const
    {
        if (ready_tables_.empty()) {
            return;
        }

        dbprove::ux::Header(out, "Tables Loaded");
        std::vector<dbprove::ux::RowStats> stats;
        for (const auto& table_name : ready_tables_) {
            stats.push_back({table_name, table(table_name).row_count});
        }
        dbprove::ux::RowStatTable(out, stats);
    }

    GeneratedTable& GeneratorState::table(const std::string_view table_name) const
    {
        sql::checkTableName(table_name);
        if (!contains(table_name)) {
            throw std::runtime_error(
                "Table not found: " + std::string(table_name) +
                ". Did you forget to call or include the appropriate REGISTER_GENERATOR");
        }
        return *available_tables().at(table_name);
    }

    Registrar::Registrar(const std::string_view table_name, const std::string_view dataset_name,
                         const std::string_view ddl, const GeneratorFunc& f,
                         const sql::RowCount rows)
    {
        sql::checkTableName(table_name);
        available_tables().emplace(table_name, new GeneratedTable{table_name, dataset_name, ddl, f, rows});
        available_datasets()[dataset_name].push_back(table_name);
    }

}
